//! Redis protocol (RESP) implementation

pub mod cluster;
pub mod command_selector;
pub mod command_template;
pub mod crc16;
pub mod slot;

use crate::Protocol;
use anyhow::{anyhow, Result};
use cluster::{parse_redirect, RedirectType};
use command_selector::CommandSelector;
use command_template::CommandTemplate;
use zeropool::BufferPool;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisOp {
    Get,
    Set,
    Incr,
    /// DEL - Delete one or more keys
    Del,
    /// Multi-get - fetch multiple keys at once
    MGet {
        count: usize,
    },
    /// MSET - Set multiple keys at once
    MSet {
        count: usize,
    },
    /// WAIT for replication
    Wait {
        num_replicas: usize,
        timeout_ms: u64,
    },
    /// AUTH - Authenticate with Redis
    Auth {
        username: Option<String>, // For Redis 6.0+ ACL
        password: String,
    },
    /// SELECT - Select database
    SelectDb {
        db: u32,
    },
    /// HELLO - Set RESP protocol version
    Hello {
        version: u8, // 2 or 3
    },
    /// SETEX - Set with expiry in seconds
    SetEx {
        ttl_seconds: u32,
    },
    /// SETRANGE - Write at specific offset
    SetRange {
        offset: usize,
    },
    /// GETRANGE - Read from offset to end
    GetRange {
        offset: usize,
        end: i64, // -1 for end of string
    },
    /// CLUSTER SLOTS - Query cluster topology
    ClusterSlots,
    /// SCAN - Iterate over keys in the database
    Scan {
        cursor: u64,
        count: Option<usize>,    // SCAN cursor [COUNT count]
        pattern: Option<String>, // SCAN cursor [MATCH pattern]
    },
    /// MULTI - Start a transaction
    Multi,
    /// EXEC - Execute all commands in transaction
    Exec,
    /// DISCARD - Discard all commands in transaction
    Discard,
    // List operations
    /// LPUSH - Push element(s) to the head of a list
    LPush,
    /// RPUSH - Push element(s) to the tail of a list
    RPush,
    /// LPOP - Remove and return element from the head of a list
    LPop,
    /// RPOP - Remove and return element from the tail of a list
    RPop,
    /// LRANGE - Get a range of elements from a list
    LRange {
        start: i64,
        stop: i64,
    },
    /// LLEN - Get the length of a list
    LLen,
    // Set operations
    /// SADD - Add member(s) to a set
    SAdd,
    /// SREM - Remove member(s) from a set
    SRem,
    /// SMEMBERS - Get all members of a set
    SMembers,
    /// SISMEMBER - Check if member exists in a set
    SIsMember,
    /// SPOP - Remove and return random member(s) from a set
    SPop {
        count: Option<usize>,
    },
    /// SCARD - Get the number of members in a set
    SCard,
    // Sorted set operations
    /// ZADD - Add member with score to sorted set
    ZAdd {
        score: f64,
    },
    /// ZREM - Remove member(s) from sorted set
    ZRem,
    /// ZRANGE - Get range of members from sorted set by index
    ZRange {
        start: i64,
        stop: i64,
        with_scores: bool,
    },
    /// ZRANGEBYSCORE - Get range of members by score
    ZRangeByScore {
        min: f64,
        max: f64,
        with_scores: bool,
    },
    /// ZSCORE - Get score of member in sorted set
    ZScore,
    /// ZCARD - Get number of members in sorted set
    ZCard,
    // Hash operations
    /// HSET - Set field in hash
    HSet {
        field: String,
    },
    /// HGET - Get field from hash
    HGet {
        field: String,
    },
    /// HMSET - Set multiple fields in hash
    HMSet {
        fields: Vec<String>,
    },
    /// HMGET - Get multiple fields from hash
    HMGet {
        fields: Vec<String>,
    },
    /// HGETALL - Get all fields and values from hash
    HGetAll,
    /// HDEL - Delete field(s) from hash
    HDel {
        field: String,
    },
    /// Custom command from template
    Custom(CommandTemplate),
}

/// Insert phase state for data population before measurement
#[derive(Debug)]
pub struct InsertPhaseState {
    /// Total number of keys to insert
    key_count: u64,
    /// Number of keys inserted so far
    inserted: u64,
    /// Value size for inserts
    value_size: usize,
    /// Key generator for inserts (clone of workload generator to preserve distribution/seed)
    key_gen: Option<crate::workload::KeyGeneration>,
}

impl InsertPhaseState {
    /// Create new insert phase state
    pub fn new(
        key_count: u64,
        value_size: usize,
        key_gen: Option<crate::workload::KeyGeneration>,
    ) -> Self {
        Self {
            key_count,
            inserted: 0,
            value_size,
            key_gen,
        }
    }

    /// Check if insert phase is complete
    pub fn is_complete(&self) -> bool {
        self.inserted >= self.key_count
    }

    /// Get next key to insert, returns None if insert phase is complete
    pub fn next_key(&mut self) -> Option<u64> {
        if self.inserted >= self.key_count {
            None
        } else {
            self.inserted += 1;
            let key = if let Some(ref mut key_gen) = self.key_gen {
                key_gen.next_key()
            } else {
                self.inserted - 1 // Fallback to sequential if no generator is configured
            };
            Some(key)
        }
    }
}

pub struct RedisProtocol {
    command_selector: Box<dyn CommandSelector<RedisOp>>,
    /// Per-connection sequence numbers for send
    conn_send_seq: std::collections::HashMap<usize, u64>,
    /// Per-connection sequence numbers for receive
    conn_recv_seq: std::collections::HashMap<usize, u64>,
    /// Track redirect statistics (for cluster support)
    moved_redirects: u64,
    ask_redirects: u64,
    /// RESP protocol version (2 or 3)
    #[allow(dead_code)] // Reserved for future RESP3-specific features
    resp_version: u8,
    /// Track connections currently in a transaction
    conn_in_transaction: std::collections::HashSet<usize>,
    /// Buffer pool for request generation
    pool: BufferPool,
    /// Command execution statistics
    command_stats: std::collections::HashMap<String, u64>,
    /// Key generator for next_request() - None means use command_selector's per-command keys
    key_gen: Option<crate::workload::KeyGeneration>,
    /// Value size for next_request()
    value_size: usize,
    /// Key prefix for generated keys (default: "key:")
    key_prefix: String,
    /// Whether to use random data for values instead of repeated 'x'
    random_data: bool,
    /// RNG for random data generation (only used when random_data is true)
    rng: Option<rand::rngs::SmallRng>,
    /// Insert phase state (None = no insert phase, go straight to measurement)
    insert_phase: Option<InsertPhaseState>,
}

impl RedisProtocol {
    /// Create a new Redis protocol with a command selector
    pub fn new(command_selector: Box<dyn CommandSelector<RedisOp>>) -> Self {
        Self {
            command_selector,
            conn_send_seq: std::collections::HashMap::new(),
            conn_recv_seq: std::collections::HashMap::new(),
            moved_redirects: 0,
            ask_redirects: 0,
            resp_version: 2, // Default to RESP2
            conn_in_transaction: std::collections::HashSet::new(),
            pool: BufferPool::new(),
            command_stats: std::collections::HashMap::new(),
            key_gen: None,
            value_size: 64,
            key_prefix: "key:".to_string(),
            random_data: false,
            rng: None,
            insert_phase: None,
        }
    }

    /// Create with embedded workload generator
    pub fn with_workload(
        command_selector: Box<dyn CommandSelector<RedisOp>>,
        key_gen: crate::workload::KeyGeneration,
        value_size: usize,
    ) -> Self {
        Self::with_workload_and_options(
            command_selector,
            key_gen,
            value_size,
            "key:".to_string(),
            false,
            None,
        )
    }

    /// Create with embedded workload generator and custom options
    pub fn with_workload_and_options(
        command_selector: Box<dyn CommandSelector<RedisOp>>,
        key_gen: crate::workload::KeyGeneration,
        value_size: usize,
        key_prefix: String,
        random_data: bool,
        seed: Option<u64>,
    ) -> Self {
        use rand::SeedableRng;

        let rng = if random_data {
            Some(match seed {
                Some(s) => rand::rngs::SmallRng::seed_from_u64(s),
                None => rand::rngs::SmallRng::seed_from_u64(rand::random()),
            })
        } else {
            None
        };

        Self {
            command_selector,
            conn_send_seq: std::collections::HashMap::new(),
            conn_recv_seq: std::collections::HashMap::new(),
            moved_redirects: 0,
            ask_redirects: 0,
            resp_version: 2,
            conn_in_transaction: std::collections::HashSet::new(),
            pool: BufferPool::new(),
            command_stats: std::collections::HashMap::new(),
            key_gen: Some(key_gen),
            value_size,
            key_prefix,
            random_data,
            rng,
            insert_phase: None,
        }
    }

    /// Set up insert phase for data population before measurement
    pub fn with_insert_phase(mut self, key_count: u64, value_size: usize) -> Self {
        // Clone generator so warmup uses same distribution/seed without consuming measurement generator
        let insert_key_gen = self.key_gen.clone();
        self.insert_phase = Some(InsertPhaseState::new(key_count, value_size, insert_key_gen));
        self
    }

    /// Check if currently in insert phase (warmup)
    pub fn is_in_insert_phase(&self) -> bool {
        self.insert_phase.as_ref().is_some_and(|p| !p.is_complete())
    }

    /// Record a command execution
    fn record_command(&mut self, op: &RedisOp) {
        let cmd_name = match op {
            RedisOp::Get => "GET",
            RedisOp::Set => "SET",
            RedisOp::Incr => "INCR",
            RedisOp::Del => "DEL",
            RedisOp::MGet { .. } => "MGET",
            RedisOp::MSet { .. } => "MSET",
            RedisOp::Wait { .. } => "WAIT",
            RedisOp::Auth { .. } => "AUTH",
            RedisOp::SelectDb { .. } => "SELECT",
            RedisOp::Hello { .. } => "HELLO",
            RedisOp::SetEx { .. } => "SETEX",
            RedisOp::SetRange { .. } => "SETRANGE",
            RedisOp::GetRange { .. } => "GETRANGE",
            RedisOp::ClusterSlots => "CLUSTER_SLOTS",
            RedisOp::Scan { .. } => "SCAN",
            RedisOp::Multi => "MULTI",
            RedisOp::Exec => "EXEC",
            RedisOp::Discard => "DISCARD",
            // List operations
            RedisOp::LPush => "LPUSH",
            RedisOp::RPush => "RPUSH",
            RedisOp::LPop => "LPOP",
            RedisOp::RPop => "RPOP",
            RedisOp::LRange { .. } => "LRANGE",
            RedisOp::LLen => "LLEN",
            // Set operations
            RedisOp::SAdd => "SADD",
            RedisOp::SRem => "SREM",
            RedisOp::SMembers => "SMEMBERS",
            RedisOp::SIsMember => "SISMEMBER",
            RedisOp::SPop { .. } => "SPOP",
            RedisOp::SCard => "SCARD",
            // Sorted set operations
            RedisOp::ZAdd { .. } => "ZADD",
            RedisOp::ZRem => "ZREM",
            RedisOp::ZRange { .. } => "ZRANGE",
            RedisOp::ZRangeByScore { .. } => "ZRANGEBYSCORE",
            RedisOp::ZScore => "ZSCORE",
            RedisOp::ZCard => "ZCARD",
            // Hash operations
            RedisOp::HSet { .. } => "HSET",
            RedisOp::HGet { .. } => "HGET",
            RedisOp::HMSet { .. } => "HMSET",
            RedisOp::HMGet { .. } => "HMGET",
            RedisOp::HGetAll => "HGETALL",
            RedisOp::HDel { .. } => "HDEL",
            RedisOp::Custom(_template) => "CUSTOM",
        };
        *self.command_stats.entry(cmd_name.to_string()).or_insert(0) += 1;
    }

    /// Get command execution statistics
    pub fn get_command_stats(&self) -> &std::collections::HashMap<String, u64> {
        &self.command_stats
    }

    /// Get next sequence number for a connection (for sending)
    fn next_send_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.conn_send_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        // Use saturating_add to prevent overflow - will stick at u64::MAX
        *seq = seq.saturating_add(1);
        result
    }

    /// Get next sequence number for a connection (for receiving)
    fn next_recv_seq(&mut self, conn_id: usize) -> u64 {
        let seq = self.conn_recv_seq.entry(conn_id).or_insert(0);
        let result = *seq;
        // Use saturating_add to prevent overflow - will stick at u64::MAX
        *seq = seq.saturating_add(1);
        result
    }

    /// Get redirect statistics (for monitoring cluster behavior)
    pub fn redirect_stats(&self) -> (u64, u64) {
        (self.moved_redirects, self.ask_redirects)
    }

    /// Reset redirect statistics
    pub fn reset_redirect_stats(&mut self) {
        self.moved_redirects = 0;
        self.ask_redirects = 0;
    }

    /// Get mutable access to key generator (for cluster protocol delegation)
    pub fn key_gen_mut(&mut self) -> Option<&mut crate::workload::KeyGeneration> {
        self.key_gen.as_mut()
    }

    /// Get configured value size
    pub fn value_size(&self) -> usize {
        self.value_size
    }

    /// Get the key prefix
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    /// Check if random data is enabled
    pub fn random_data(&self) -> bool {
        self.random_data
    }

    /// Format a key with the configured prefix
    fn format_key(&self, key: u64) -> String {
        format!("{}{}", self.key_prefix, key)
    }

    /// Generate value data of the specified size
    fn generate_value(&mut self, size: usize) -> String {
        if self.random_data {
            if let Some(ref mut rng) = self.rng {
                use rand::Rng;
                // Generate printable ASCII characters (33-126) for safe RESP encoding
                (0..size)
                    .map(|_| {
                        let c: u8 = rng.random_range(33..127);
                        c as char
                    })
                    .collect()
            } else {
                // Fallback to 'x' if RNG not available
                "x".repeat(size)
            }
        } else {
            "x".repeat(size)
        }
    }

    /// Check if a connection is in a transaction
    pub fn is_in_transaction(&self, conn_id: usize) -> bool {
        self.conn_in_transaction.contains(&conn_id)
    }

    /// Mark a connection as being in a transaction
    fn set_transaction_state(&mut self, conn_id: usize, in_transaction: bool) {
        if in_transaction {
            self.conn_in_transaction.insert(conn_id);
        } else {
            self.conn_in_transaction.remove(&conn_id);
        }
    }

    /// Clean up connection state when connection closes
    /// This prevents HashMap leak with dead connection IDs
    pub fn cleanup_connection(&mut self, conn_id: usize) {
        self.conn_send_seq.remove(&conn_id);
        self.conn_recv_seq.remove(&conn_id);
        self.conn_in_transaction.remove(&conn_id);
    }
}

impl RedisProtocol {
    /// Check if protocol has per-command key generation enabled
    pub fn has_per_command_keys(&self) -> bool {
        self.command_selector.has_per_command_keys()
    }

    /// Generate request using per-command key generation
    /// This variant selects the command first, generates key for that command,
    /// then creates the request
    pub fn generate_request_with_command_keys(
        &mut self,
        conn_id: usize,
        value_size: usize,
    ) -> crate::Request<(usize, u64)> {
        let seq = self.next_send_seq(conn_id);
        let id = (conn_id, seq);

        // Select command first
        let operation = self.command_selector.next_command();
        self.record_command(&operation);

        // Generate key for this specific command
        let key = self
            .command_selector
            .generate_key_for_command(&operation)
            .expect("Per-command key generation should be available");

        let request_data = self.format_command(&operation, key, value_size);
        crate::Request::measurement(request_data, id)
    }

    /// Generate a SET request with imported data (string key + value bytes)
    pub fn generate_set_with_imported_data(
        &mut self,
        conn_id: usize,
        key: &str,
        value: &[u8],
    ) -> crate::Request<(usize, u64)> {
        let seq = self.next_send_seq(conn_id);
        let id = (conn_id, seq);

        // Format: *3\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<vallen>\r\n<value>\r\n
        let mut request = self.pool.get(256 + key.len() + value.len());
        request.clear();
        request.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
        request.extend_from_slice(format!("${}\r\n", key.len()).as_bytes());
        request.extend_from_slice(key.as_bytes());
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
        request.extend_from_slice(value);
        request.extend_from_slice(b"\r\n");

        crate::Request::measurement(request, id)
    }

    /// Generate a GET request with imported data (string key)
    pub fn generate_get_with_imported_data(
        &mut self,
        conn_id: usize,
        key: &str,
    ) -> crate::Request<(usize, u64)> {
        let seq = self.next_send_seq(conn_id);
        let id = (conn_id, seq);

        // Format: *2\r\n$3\r\nGET\r\n$<keylen>\r\n<key>\r\n
        let mut request = self.pool.get(256 + key.len());
        request.clear();
        request.extend_from_slice(b"*2\r\n$3\r\nGET\r\n");
        request.extend_from_slice(format!("${}\r\n", key.len()).as_bytes());
        request.extend_from_slice(key.as_bytes());
        request.extend_from_slice(b"\r\n");

        // Move the buffer (don't clone) to avoid memory leak
        crate::Request::measurement(request, id)
    }

    /// Generate a request with specific key and value size
    ///
    /// This is used internally and by RedisClusterProtocol for routing requests.
    pub fn generate_request_with_key(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
    ) -> crate::Request<(usize, u64)> {
        let seq = self.next_send_seq(conn_id);
        let id = (conn_id, seq);

        // Select which command to execute
        let operation = self.command_selector.next_command();
        self.record_command(&operation);

        // Update transaction state based on command
        match &operation {
            RedisOp::Multi => self.set_transaction_state(conn_id, true),
            RedisOp::Exec | RedisOp::Discard => self.set_transaction_state(conn_id, false),
            _ => {}
        }

        let request_data = self.format_command(&operation, key, value_size);

        crate::Request::measurement(request_data, id)
    }

    /// Generate a request with specific operation, key and value size
    ///
    /// This is used for insert phase warmup, where we always want SET regardless
    /// of the configured command selector.
    pub fn generate_request_for_op(
        &mut self,
        conn_id: usize,
        key: u64,
        value_size: usize,
        operation: &RedisOp,
    ) -> crate::Request<(usize, u64)> {
        let seq = self.next_send_seq(conn_id);
        let id = (conn_id, seq);
        self.record_command(operation);
        let request_data = self.format_command(operation, key, value_size);
        crate::Request::measurement(request_data, id)
    }

    /// Format a Redis command into RESP bytes
    fn format_command(&mut self, operation: &RedisOp, key: u64, value_size: usize) -> Vec<u8> {
        match operation {
            RedisOp::Get => {
                let key_str = self.format_key(key);
                format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key_str.len(), key_str).into_bytes()
            }
            RedisOp::Set => {
                let key_str = self.format_key(key);
                let value = self.generate_value(value_size);
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    value_size,
                    value
                )
                .into_bytes()
            }
            RedisOp::Incr => {
                let key_str = self.format_key(key);
                format!("*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n", key_str.len(), key_str).into_bytes()
            }
            RedisOp::MGet { count } => {
                let mut request = format!("*{}\r\n$4\r\nMGET\r\n", count + 1);
                for i in 0..*count {
                    // Use saturating_add to prevent overflow - keys wrap to max value
                    let curr_key = key.saturating_add(i as u64);
                    let key_str = self.format_key(curr_key);
                    request.push_str(&format!("${}\r\n{}\r\n", key_str.len(), key_str));
                }
                request.into_bytes()
            }
            RedisOp::Wait { num_replicas, timeout_ms } => {
                let num_str = num_replicas.to_string();
                let timeout_str = timeout_ms.to_string();
                format!(
                    "*3\r\n$4\r\nWAIT\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    num_str.len(),
                    num_str,
                    timeout_str.len(),
                    timeout_str
                )
                .into_bytes()
            }
            RedisOp::Auth { username, password } => {
                if let Some(user) = username {
                    format!(
                        "*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        user.len(),
                        user,
                        password.len(),
                        password
                    )
                    .into_bytes()
                } else {
                    format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", password.len(), password)
                        .into_bytes()
                }
            }
            RedisOp::SelectDb { db } => {
                let db_str = db.to_string();
                format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db_str.len(), db_str).into_bytes()
            }
            RedisOp::Hello { version } => {
                format!("*2\r\n$5\r\nHELLO\r\n$1\r\n{}\r\n", version).into_bytes()
            }
            RedisOp::SetEx { ttl_seconds } => {
                let key_str = self.format_key(key);
                let value = self.generate_value(value_size);
                let ttl_str = ttl_seconds.to_string();
                format!(
                    "*4\r\n$5\r\nSETEX\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    ttl_str.len(),
                    ttl_str,
                    value_size,
                    value
                )
                .into_bytes()
            }
            RedisOp::SetRange { offset } => {
                let key_str = self.format_key(key);
                let value = self.generate_value(value_size);
                let offset_str = offset.to_string();
                format!(
                    "*4\r\n$8\r\nSETRANGE\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    offset_str.len(),
                    offset_str,
                    value_size,
                    value
                )
                .into_bytes()
            }
            RedisOp::GetRange { offset, end } => {
                let key_str = self.format_key(key);
                let offset_str = offset.to_string();
                let end_str = end.to_string();
                format!(
                    "*4\r\n$8\r\nGETRANGE\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    offset_str.len(),
                    offset_str,
                    end_str.len(),
                    end_str
                )
                .into_bytes()
            }
            RedisOp::ClusterSlots => b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n".to_vec(),
            RedisOp::Scan { cursor, count, pattern } => {
                let cursor_str = cursor.to_string();
                let mut parts = vec![];
                let mut num_args = 2; // SCAN + cursor

                // Build SCAN cursor
                parts.push(format!("$4\r\nSCAN\r\n${}\r\n{}\r\n", cursor_str.len(), cursor_str));

                // Add COUNT if specified
                if let Some(c) = count {
                    num_args += 2; // COUNT + value
                    let count_str = c.to_string();
                    parts.push(format!("$5\r\nCOUNT\r\n${}\r\n{}\r\n", count_str.len(), count_str));
                }

                // Add MATCH if specified
                if let Some(p) = pattern {
                    num_args += 2; // MATCH + pattern
                    parts.push(format!("$5\r\nMATCH\r\n${}\r\n{}\r\n", p.len(), p));
                }

                // Build final request
                let mut request = format!("*{}\r\n", num_args);
                for part in parts {
                    request.push_str(&part);
                }
                request.into_bytes()
            }
            RedisOp::Multi => b"*1\r\n$5\r\nMULTI\r\n".to_vec(),
            RedisOp::Exec => b"*1\r\n$4\r\nEXEC\r\n".to_vec(),
            RedisOp::Discard => b"*1\r\n$7\r\nDISCARD\r\n".to_vec(),
            // DEL command
            RedisOp::Del => {
                let key_str = self.format_key(key);
                format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key_str.len(), key_str).into_bytes()
            }
            // MSET command
            RedisOp::MSet { count } => {
                let value = self.generate_value(value_size);
                let mut request = format!("*{}\r\n$4\r\nMSET\r\n", count * 2 + 1);
                for i in 0..*count {
                    // Use saturating_add to prevent overflow - keys wrap to max value
                    let curr_key = key.saturating_add(i as u64);
                    let key_str = self.format_key(curr_key);
                    request.push_str(&format!(
                        "${}\r\n{}\r\n${}\r\n{}\r\n",
                        key_str.len(),
                        key_str,
                        value_size,
                        value
                    ));
                }
                request.into_bytes()
            }
            // List operations
            RedisOp::LPush => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                let value = self.generate_value(value_size);
                format!(
                    "*3\r\n$5\r\nLPUSH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    value_size,
                    value
                )
                .into_bytes()
            }
            RedisOp::RPush => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                let value = self.generate_value(value_size);
                format!(
                    "*3\r\n$5\r\nRPUSH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    value_size,
                    value
                )
                .into_bytes()
            }
            RedisOp::LPop => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                format!("*2\r\n$4\r\nLPOP\r\n${}\r\n{}\r\n", key_str.len(), key_str).into_bytes()
            }
            RedisOp::RPop => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                format!("*2\r\n$4\r\nRPOP\r\n${}\r\n{}\r\n", key_str.len(), key_str).into_bytes()
            }
            RedisOp::LRange { start, stop } => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                let start_str = start.to_string();
                let stop_str = stop.to_string();
                format!(
                    "*4\r\n$6\r\nLRANGE\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    start_str.len(),
                    start_str,
                    stop_str.len(),
                    stop_str
                )
                .into_bytes()
            }
            RedisOp::LLen => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                format!("*2\r\n$4\r\nLLEN\r\n${}\r\n{}\r\n", key_str.len(), key_str).into_bytes()
            }
            // Set operations
            RedisOp::SAdd => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                format!(
                    "*3\r\n$4\r\nSADD\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    member.len(),
                    member
                )
                .into_bytes()
            }
            RedisOp::SRem => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                format!(
                    "*3\r\n$4\r\nSREM\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    member.len(),
                    member
                )
                .into_bytes()
            }
            RedisOp::SMembers => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                format!("*2\r\n$8\r\nSMEMBERS\r\n${}\r\n{}\r\n", key_str.len(), key_str)
                    .into_bytes()
            }
            RedisOp::SIsMember => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                format!(
                    "*3\r\n$9\r\nSISMEMBER\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    member.len(),
                    member
                )
                .into_bytes()
            }
            RedisOp::SPop { count } => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                if let Some(c) = count {
                    let count_str = c.to_string();
                    format!(
                        "*3\r\n$4\r\nSPOP\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key_str.len(),
                        key_str,
                        count_str.len(),
                        count_str
                    )
                    .into_bytes()
                } else {
                    format!("*2\r\n$4\r\nSPOP\r\n${}\r\n{}\r\n", key_str.len(), key_str)
                        .into_bytes()
                }
            }
            RedisOp::SCard => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                format!("*2\r\n$5\r\nSCARD\r\n${}\r\n{}\r\n", key_str.len(), key_str).into_bytes()
            }
            // Sorted set operations
            RedisOp::ZAdd { score } => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                let score_str = score.to_string();
                format!(
                    "*4\r\n$4\r\nZADD\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    score_str.len(),
                    score_str,
                    member.len(),
                    member
                )
                .into_bytes()
            }
            RedisOp::ZRem => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                format!(
                    "*3\r\n$4\r\nZREM\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    member.len(),
                    member
                )
                .into_bytes()
            }
            RedisOp::ZRange { start, stop, with_scores } => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let start_str = start.to_string();
                let stop_str = stop.to_string();
                if *with_scores {
                    format!(
                        "*5\r\n$6\r\nZRANGE\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n$10\r\nWITHSCORES\r\n",
                        key_str.len(),
                        key_str,
                        start_str.len(),
                        start_str,
                        stop_str.len(),
                        stop_str
                    )
                    .into_bytes()
                } else {
                    format!(
                        "*4\r\n$6\r\nZRANGE\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key_str.len(),
                        key_str,
                        start_str.len(),
                        start_str,
                        stop_str.len(),
                        stop_str
                    )
                    .into_bytes()
                }
            }
            RedisOp::ZRangeByScore { min, max, with_scores } => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let min_str = min.to_string();
                let max_str = max.to_string();
                if *with_scores {
                    format!(
                        "*5\r\n$13\r\nZRANGEBYSCORE\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n$10\r\nWITHSCORES\r\n",
                        key_str.len(),
                        key_str,
                        min_str.len(),
                        min_str,
                        max_str.len(),
                        max_str
                    )
                    .into_bytes()
                } else {
                    format!(
                        "*4\r\n$13\r\nZRANGEBYSCORE\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key_str.len(),
                        key_str,
                        min_str.len(),
                        min_str,
                        max_str.len(),
                        max_str
                    )
                    .into_bytes()
                }
            }
            RedisOp::ZScore => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                format!(
                    "*3\r\n$6\r\nZSCORE\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    member.len(),
                    member
                )
                .into_bytes()
            }
            RedisOp::ZCard => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                format!("*2\r\n$5\r\nZCARD\r\n${}\r\n{}\r\n", key_str.len(), key_str).into_bytes()
            }
            // Hash operations
            RedisOp::HSet { field } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                let value = self.generate_value(value_size);
                format!(
                    "*4\r\n$4\r\nHSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    field.len(),
                    field,
                    value_size,
                    value
                )
                .into_bytes()
            }
            RedisOp::HGet { field } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                format!(
                    "*3\r\n$4\r\nHGET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    field.len(),
                    field
                )
                .into_bytes()
            }
            RedisOp::HMSet { fields } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                let value = self.generate_value(value_size);
                // *<num_args>\r\n$5\r\nHMSET\r\n$<keylen>\r\n<key>\r\n[<field><value>...]
                let num_args = 2 + fields.len() * 2; // HMSET + key + (field + value) * count
                let mut request = format!(
                    "*{}\r\n$5\r\nHMSET\r\n${}\r\n{}\r\n",
                    num_args,
                    key_str.len(),
                    key_str
                );
                for field in fields {
                    request.push_str(&format!(
                        "${}\r\n{}\r\n${}\r\n{}\r\n",
                        field.len(),
                        field,
                        value_size,
                        value
                    ));
                }
                request.into_bytes()
            }
            RedisOp::HMGet { fields } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                // *<num_args>\r\n$5\r\nHMGET\r\n$<keylen>\r\n<key>\r\n[<field>...]
                let num_args = 2 + fields.len(); // HMGET + key + fields
                let mut request = format!(
                    "*{}\r\n$5\r\nHMGET\r\n${}\r\n{}\r\n",
                    num_args,
                    key_str.len(),
                    key_str
                );
                for field in fields {
                    request.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                }
                request.into_bytes()
            }
            RedisOp::HGetAll => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                format!("*2\r\n$7\r\nHGETALL\r\n${}\r\n{}\r\n", key_str.len(), key_str).into_bytes()
            }
            RedisOp::HDel { field } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                format!(
                    "*3\r\n$4\r\nHDEL\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key_str.len(),
                    key_str,
                    field.len(),
                    field
                )
                .into_bytes()
            }
            RedisOp::Custom(template) => {
                // For custom templates, generate value for __data__ substitution
                let value = self.generate_value(value_size);
                template.generate_request_with_options(key, value_size, &self.key_prefix, &value)
            }
        }
    }
}

impl Protocol for RedisProtocol {
    type RequestId = (usize, u64); // (conn_id, sequence)

    fn next_request(&mut self, conn_id: usize) -> crate::Request<Self::RequestId> {
        // Phase 1: Insert phase (warmup) - populate data before measurement
        if let Some(ref mut insert_state) = self.insert_phase {
            if let Some(key) = insert_state.next_key() {
                let value_size = insert_state.value_size;
                // Generate SET request for insert phase
                let request = self.generate_request_for_op(conn_id, key, value_size, &RedisOp::Set);
                return crate::Request::warmup(request.data, request.request_id);
            }
        }

        // Phase 2: Normal measurement phase
        let request = if self.command_selector.has_per_command_keys() {
            self.generate_request_with_command_keys(conn_id, self.value_size)
        } else {
            // Use embedded key generator or default to 0
            let key = self.key_gen.as_mut().map(|g| g.next_key()).unwrap_or(0);
            self.generate_request_with_key(conn_id, key, self.value_size)
        };
        request
    }

    fn parse_response(
        &mut self,
        conn_id: usize,
        data: &[u8],
    ) -> Result<(usize, Option<Self::RequestId>)> {
        if data.is_empty() {
            return Ok((0, None));
        }

        // Parse RESP response and find complete message
        let consumed = Self::find_resp_message_end(data)?;
        if consumed == 0 {
            // Incomplete response
            return Ok((0, None));
        }

        // Validate response
        match data[0] {
            b'+' | b':' | b'$' | b'*' | b'_' | b',' | b'#' | b'(' | b'%' | b'~' | b'|' | b'!'
            | b'=' | b'>' => {
                // Valid response (RESP2 and RESP3 types) - return the next expected ID for this connection
                // (Redis guarantees FIFO ordering per connection)
                let seq = self.next_recv_seq(conn_id);
                Ok((consumed, Some((conn_id, seq))))
            }
            b'-' => {
                // Check if this is a cluster redirect (MOVED or ASK)
                match parse_redirect(data) {
                    Ok(Some(RedirectType::Moved { slot, addr })) => {
                        // MOVED redirect - permanent slot reassignment
                        self.moved_redirects += 1;
                        Err(anyhow!(
                            "MOVED redirect: slot {} moved to {} (total MOVED: {})",
                            slot,
                            addr,
                            self.moved_redirects
                        ))
                    }
                    Ok(Some(RedirectType::Ask { slot, addr })) => {
                        // ASK redirect - temporary migration state
                        self.ask_redirects += 1;
                        Err(anyhow!(
                            "ASK redirect: slot {} temporarily at {} (total ASK: {})",
                            slot,
                            addr,
                            self.ask_redirects
                        ))
                    }
                    Ok(None) | Err(_) => {
                        // Regular Redis error (not a redirect)
                        Err(anyhow!("Redis error: {}", String::from_utf8_lossy(&data[1..consumed])))
                    }
                }
            }
            _ => Err(anyhow!("Invalid RESP response")),
        }
    }

    fn name(&self) -> &'static str {
        "redis"
    }

    fn reset(&mut self) {
        self.conn_send_seq.clear();
        self.conn_recv_seq.clear();
        self.conn_in_transaction.clear();
        self.command_selector.reset();
        self.command_stats.clear();
        if let Some(ref mut key_gen) = self.key_gen {
            key_gen.reset();
        }
        // Reset insert phase to allow re-running warmup
        if let Some(ref mut insert_state) = self.insert_phase {
            insert_state.inserted = 0;
            if let Some(ref mut gen) = insert_state.key_gen {
                gen.reset();
            }
        }
        // Clear redirect statistics
        self.moved_redirects = 0;
        self.ask_redirects = 0;
    }
}

impl RedisProtocol {
    /// Maximum recursion depth for nested RESP structures
    const MAX_RESP_DEPTH: usize = 32;

    /// Maximum array/map size to prevent DoS
    const MAX_ARRAY_SIZE: i64 = 10_000_000;

    /// Maximum bulk string size (1GB)
    const MAX_BULK_STRING_SIZE: i64 = 1_073_741_824;

    /// Maximum total message size (10MB) to prevent scanning huge buffers
    const MAX_MESSAGE_SIZE: usize = 10_485_760;

    /// Find the end of a complete RESP message, returns bytes consumed
    /// Supports both RESP2 and RESP3 types
    fn find_resp_message_end(data: &[u8]) -> Result<usize> {
        Self::find_resp_message_end_with_depth(data, 0)
    }

    /// Internal implementation with depth tracking
    fn find_resp_message_end_with_depth(data: &[u8], depth: usize) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }

        // Check message size to prevent excessive scanning
        if data.len() > Self::MAX_MESSAGE_SIZE {
            return Err(anyhow!(
                "RESP message too large: {} bytes (max: {})",
                data.len(),
                Self::MAX_MESSAGE_SIZE
            ));
        }

        // Check recursion depth to prevent stack overflow
        if depth > Self::MAX_RESP_DEPTH {
            return Err(anyhow!("RESP message nesting too deep (max: {})", Self::MAX_RESP_DEPTH));
        }

        match data[0] {
            b'+' | b'-' | b':' => {
                // RESP2: Simple strings, errors, integers end with \r\n
                if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
                    Ok(pos + 2)
                } else {
                    Ok(0) // Incomplete
                }
            }
            b'$' | b'!' | b'=' => {
                // RESP2: Bulk string ($)
                // RESP3: Blob error (!), Verbatim string (=)
                if let Some(first_crlf) = data.windows(2).position(|w| w == b"\r\n") {
                    let length_str = std::str::from_utf8(&data[1..first_crlf])
                        .map_err(|e| anyhow!("Invalid bulk string length: {e}"))?;
                    let length: i64 = length_str
                        .parse()
                        .map_err(|e| anyhow!("Failed to parse bulk string length: {e}"))?;

                    if length == -1 {
                        // Null bulk string
                        return Ok(first_crlf + 2);
                    }

                    // Validate length is non-negative and within limits
                    if length < 0 {
                        return Err(anyhow!("Invalid negative bulk string length: {}", length));
                    }
                    if length > Self::MAX_BULK_STRING_SIZE {
                        return Err(anyhow!(
                            "Bulk string too large: {} (max: {})",
                            length,
                            Self::MAX_BULK_STRING_SIZE
                        ));
                    }

                    // Use checked arithmetic to prevent overflow
                    let expected_total = first_crlf
                        .checked_add(2)
                        .and_then(|v| v.checked_add(length as usize))
                        .and_then(|v| v.checked_add(2))
                        .ok_or_else(|| {
                            anyhow!("Integer overflow in bulk string size calculation")
                        })?;

                    if data.len() >= expected_total {
                        Ok(expected_total)
                    } else {
                        Ok(0) // Incomplete
                    }
                } else {
                    Ok(0) // Incomplete
                }
            }
            b'*' | b'%' | b'~' | b'|' | b'>' => {
                // RESP2: Array (*)
                // RESP3: Map (%), Set (~), Attribute (|), Push (>)
                // Need to recursively parse all elements
                let first_crlf = match data.windows(2).position(|w| w == b"\r\n") {
                    Some(pos) => pos,
                    None => return Ok(0), // Incomplete
                };

                let count_str = std::str::from_utf8(&data[1..first_crlf])
                    .map_err(|e| anyhow!("Invalid array count: {e}"))?;
                let count: i64 =
                    count_str.parse().map_err(|e| anyhow!("Failed to parse array count: {e}"))?;

                if count == -1 {
                    // Null array
                    return Ok(first_crlf + 2);
                }

                // Validate count is non-negative and within limits
                if count < -1 {
                    return Err(anyhow!("Invalid array count: {} (must be >= -1)", count));
                }
                if count > Self::MAX_ARRAY_SIZE {
                    return Err(anyhow!(
                        "Array too large: {} (max: {})",
                        count,
                        Self::MAX_ARRAY_SIZE
                    ));
                }

                let mut pos = first_crlf + 2;
                // Parse each element in the array with incremented depth
                for _ in 0..count {
                    if pos >= data.len() {
                        return Ok(0); // Incomplete
                    }
                    let elem_size =
                        Self::find_resp_message_end_with_depth(&data[pos..], depth + 1)?;
                    if elem_size == 0 {
                        return Ok(0); // Incomplete element
                    }
                    pos += elem_size;
                }
                Ok(pos)
            }
            b'_' => {
                // RESP3: Null (_\r\n)
                if data.len() >= 3 && &data[0..3] == b"_\r\n" {
                    Ok(3)
                } else {
                    Ok(0) // Incomplete
                }
            }
            b',' | b'(' => {
                // RESP3: Double (,<floating-point-number>\r\n)
                // RESP3: Big number ((<big-number>\r\n)
                if let Some(pos) = data.windows(2).position(|w| w == b"\r\n") {
                    Ok(pos + 2)
                } else {
                    Ok(0) // Incomplete
                }
            }
            b'#' => {
                // RESP3: Boolean (#t\r\n or #f\r\n)
                if data.len() >= 4 {
                    // Validate actual boolean value
                    if data[1] != b't' && data[1] != b'f' {
                        return Err(anyhow!(
                            "Invalid RESP3 boolean value: must be 't' or 'f', got '{}'",
                            data[1] as char
                        ));
                    }
                    if &data[2..4] == b"\r\n" {
                        Ok(4)
                    } else {
                        Ok(0) // Incomplete
                    }
                } else {
                    Ok(0) // Incomplete
                }
            }
            _ => Err(anyhow!("Invalid RESP message type")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use command_selector::FixedCommandSelector;

    #[test]
    fn test_protocol_name() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let protocol = RedisProtocol::new(selector);
        assert_eq!(protocol.name(), "redis");
    }

    #[test]
    fn test_generate_get_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert_eq!(request.request_id, (0, 0)); // First request on connection 0
        assert!(request_str.contains("GET"));
        assert!(request_str.contains("key:42"));
        assert!(request_str.starts_with("*2\r\n")); // Array with 2 elements
    }

    #[test]
    fn test_generate_set_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 100, 5);
        let request_str = String::from_utf8_lossy(&request.data);

        assert_eq!(request.request_id, (0, 0));
        assert!(request_str.contains("SET"));
        assert!(request_str.contains("key:100"));
        assert!(request_str.contains("xxxxx")); // 5 bytes of value
        assert!(request_str.starts_with("*3\r\n")); // Array with 3 elements
    }

    #[test]
    fn test_generate_incr_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Incr));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 999, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.contains("INCR"));
        assert!(request_str.contains("key:999"));
        assert!(request_str.starts_with("*2\r\n"));
    }

    #[test]
    fn test_generate_mget_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::MGet { count: 3 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 10, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.contains("MGET"));
        assert!(request_str.contains("key:10"));
        assert!(request_str.contains("key:11"));
        assert!(request_str.contains("key:12"));
        assert!(request_str.starts_with("*4\r\n")); // MGET + 3 keys
    }

    #[test]
    fn test_generate_wait_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Wait {
            num_replicas: 2,
            timeout_ms: 1000,
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.contains("WAIT"));
        assert!(request_str.contains("2")); // num_replicas
        assert!(request_str.contains("1000")); // timeout_ms
        assert!(request_str.starts_with("*3\r\n"));
    }

    #[test]
    fn test_generate_custom_request() {
        let template = CommandTemplate::parse("HSET myhash __key__ __data__").unwrap();
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Custom(template)));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 5, 3);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.contains("HSET"));
        assert!(request_str.contains("myhash"));
        assert!(request_str.contains("key:5"));
        assert!(request_str.contains("xxx")); // 3 bytes of value
    }

    #[test]
    fn test_sequence_numbers() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Generate multiple requests on same connection
        let request = protocol.generate_request_with_key(0, 1, 64);
        let request2 = protocol.generate_request_with_key(0, 2, 64);
        let request3 = protocol.generate_request_with_key(0, 3, 64);

        assert_eq!(request.request_id, (0, 0));
        assert_eq!(request2.request_id, (0, 1));
        assert_eq!(request3.request_id, (0, 2));

        // Different connection has independent sequence
        let request4 = protocol.generate_request_with_key(1, 1, 64);
        assert_eq!(request4.request_id, (1, 0));
    }

    #[test]
    fn test_parse_simple_string_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"+OK\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 5);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_integer_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Incr));
        let mut protocol = RedisProtocol::new(selector);

        let response = b":42\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 5);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_bulk_string_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"$5\r\nhello\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 11);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"$-1\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_parse_array_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Complete array with 3 simple string elements
        let response = b"*3\r\n+one\r\n+two\r\n+three\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 24); // *3\r\n (4) + +one\r\n (6) + +two\r\n (6) + +three\r\n (8)
    }

    #[test]
    fn test_parse_error_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"-ERR unknown command\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ERR unknown command"));
    }

    #[test]
    fn test_parse_incomplete_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"+OK"; // Missing \r\n
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(id, None);
    }

    #[test]
    fn test_parse_empty_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(id, None);
    }

    #[test]
    fn test_parse_invalid_response_type() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"X invalid\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_incomplete_bulk_string() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"$10\r\nhello"; // Only 5 bytes of 10
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0); // Incomplete
        assert_eq!(id, None);
    }

    #[test]
    fn test_reset() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Generate some requests to build up sequence numbers
        protocol.generate_request_with_key(0, 1, 64);
        protocol.generate_request_with_key(0, 2, 64);
        protocol.generate_request_with_key(1, 1, 64);

        // Reset should clear sequence numbers
        protocol.reset();

        // Next request should start from sequence 0 again
        let request = protocol.generate_request_with_key(0, 1, 64);
        assert_eq!(request.request_id, (0, 0));
    }

    #[test]
    fn test_multiple_connections() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Generate requests on different connections
        let request = protocol.generate_request_with_key(0, 1, 64);
        let request2 = protocol.generate_request_with_key(1, 1, 64);
        let request3 = protocol.generate_request_with_key(2, 1, 64);

        assert_eq!(request.request_id, (0, 0));
        assert_eq!(request2.request_id, (1, 0));
        assert_eq!(request3.request_id, (2, 0));

        // Each connection maintains independent sequence
        let request4 = protocol.generate_request_with_key(0, 2, 64);
        let request5 = protocol.generate_request_with_key(1, 2, 64);

        assert_eq!(request4.request_id, (0, 1));
        assert_eq!(request5.request_id, (1, 1));
    }

    #[test]
    fn test_response_sequence_tracking() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Parse multiple responses on same connection
        let response = b"+OK\r\n";

        let (_, id1) = protocol.parse_response(0, response).unwrap();
        let (_, id2) = protocol.parse_response(0, response).unwrap();
        let (_, id3) = protocol.parse_response(0, response).unwrap();

        assert_eq!(id1, Some((0, 0)));
        assert_eq!(id2, Some((0, 1)));
        assert_eq!(id3, Some((0, 2)));
    }

    #[test]
    fn test_parse_invalid_resp_type() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Invalid RESP type byte (not +, -, :, $, or *)
        let invalid_response = b"@INVALID\r\n";
        let result = protocol.parse_response(0, invalid_response);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid RESP"));
    }

    #[test]
    fn test_parse_incomplete_array() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Incomplete array (no \r\n)
        let incomplete = b"*2";
        let result = protocol.parse_response(0, incomplete).unwrap();
        assert_eq!(result, (0, None)); // Should indicate incomplete
    }

    #[test]
    fn test_parse_incomplete_bulk_length() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Incomplete bulk string (no \r\n after length)
        let incomplete = b"$10";
        let result = protocol.parse_response(0, incomplete).unwrap();
        assert_eq!(result, (0, None)); // Should indicate incomplete
    }

    #[test]
    fn test_find_resp_message_end_empty() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let protocol = RedisProtocol::new(selector);

        // Empty data should return 0
        let result = RedisProtocol::find_resp_message_end(&[]).unwrap();
        let _ = protocol; // Silence unused variable warning
        assert_eq!(result, 0);
    }

    #[test]
    fn test_moved_redirect_detection() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let moved_error = b"-MOVED 3999 127.0.0.1:7001\r\n";
        let result = protocol.parse_response(0, moved_error);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("MOVED redirect"));
        assert!(err.contains("3999"));
        assert!(err.contains("127.0.0.1:7001"));

        // Check statistics
        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 1);
        assert_eq!(ask, 0);
    }

    #[test]
    fn test_ask_redirect_detection() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let ask_error = b"-ASK 5000 127.0.0.1:7002\r\n";
        let result = protocol.parse_response(0, ask_error);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("ASK redirect"));
        assert!(err.contains("5000"));
        assert!(err.contains("127.0.0.1:7002"));

        // Check statistics
        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 0);
        assert_eq!(ask, 1);
    }

    #[test]
    fn test_multiple_redirects_statistics() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Simulate multiple MOVED redirects
        let _ = protocol.parse_response(0, b"-MOVED 100 127.0.0.1:7001\r\n");
        let _ = protocol.parse_response(0, b"-MOVED 200 127.0.0.1:7002\r\n");

        // Simulate ASK redirects
        let _ = protocol.parse_response(0, b"-ASK 300 127.0.0.1:7003\r\n");

        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 2);
        assert_eq!(ask, 1);

        // Reset stats
        protocol.reset_redirect_stats();
        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 0);
        assert_eq!(ask, 0);
    }

    #[test]
    fn test_regular_error_vs_redirect() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Regular error should not increment redirect stats
        let regular_error = b"-ERR unknown command\r\n";
        let result = protocol.parse_response(0, regular_error);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Redis error"));
        assert!(!err.contains("redirect"));

        // Stats should be zero
        let (moved, ask) = protocol.redirect_stats();
        assert_eq!(moved, 0);
        assert_eq!(ask, 0);
    }

    // Tests for new commands (AUTH, SELECT, HELLO, etc.)

    #[test]
    fn test_generate_auth_simple() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Auth {
            username: None,
            password: "mypassword".to_string(),
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert_eq!(request.request_id, (0, 0));
        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("AUTH"));
        assert!(request_str.contains("mypassword"));
        assert!(!request_str.contains("username")); // Simple auth has no username
    }

    #[test]
    fn test_generate_auth_acl() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Auth {
            username: Some("myuser".to_string()),
            password: "mypassword".to_string(),
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert_eq!(request.request_id, (0, 0));
        assert!(request_str.starts_with("*3\r\n")); // 3 elements for ACL auth
        assert!(request_str.contains("AUTH"));
        assert!(request_str.contains("myuser"));
        assert!(request_str.contains("mypassword"));
    }

    #[test]
    fn test_generate_select_db() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SelectDb { db: 5 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("SELECT"));
        assert!(request_str.contains("5"));
    }

    #[test]
    fn test_generate_hello_resp2() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Hello { version: 2 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("HELLO"));
        assert!(request_str.contains("2"));
    }

    #[test]
    fn test_generate_hello_resp3() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Hello { version: 3 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("HELLO"));
        assert!(request_str.contains("3"));
    }

    #[test]
    fn test_generate_setex() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SetEx { ttl_seconds: 300 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 100, 10);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n")); // 4 elements: SETEX key ttl value
        assert!(request_str.contains("SETEX"));
        assert!(request_str.contains("key:100"));
        assert!(request_str.contains("300")); // TTL
        assert!(request_str.contains("xxxxxxxxxx")); // 10 x's
    }

    #[test]
    fn test_generate_setrange() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SetRange { offset: 5 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 100, 3);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("SETRANGE"));
        assert!(request_str.contains("key:100"));
        assert!(request_str.contains("5")); // offset
        assert!(request_str.contains("xxx")); // 3 x's
    }

    #[test]
    fn test_generate_getrange() {
        let selector =
            Box::new(FixedCommandSelector::new(RedisOp::GetRange { offset: 10, end: -1 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 100, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("GETRANGE"));
        assert!(request_str.contains("key:100"));
        assert!(request_str.contains("10")); // offset
        assert!(request_str.contains("-1")); // end
    }

    #[test]
    fn test_generate_cluster_slots() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::ClusterSlots));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("CLUSTER"));
        assert!(request_str.contains("SLOTS"));
    }

    // RESP3 response parsing tests

    #[test]
    fn test_parse_resp3_null() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"_\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_resp3_boolean_true() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"#t\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 4);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_resp3_boolean_false() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"#f\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 4);
    }

    #[test]
    fn test_parse_resp3_double() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b",3.14159\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 10);
    }

    #[test]
    fn test_parse_resp3_big_number() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"(3492890328409238509324850943850943825024385\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 46); // 1 + 43 digits + 2 (\r\n)
    }

    #[test]
    fn test_parse_resp3_blob_error() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"!21\r\nSYNTAX invalid syntax\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 28);
    }

    #[test]
    fn test_parse_resp3_verbatim_string() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"=15\r\ntxt:Some string\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 22); // =15\r\n (4) + txt:Some string (15) + \r\n (2) + 1 = 22
    }

    #[test]
    fn test_parse_resp3_map() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Map with count 2 (current impl treats as 2 elements, not 2 pairs)
        let response = b"%2\r\n+k1\r\n+v1\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 14); // %2\r\n (4) + +k1\r\n (5) + +v1\r\n (5) = 14
    }

    #[test]
    fn test_parse_resp3_set() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Complete set with 5 elements
        let response = b"~5\r\n+a\r\n+b\r\n+c\r\n+d\r\n+e\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 24); // ~5\r\n (4) + 5 * +X\r\n (4 each)
    }

    #[test]
    fn test_parse_resp3_attribute() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        // Attribute with count 1 (current impl treats as 1 element)
        let response = b"|1\r\n+key\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, _) = result.unwrap();
        assert_eq!(consumed, 10); // |1\r\n (4) + +key\r\n (6) = 10
    }

    #[test]
    fn test_parse_resp3_incomplete_null() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"_\r"; // Missing \n
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0); // Incomplete
        assert_eq!(id, None);
    }

    #[test]
    fn test_parse_resp3_incomplete_boolean() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let mut protocol = RedisProtocol::new(selector);

        let response = b"#t"; // Missing \r\n
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 0); // Incomplete
        assert_eq!(id, None);
    }

    #[test]
    fn test_resp_version_default() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let protocol = RedisProtocol::new(selector);

        assert_eq!(protocol.resp_version, 2); // Default to RESP2
    }

    #[test]
    fn test_generate_set_with_imported_data() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        let key = "user:1000";
        let value = b"alice";
        let request = protocol.generate_set_with_imported_data(0, key, value);

        // Verify request ID
        assert_eq!(request.request_id, (0, 0));

        // Verify RESP format: *3\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<vallen>\r\n<value>\r\n
        let request_str = String::from_utf8_lossy(&request.data);
        assert!(request_str.starts_with("*3\r\n")); // Array with 3 elements
        assert!(request_str.contains("$3\r\nSET\r\n")); // SET command
        assert!(request_str.contains(&format!("${}\r\n{}\r\n", key.len(), key))); // Key
        assert!(request_str.contains(&format!("${}\r\n", value.len()))); // Value length
        assert!(request_str.contains("alice")); // Value content
    }

    #[test]
    fn test_generate_set_with_imported_data_binary() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        let key = "session:abc";
        let value = b"\x00\x01\x02\x03\xff\xfe"; // Binary data
        let request = protocol.generate_set_with_imported_data(1, key, value);

        // Verify request ID
        assert_eq!(request.request_id, (1, 0));

        // Verify RESP format is correct for binary data
        let request_str = String::from_utf8_lossy(&request.data);
        assert!(request_str.starts_with("*3\r\n$3\r\nSET\r\n"));
        assert!(request_str.contains(&format!("${}\r\n{}\r\n", key.len(), key)));
        assert!(request_str.contains(&format!("${}\r\n", value.len())));

        // Verify binary data is preserved
        assert!(request.data.len() > 30); // Should have all the data
    }

    #[test]
    fn test_generate_set_with_imported_data_empty_value() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        let key = "empty:key";
        let value = b"";
        let request = protocol.generate_set_with_imported_data(0, key, value);

        assert_eq!(request.request_id, (0, 0));

        // Should still be valid RESP with $0\r\n\r\n for empty value
        let request_str = String::from_utf8_lossy(&request.data);
        assert!(request_str.contains("$0\r\n\r\n"));
    }

    #[test]
    fn test_generate_set_with_imported_data_sequence() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        // Generate multiple requests, verify sequence numbers increment
        let request1 = protocol.generate_set_with_imported_data(0, "key1", b"val1");
        let request2 = protocol.generate_set_with_imported_data(0, "key2", b"val2");
        let request3 = protocol.generate_set_with_imported_data(0, "key3", b"val3");

        assert_eq!(request1.request_id, (0, 0));
        assert_eq!(request2.request_id, (0, 1));
        assert_eq!(request3.request_id, (0, 2));
    }

    #[test]
    fn test_generate_set_with_imported_data_multiple_connections() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        // Different connections should have independent sequences
        let request1 = protocol.generate_set_with_imported_data(0, "key1", b"val1");
        let request2 = protocol.generate_set_with_imported_data(1, "key2", b"val2");
        let request3 = protocol.generate_set_with_imported_data(0, "key3", b"val3");

        assert_eq!(request1.request_id, (0, 0));
        assert_eq!(request2.request_id, (1, 0)); // Conn 1, sequence 0
        assert_eq!(request3.request_id, (0, 1)); // Conn 0, sequence 1
    }

    #[test]
    fn test_generate_scan_basic() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Scan {
            cursor: 0,
            count: None,
            pattern: None,
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n")); // 2 elements: SCAN + cursor
        assert!(request_str.contains("$4\r\nSCAN\r\n"));
        assert!(request_str.contains("$1\r\n0\r\n")); // cursor = 0
        assert!(!request_str.contains("COUNT"));
        assert!(!request_str.contains("MATCH"));
    }

    #[test]
    fn test_generate_scan_with_count() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Scan {
            cursor: 10,
            count: Some(100),
            pattern: None,
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n")); // 4 elements: SCAN + cursor + COUNT + count
        assert!(request_str.contains("SCAN"));
        assert!(request_str.contains("10")); // cursor
        assert!(request_str.contains("COUNT"));
        assert!(request_str.contains("100")); // count value
    }

    #[test]
    fn test_generate_scan_with_pattern() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Scan {
            cursor: 5,
            count: None,
            pattern: Some("user:*".to_string()),
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n")); // 4 elements: SCAN + cursor + MATCH + pattern
        assert!(request_str.contains("SCAN"));
        assert!(request_str.contains("5")); // cursor
        assert!(request_str.contains("MATCH"));
        assert!(request_str.contains("user:*")); // pattern
    }

    #[test]
    fn test_generate_scan_with_count_and_pattern() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Scan {
            cursor: 100,
            count: Some(50),
            pattern: Some("key:*".to_string()),
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*6\r\n")); // 6 elements: SCAN + cursor + COUNT + count + MATCH + pattern
        assert!(request_str.contains("SCAN"));
        assert!(request_str.contains("100")); // cursor
        assert!(request_str.contains("COUNT"));
        assert!(request_str.contains("50")); // count value
        assert!(request_str.contains("MATCH"));
        assert!(request_str.contains("key:*")); // pattern
    }

    #[test]
    fn test_generate_scan_sequence() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Scan {
            cursor: 0,
            count: Some(10),
            pattern: None,
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 1, 64);
        let request2 = protocol.generate_request_with_key(0, 2, 64);
        let request3 = protocol.generate_request_with_key(0, 3, 64);

        assert_eq!(request.request_id, (0, 0));
        assert_eq!(request2.request_id, (0, 1));
        assert_eq!(request3.request_id, (0, 2));
    }

    #[test]
    fn test_parse_scan_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Scan {
            cursor: 0,
            count: None,
            pattern: None,
        }));
        let mut protocol = RedisProtocol::new(selector);

        // SCAN returns an array with 2 elements: [cursor, [key1, key2, ...]]
        // Complete response: *2\r\n:0\r\n*2\r\n+key1\r\n+key2\r\n
        let response = b"*2\r\n:0\r\n*2\r\n+key1\r\n+key2\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 26); // *2\r\n (4) + :0\r\n (4) + *2\r\n (4) + +key1\r\n (7) + +key2\r\n (7) = 26
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_generate_multi() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Multi));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*1\r\n")); // 1 element: MULTI
        assert!(request_str.contains("$5\r\nMULTI\r\n"));
    }

    #[test]
    fn test_generate_exec() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Exec));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*1\r\n")); // 1 element: EXEC
        assert!(request_str.contains("$4\r\nEXEC\r\n"));
    }

    #[test]
    fn test_generate_discard() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Discard));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*1\r\n")); // 1 element: DISCARD
        assert!(request_str.contains("$7\r\nDISCARD\r\n"));
    }

    #[test]
    fn test_transaction_state_tracking() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Multi));
        let mut protocol = RedisProtocol::new(selector);

        // Initially not in transaction
        assert!(!protocol.is_in_transaction(0));

        // After MULTI, should be in transaction
        protocol.generate_request_with_key(0, 42, 64);
        assert!(protocol.is_in_transaction(0));

        // Switch to EXEC command
        protocol.command_selector = Box::new(FixedCommandSelector::new(RedisOp::Exec));
        protocol.generate_request_with_key(0, 42, 64);

        // After EXEC, should not be in transaction
        assert!(!protocol.is_in_transaction(0));
    }

    #[test]
    fn test_transaction_state_with_discard() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Multi));
        let mut protocol = RedisProtocol::new(selector);

        // Start transaction
        protocol.generate_request_with_key(0, 42, 64);
        assert!(protocol.is_in_transaction(0));

        // Switch to DISCARD command
        protocol.command_selector = Box::new(FixedCommandSelector::new(RedisOp::Discard));
        protocol.generate_request_with_key(0, 42, 64);

        // After DISCARD, should not be in transaction
        assert!(!protocol.is_in_transaction(0));
    }

    #[test]
    fn test_transaction_state_per_connection() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Multi));
        let mut protocol = RedisProtocol::new(selector);

        // Start transaction on connection 0
        protocol.generate_request_with_key(0, 42, 64);
        assert!(protocol.is_in_transaction(0));
        assert!(!protocol.is_in_transaction(1)); // Connection 1 not in transaction

        // Start transaction on connection 1
        protocol.generate_request_with_key(1, 42, 64);
        assert!(protocol.is_in_transaction(0));
        assert!(protocol.is_in_transaction(1));

        // End transaction on connection 0
        protocol.command_selector = Box::new(FixedCommandSelector::new(RedisOp::Exec));
        protocol.generate_request_with_key(0, 42, 64);
        assert!(!protocol.is_in_transaction(0));
        assert!(protocol.is_in_transaction(1)); // Connection 1 still in transaction
    }

    #[test]
    fn test_parse_multi_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Multi));
        let mut protocol = RedisProtocol::new(selector);

        // MULTI responds with +OK
        let response = b"+OK\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 5);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_queued_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let mut protocol = RedisProtocol::new(selector);

        // Commands in transaction respond with +QUEUED
        let response = b"+QUEUED\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 9);
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_parse_exec_response() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Exec));
        let mut protocol = RedisProtocol::new(selector);

        // EXEC responds with an array of results
        // Complete response: *2\r\n+OK\r\n:1\r\n (array with 2 elements: simple string and integer)
        let response = b"*2\r\n+OK\r\n:1\r\n";
        let result = protocol.parse_response(0, response);

        assert!(result.is_ok());
        let (consumed, id) = result.unwrap();
        assert_eq!(consumed, 13); // *2\r\n (4) + +OK\r\n (5) + :1\r\n (4) = 13
        assert_eq!(id, Some((0, 0)));
    }

    #[test]
    fn test_transaction_reset() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Multi));
        let mut protocol = RedisProtocol::new(selector);

        // Start transactions on multiple connections
        protocol.generate_request_with_key(0, 42, 64);
        protocol.generate_request_with_key(1, 42, 64);
        assert!(protocol.is_in_transaction(0));
        assert!(protocol.is_in_transaction(1));

        // Reset should clear all transaction state
        protocol.reset();
        assert!(!protocol.is_in_transaction(0));
        assert!(!protocol.is_in_transaction(1));
    }

    #[test]
    fn test_command_tracking() {
        use command_selector::FixedCommandSelector;

        // Test GET command tracking
        let mut protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Get)));

        // Generate some requests
        for _ in 0..10 {
            protocol.generate_request_with_key(0, 123, 100);
        }

        let stats = protocol.get_command_stats();
        assert_eq!(stats.get("GET"), Some(&10));
        assert_eq!(stats.len(), 1);

        // Reset and verify
        protocol.reset();
        let stats = protocol.get_command_stats();
        assert_eq!(stats.len(), 0);
    }

    #[test]
    fn test_command_tracking_multiple_commands() {
        use command_selector::WeightedCommandSelector;

        // Create weighted selector with GET and SET
        let commands = vec![(RedisOp::Get, 0.7), (RedisOp::Set, 0.3)];
        let selector = WeightedCommandSelector::with_seed(commands, Some(42)).unwrap();
        let mut protocol = RedisProtocol::new(Box::new(selector));

        // Generate 100 requests
        for i in 0..100 {
            protocol.generate_request_with_key(0, i as u64, 100);
        }

        let stats = protocol.get_command_stats();

        // Should have both GET and SET
        assert!(stats.contains_key("GET"));
        assert!(stats.contains_key("SET"));

        // Total should be 100
        let total: u64 = stats.values().sum();
        assert_eq!(total, 100);

        // GET should be majority with 70/30 split
        let get_count = *stats.get("GET").unwrap();
        let set_count = *stats.get("SET").unwrap();
        assert!(get_count > set_count);
    }

    #[test]
    fn test_command_tracking_special_commands() {
        use command_selector::FixedCommandSelector;

        // Test MGET
        let mut protocol =
            RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::MGet { count: 5 })));
        protocol.generate_request_with_key(0, 123, 100);
        assert_eq!(protocol.get_command_stats().get("MGET"), Some(&1));

        // Test INCR
        let mut protocol = RedisProtocol::new(Box::new(FixedCommandSelector::new(RedisOp::Incr)));
        protocol.generate_request_with_key(0, 123, 100);
        assert_eq!(protocol.get_command_stats().get("INCR"), Some(&1));
    }

    // Tests for new commands

    #[test]
    fn test_generate_del_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::Del));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("DEL"));
        assert!(request_str.contains("key:42"));
    }

    #[test]
    fn test_generate_mset_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::MSet { count: 3 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 10, 5);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*7\r\n")); // MSET + 3 * (key + value)
        assert!(request_str.contains("MSET"));
        assert!(request_str.contains("key:10"));
        assert!(request_str.contains("key:11"));
        assert!(request_str.contains("key:12"));
        assert!(request_str.contains("xxxxx")); // 5 bytes of value
    }

    #[test]
    fn test_generate_lpush_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::LPush));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 10);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("LPUSH"));
        assert!(request_str.contains("list:42"));
        assert!(request_str.contains("xxxxxxxxxx")); // 10 bytes of value
    }

    #[test]
    fn test_generate_rpush_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::RPush));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 10);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("RPUSH"));
        assert!(request_str.contains("list:42"));
    }

    #[test]
    fn test_generate_lpop_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::LPop));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("LPOP"));
        assert!(request_str.contains("list:42"));
    }

    #[test]
    fn test_generate_rpop_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::RPop));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("RPOP"));
        assert!(request_str.contains("list:42"));
    }

    #[test]
    fn test_generate_lrange_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::LRange { start: 0, stop: 10 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("LRANGE"));
        assert!(request_str.contains("list:42"));
        assert!(request_str.contains("0")); // start
        assert!(request_str.contains("10")); // stop
    }

    #[test]
    fn test_generate_llen_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::LLen));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("LLEN"));
        assert!(request_str.contains("list:42"));
    }

    #[test]
    fn test_generate_sadd_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SAdd));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("SADD"));
        assert!(request_str.contains("set:42"));
        assert!(request_str.contains("member:42"));
    }

    #[test]
    fn test_generate_srem_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SRem));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("SREM"));
        assert!(request_str.contains("set:42"));
        assert!(request_str.contains("member:42"));
    }

    #[test]
    fn test_generate_smembers_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SMembers));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("SMEMBERS"));
        assert!(request_str.contains("set:42"));
    }

    #[test]
    fn test_generate_sismember_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SIsMember));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("SISMEMBER"));
        assert!(request_str.contains("set:42"));
        assert!(request_str.contains("member:42"));
    }

    #[test]
    fn test_generate_spop_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SPop { count: None }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("SPOP"));
        assert!(request_str.contains("set:42"));
    }

    #[test]
    fn test_generate_spop_with_count_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SPop { count: Some(5) }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("SPOP"));
        assert!(request_str.contains("set:42"));
        assert!(request_str.contains("5"));
    }

    #[test]
    fn test_generate_scard_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::SCard));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("SCARD"));
        assert!(request_str.contains("set:42"));
    }

    #[test]
    fn test_generate_zadd_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::ZAdd { score: 1.5 }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("ZADD"));
        assert!(request_str.contains("zset:42"));
        assert!(request_str.contains("1.5"));
        assert!(request_str.contains("member:42"));
    }

    #[test]
    fn test_generate_zrem_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::ZRem));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("ZREM"));
        assert!(request_str.contains("zset:42"));
        assert!(request_str.contains("member:42"));
    }

    #[test]
    fn test_generate_zrange_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::ZRange {
            start: 0,
            stop: 10,
            with_scores: false,
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("ZRANGE"));
        assert!(request_str.contains("zset:42"));
        assert!(!request_str.contains("WITHSCORES"));
    }

    #[test]
    fn test_generate_zrange_with_scores_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::ZRange {
            start: 0,
            stop: -1,
            with_scores: true,
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*5\r\n"));
        assert!(request_str.contains("ZRANGE"));
        assert!(request_str.contains("zset:42"));
        assert!(request_str.contains("WITHSCORES"));
    }

    #[test]
    fn test_generate_zrangebyscore_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::ZRangeByScore {
            min: 0.0,
            max: 100.0,
            with_scores: false,
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("ZRANGEBYSCORE"));
        assert!(request_str.contains("zset:42"));
    }

    #[test]
    fn test_generate_zscore_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::ZScore));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("ZSCORE"));
        assert!(request_str.contains("zset:42"));
        assert!(request_str.contains("member:42"));
    }

    #[test]
    fn test_generate_zcard_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::ZCard));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("ZCARD"));
        assert!(request_str.contains("zset:42"));
    }

    #[test]
    fn test_generate_hset_request() {
        let selector =
            Box::new(FixedCommandSelector::new(RedisOp::HSet { field: "field1".to_string() }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 10);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n"));
        assert!(request_str.contains("HSET"));
        assert!(request_str.contains("hash:42"));
        assert!(request_str.contains("field1"));
        assert!(request_str.contains("xxxxxxxxxx")); // 10 bytes of value
    }

    #[test]
    fn test_generate_hget_request() {
        let selector =
            Box::new(FixedCommandSelector::new(RedisOp::HGet { field: "field1".to_string() }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("HGET"));
        assert!(request_str.contains("hash:42"));
        assert!(request_str.contains("field1"));
    }

    #[test]
    fn test_generate_hmset_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::HMSet {
            fields: vec!["f1".to_string(), "f2".to_string(), "f3".to_string()],
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 5);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*8\r\n")); // HMSET + key + 3 * (field + value)
        assert!(request_str.contains("HMSET"));
        assert!(request_str.contains("hash:42"));
        assert!(request_str.contains("f1"));
        assert!(request_str.contains("f2"));
        assert!(request_str.contains("f3"));
    }

    #[test]
    fn test_generate_hmget_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::HMGet {
            fields: vec!["f1".to_string(), "f2".to_string()],
        }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*4\r\n")); // HMGET + key + 2 fields
        assert!(request_str.contains("HMGET"));
        assert!(request_str.contains("hash:42"));
        assert!(request_str.contains("f1"));
        assert!(request_str.contains("f2"));
    }

    #[test]
    fn test_generate_hgetall_request() {
        let selector = Box::new(FixedCommandSelector::new(RedisOp::HGetAll));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*2\r\n"));
        assert!(request_str.contains("HGETALL"));
        assert!(request_str.contains("hash:42"));
    }

    #[test]
    fn test_generate_hdel_request() {
        let selector =
            Box::new(FixedCommandSelector::new(RedisOp::HDel { field: "field1".to_string() }));
        let mut protocol = RedisProtocol::new(selector);

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.starts_with("*3\r\n"));
        assert!(request_str.contains("HDEL"));
        assert!(request_str.contains("hash:42"));
        assert!(request_str.contains("field1"));
    }

    #[test]
    fn test_custom_key_prefix() {
        use crate::workload::KeyGeneration;

        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let key_gen = KeyGeneration::sequential(0);
        let mut protocol = RedisProtocol::with_workload_and_options(
            selector,
            key_gen,
            64,
            "memtier-".to_string(),
            false,
            None,
        );

        let request = protocol.generate_request_with_key(0, 42, 64);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.contains("memtier-42"), "Expected custom key prefix 'memtier-'");
        assert!(!request_str.contains("key:"), "Should not contain default prefix 'key:'");
    }

    #[test]
    fn test_custom_key_prefix_set() {
        use crate::workload::KeyGeneration;

        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let key_gen = KeyGeneration::sequential(0);
        let mut protocol = RedisProtocol::with_workload_and_options(
            selector,
            key_gen,
            10,
            "test:".to_string(),
            false,
            None,
        );

        let request = protocol.generate_request_with_key(0, 100, 10);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.contains("test:100"), "Expected custom key prefix 'test:'");
        assert!(request_str.contains("xxxxxxxxxx"), "Expected 10 x's for value");
    }

    #[test]
    fn test_random_data_generation() {
        use crate::workload::KeyGeneration;

        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let key_gen = KeyGeneration::sequential(0);
        let mut protocol = RedisProtocol::with_workload_and_options(
            selector,
            key_gen,
            100,
            "key:".to_string(),
            true,     // Enable random data
            Some(42), // Use fixed seed for reproducibility
        );

        let request1 = protocol.generate_request_with_key(0, 1, 100);
        let request_str1 = String::from_utf8_lossy(&request1.data);

        // Random data should NOT contain all 'x's
        assert!(
            !request_str1.contains("xxxxxxxxxxxxxxxxxxxx"),
            "Random data should not be all x's"
        );

        // Should still contain the key
        assert!(request_str1.contains("key:1"));
    }

    #[test]
    fn test_random_data_reproducibility() {
        use crate::workload::KeyGeneration;

        // Create two protocols with the same seed
        let selector1 = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let key_gen1 = KeyGeneration::sequential(0);
        let mut protocol1 = RedisProtocol::with_workload_and_options(
            selector1,
            key_gen1,
            50,
            "key:".to_string(),
            true,
            Some(12345),
        );

        let selector2 = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let key_gen2 = KeyGeneration::sequential(0);
        let mut protocol2 = RedisProtocol::with_workload_and_options(
            selector2,
            key_gen2,
            50,
            "key:".to_string(),
            true,
            Some(12345),
        );

        // Generate requests - they should be identical with the same seed
        let request1 = protocol1.generate_request_with_key(0, 1, 50);
        let request2 = protocol2.generate_request_with_key(0, 1, 50);

        assert_eq!(request1, request2, "Same seed should produce same random data");
    }

    #[test]
    fn test_key_prefix_with_data_structures() {
        use crate::workload::KeyGeneration;

        // Test that key prefix applies to list operations
        let selector = Box::new(FixedCommandSelector::new(RedisOp::LPush));
        let key_gen = KeyGeneration::sequential(0);
        let mut protocol = RedisProtocol::with_workload_and_options(
            selector,
            key_gen,
            10,
            "myapp:".to_string(),
            false,
            None,
        );

        let request = protocol.generate_request_with_key(0, 42, 10);
        let request_str = String::from_utf8_lossy(&request.data);

        assert!(request_str.contains("myapp:list:42"), "List key should have custom prefix");
    }

    #[test]
    fn test_key_prefix_accessor() {
        use crate::workload::KeyGeneration;

        let selector = Box::new(FixedCommandSelector::new(RedisOp::Get));
        let key_gen = KeyGeneration::sequential(0);
        let protocol = RedisProtocol::with_workload_and_options(
            selector,
            key_gen,
            64,
            "custom:".to_string(),
            false,
            None,
        );

        assert_eq!(protocol.key_prefix(), "custom:");
        assert!(!protocol.random_data());
    }

    #[test]
    fn test_random_data_accessor() {
        use crate::workload::KeyGeneration;

        let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
        let key_gen = KeyGeneration::sequential(0);
        let protocol = RedisProtocol::with_workload_and_options(
            selector,
            key_gen,
            64,
            "key:".to_string(),
            true,
            Some(42),
        );

        assert!(protocol.random_data());
    }
}
