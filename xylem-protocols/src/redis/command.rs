//! Redis commands and their RESP encoding

use super::command_template::CommandTemplate;
use super::resp::encode_command;
use super::RedisProtocol;

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

impl RedisOp {
    /// Canonical command name used for statistics and per-command value sizing.
    pub fn name(&self) -> &'static str {
        match self {
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
        }
    }
}

impl RedisProtocol {
    /// Format a Redis command into RESP bytes
    pub(super) fn format_command(
        &mut self,
        operation: &RedisOp,
        key: u64,
        value_size: usize,
    ) -> Vec<u8> {
        match operation {
            RedisOp::Get => {
                let key_str = self.format_key(key);
                encode_command(&["GET", &key_str])
            }
            RedisOp::Set => {
                let key_str = self.format_key(key);
                let value = self.generate_value(value_size);
                encode_command(&["SET", &key_str, &value])
            }
            RedisOp::Incr => {
                let key_str = self.format_key(key);
                encode_command(&["INCR", &key_str])
            }
            RedisOp::MGet { count } => {
                // Use saturating_add to prevent overflow - keys wrap to max value
                let keys: Vec<String> =
                    (0..*count).map(|i| self.format_key(key.saturating_add(i as u64))).collect();
                let mut args = vec!["MGET"];
                args.extend(keys.iter().map(String::as_str));
                encode_command(&args)
            }
            RedisOp::Wait { num_replicas, timeout_ms } => {
                let num_str = num_replicas.to_string();
                let timeout_str = timeout_ms.to_string();
                encode_command(&["WAIT", &num_str, &timeout_str])
            }
            RedisOp::Auth { username, password } => {
                if let Some(user) = username {
                    encode_command(&["AUTH", user, password])
                } else {
                    encode_command(&["AUTH", password])
                }
            }
            RedisOp::SelectDb { db } => {
                let db_str = db.to_string();
                encode_command(&["SELECT", &db_str])
            }
            RedisOp::Hello { version } => {
                let version_str = version.to_string();
                encode_command(&["HELLO", &version_str])
            }
            RedisOp::SetEx { ttl_seconds } => {
                let key_str = self.format_key(key);
                let value = self.generate_value(value_size);
                let ttl_str = ttl_seconds.to_string();
                encode_command(&["SETEX", &key_str, &ttl_str, &value])
            }
            RedisOp::SetRange { offset } => {
                let key_str = self.format_key(key);
                let value = self.generate_value(value_size);
                let offset_str = offset.to_string();
                encode_command(&["SETRANGE", &key_str, &offset_str, &value])
            }
            RedisOp::GetRange { offset, end } => {
                let key_str = self.format_key(key);
                let offset_str = offset.to_string();
                let end_str = end.to_string();
                encode_command(&["GETRANGE", &key_str, &offset_str, &end_str])
            }
            RedisOp::ClusterSlots => encode_command(&["CLUSTER", "SLOTS"]),
            RedisOp::Scan { cursor, count, pattern } => {
                let cursor_str = cursor.to_string();
                let count_str = count.map(|c| c.to_string());
                let mut args = vec!["SCAN", cursor_str.as_str()];

                // Add COUNT if specified
                if let Some(count_str) = &count_str {
                    args.push("COUNT");
                    args.push(count_str);
                }

                // Add MATCH if specified
                if let Some(p) = pattern {
                    args.push("MATCH");
                    args.push(p);
                }

                encode_command(&args)
            }
            RedisOp::Multi => encode_command(&["MULTI"]),
            RedisOp::Exec => encode_command(&["EXEC"]),
            RedisOp::Discard => encode_command(&["DISCARD"]),
            // DEL command
            RedisOp::Del => {
                let key_str = self.format_key(key);
                encode_command(&["DEL", &key_str])
            }
            // MSET command
            RedisOp::MSet { count } => {
                let value = self.generate_value(value_size);
                // Use saturating_add to prevent overflow - keys wrap to max value
                let keys: Vec<String> =
                    (0..*count).map(|i| self.format_key(key.saturating_add(i as u64))).collect();
                let mut args = vec!["MSET"];
                for key_str in &keys {
                    args.push(key_str);
                    args.push(&value);
                }
                encode_command(&args)
            }
            // List operations
            RedisOp::LPush => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                let value = self.generate_value(value_size);
                encode_command(&["LPUSH", &key_str, &value])
            }
            RedisOp::RPush => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                let value = self.generate_value(value_size);
                encode_command(&["RPUSH", &key_str, &value])
            }
            RedisOp::LPop => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                encode_command(&["LPOP", &key_str])
            }
            RedisOp::RPop => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                encode_command(&["RPOP", &key_str])
            }
            RedisOp::LRange { start, stop } => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                let start_str = start.to_string();
                let stop_str = stop.to_string();
                encode_command(&["LRANGE", &key_str, &start_str, &stop_str])
            }
            RedisOp::LLen => {
                let key_str = format!("{}list:{}", self.key_prefix, key);
                encode_command(&["LLEN", &key_str])
            }
            // Set operations
            RedisOp::SAdd => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                encode_command(&["SADD", &key_str, &member])
            }
            RedisOp::SRem => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                encode_command(&["SREM", &key_str, &member])
            }
            RedisOp::SMembers => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                encode_command(&["SMEMBERS", &key_str])
            }
            RedisOp::SIsMember => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                encode_command(&["SISMEMBER", &key_str, &member])
            }
            RedisOp::SPop { count } => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                if let Some(c) = count {
                    let count_str = c.to_string();
                    encode_command(&["SPOP", &key_str, &count_str])
                } else {
                    encode_command(&["SPOP", &key_str])
                }
            }
            RedisOp::SCard => {
                let key_str = format!("{}set:{}", self.key_prefix, key);
                encode_command(&["SCARD", &key_str])
            }
            // Sorted set operations
            RedisOp::ZAdd { score } => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                let score_str = score.to_string();
                encode_command(&["ZADD", &key_str, &score_str, &member])
            }
            RedisOp::ZRem => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                encode_command(&["ZREM", &key_str, &member])
            }
            RedisOp::ZRange { start, stop, with_scores } => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let start_str = start.to_string();
                let stop_str = stop.to_string();
                if *with_scores {
                    encode_command(&["ZRANGE", &key_str, &start_str, &stop_str, "WITHSCORES"])
                } else {
                    encode_command(&["ZRANGE", &key_str, &start_str, &stop_str])
                }
            }
            RedisOp::ZRangeByScore { min, max, with_scores } => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let min_str = min.to_string();
                let max_str = max.to_string();
                if *with_scores {
                    encode_command(&["ZRANGEBYSCORE", &key_str, &min_str, &max_str, "WITHSCORES"])
                } else {
                    encode_command(&["ZRANGEBYSCORE", &key_str, &min_str, &max_str])
                }
            }
            RedisOp::ZScore => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                let member = format!("member:{}", key);
                encode_command(&["ZSCORE", &key_str, &member])
            }
            RedisOp::ZCard => {
                let key_str = format!("{}zset:{}", self.key_prefix, key);
                encode_command(&["ZCARD", &key_str])
            }
            // Hash operations
            RedisOp::HSet { field } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                let value = self.generate_value(value_size);
                encode_command(&["HSET", &key_str, field, &value])
            }
            RedisOp::HGet { field } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                encode_command(&["HGET", &key_str, field])
            }
            RedisOp::HMSet { fields } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                let value = self.generate_value(value_size);
                // HMSET key field value [field value ...]
                let mut args = vec!["HMSET", key_str.as_str()];
                for field in fields {
                    args.push(field);
                    args.push(&value);
                }
                encode_command(&args)
            }
            RedisOp::HMGet { fields } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                // HMGET key field [field ...]
                let mut args = vec!["HMGET", key_str.as_str()];
                args.extend(fields.iter().map(String::as_str));
                encode_command(&args)
            }
            RedisOp::HGetAll => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                encode_command(&["HGETALL", &key_str])
            }
            RedisOp::HDel { field } => {
                let key_str = format!("{}hash:{}", self.key_prefix, key);
                encode_command(&["HDEL", &key_str, field])
            }
            RedisOp::Custom(template) => {
                // For custom templates, generate value for __data__ substitution
                let value = self.generate_value(value_size);
                template.generate_request_with_options(key, value_size, &self.key_prefix, &value)
            }
        }
    }
}
