//! Redis commands and their RESP encoding

use super::command_template::CommandTemplate;
use super::resp::encode_command;
use super::RedisProtocol;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisOp {
    Get,
    Set,
    Incr,
    /// Multi-get - fetch multiple keys at once
    MGet {
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
            RedisOp::MGet { .. } => "MGET",
            RedisOp::Wait { .. } => "WAIT",
            RedisOp::Auth { .. } => "AUTH",
            RedisOp::SelectDb { .. } => "SELECT",
            RedisOp::Hello { .. } => "HELLO",
            RedisOp::SetEx { .. } => "SETEX",
            RedisOp::SetRange { .. } => "SETRANGE",
            RedisOp::GetRange { .. } => "GETRANGE",
            RedisOp::Scan { .. } => "SCAN",
            RedisOp::Multi => "MULTI",
            RedisOp::Exec => "EXEC",
            RedisOp::Discard => "DISCARD",
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
            RedisOp::Custom(template) => {
                // For custom templates, generate value for __data__ substitution
                let value = self.generate_value(value_size);
                template.generate_request_with_options(key, value_size, &self.key_prefix, &value)
            }
        }
    }
}
