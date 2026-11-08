//! Connection setup: the AUTH, SELECT and HELLO commands every connection
//! sends once, in that order, before any measured or insert-phase request.
//!
//! The commands go out one at a time. While one is pending, the connection
//! cannot send, so its reply is the next reply on the connection. An error
//! reply fails the run. Setup requests are tagged as such, so the worker
//! neither measures nor dumps them.

use super::command::RedisOp;
use anyhow::Result;
use std::collections::HashMap;

/// The setup commands of every connection of a protocol instance
#[derive(Clone, Default)]
pub struct ConnectionSetup {
    steps: Vec<RedisOp>,
    /// Kept to strip from server error messages
    password: Option<String>,
}

impl std::fmt::Debug for ConnectionSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Step names only: AUTH carries the password.
        f.debug_struct("ConnectionSetup").field("steps", &self.step_names()).finish()
    }
}

impl ConnectionSetup {
    /// Build the setup sequence: AUTH with `auth` (username, password),
    /// SELECT `db`, then HELLO 3 if `resp` is 3. RESP2 is the default of a
    /// new connection, so `resp = 2` sends no HELLO.
    pub fn new(
        auth: Option<(Option<String>, String)>,
        db: Option<u32>,
        resp: Option<u8>,
    ) -> Result<Self> {
        let mut setup = Self::default();
        if let Some((username, password)) = auth {
            if let Some(username) = &username {
                anyhow::ensure!(!username.is_empty(), "Redis auth.username must not be empty");
            }
            setup.password = Some(password.clone()).filter(|password| !password.is_empty());
            setup.steps.push(RedisOp::Auth { username, password });
        }
        if let Some(db) = db {
            // Redis parses the index as a signed 32-bit integer.
            anyhow::ensure!(
                db <= i32::MAX as u32,
                "Redis db must be at most {}, got {db}",
                i32::MAX
            );
            setup.steps.push(RedisOp::SelectDb { db });
        }
        match resp {
            None | Some(2) => {}
            Some(3) => setup.steps.push(RedisOp::Hello { version: 3 }),
            Some(other) => anyhow::bail!("Redis resp must be 2 or 3, got {other}"),
        }
        Ok(setup)
    }

    /// True when connections need no setup
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// The commands in the order connections send them
    pub fn step_names(&self) -> Vec<&'static str> {
        self.steps.iter().map(RedisOp::name).collect()
    }

    pub(super) fn step(&self, index: usize) -> &RedisOp {
        &self.steps[index]
    }

    pub(super) fn len(&self) -> usize {
        self.steps.len()
    }

    /// A server error message with any occurrence of the password removed
    pub(super) fn redact(&self, message: &str) -> String {
        match &self.password {
            Some(password) => message.replace(password.as_str(), "<redacted>"),
            None => message.to_string(),
        }
    }
}

/// Setup progress of one connection. A connection without an entry has not
/// sent its first setup command yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SetupState {
    /// The reply to the setup command at this index is pending
    Awaiting(usize),
    /// The setup command at this index is the next to send
    Next(usize),
    /// Every setup command succeeded
    Done,
}

/// Per-connection setup progress
#[derive(Debug, Default)]
pub(super) struct SetupTracker {
    states: HashMap<usize, SetupState>,
}

impl SetupTracker {
    pub(super) fn state(&self, conn_id: usize) -> SetupState {
        self.states.get(&conn_id).copied().unwrap_or(SetupState::Next(0))
    }

    pub(super) fn set(&mut self, conn_id: usize, state: SetupState) {
        self.states.insert(conn_id, state);
    }

    pub(super) fn clear(&mut self) {
        self.states.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::command_selector::FixedCommandSelector;
    use crate::redis::RedisProtocol;
    use crate::workload::KeyGeneration;
    use crate::{Parsed, Protocol, Request};

    const AUTH: &[u8] = b"*3\r\n$4\r\nAUTH\r\n$5\r\nbench\r\n$6\r\npw-123\r\n";
    const SELECT: &[u8] = b"*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n";
    const HELLO: &[u8] = b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n";
    /// A HELLO 3 reply: a RESP3 map with a nested empty array
    const HELLO_REPLY: &[u8] = b"%3\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:3\r\n\
                                 $7\r\nmodules\r\n*0\r\n";

    fn full_setup() -> ConnectionSetup {
        ConnectionSetup::new(
            Some((Some("bench".to_string()), "pw-123".to_string())),
            Some(3),
            Some(3),
        )
        .unwrap()
    }

    /// GETs of keys 7, 8, ... with 3-byte values after `setup`
    fn protocol(op: RedisOp, setup: ConnectionSetup) -> RedisProtocol {
        RedisProtocol::with_workload_and_options(
            Box::new(FixedCommandSelector::new(op)),
            KeyGeneration::sequential(7),
            3,
            "key:".to_string(),
            false,
            None,
        )
        .with_connection_setup(setup)
    }

    /// Send the setup request expected next on `conn_id` and answer it
    fn complete_step(
        protocol: &mut RedisProtocol,
        conn_id: usize,
        seq: u64,
        request: &[u8],
        reply: &[u8],
    ) {
        assert!(protocol.can_send(conn_id));
        assert_eq!(
            protocol.next_request(conn_id),
            Request::setup(request.to_vec(), (conn_id, seq))
        );
        assert!(!protocol.can_send(conn_id), "nothing else goes out while setup is pending");
        assert_eq!(
            protocol.parse_response(conn_id, reply).unwrap(),
            Parsed::ok(reply.len(), (conn_id, seq))
        );
    }

    #[test]
    fn setup_sequence_is_built_from_config_values() {
        assert!(ConnectionSetup::new(None, None, None).unwrap().is_empty());
        assert!(
            ConnectionSetup::new(None, None, Some(2)).unwrap().is_empty(),
            "RESP2 needs no HELLO"
        );
        assert_eq!(full_setup().step_names(), ["AUTH", "SELECT", "HELLO"]);
        let requirepass = ConnectionSetup::new(Some((None, "pw".into())), None, None).unwrap();
        assert_eq!(requirepass.step_names(), ["AUTH"]);
        assert_eq!(
            ConnectionSetup::new(None, Some(0), Some(3)).unwrap().step_names(),
            ["SELECT", "HELLO"]
        );
        for (setup, error) in [
            (ConnectionSetup::new(None, None, Some(1)), "Redis resp must be 2 or 3, got 1"),
            (ConnectionSetup::new(None, Some(1 << 31), None), "Redis db must be at most"),
            (
                ConnectionSetup::new(Some((Some(String::new()), "pw".into())), None, None),
                "Redis auth.username must not be empty",
            ),
        ] {
            assert!(setup.unwrap_err().to_string().starts_with(error));
        }
        assert!(!format!("{:?}", full_setup()).contains("pw-123"));
    }

    #[test]
    fn setup_runs_one_command_at_a_time_before_insert_and_measured_requests() {
        let mut protocol = protocol(RedisOp::Get, full_setup()).with_insert_phase(1, 3);
        complete_step(&mut protocol, 0, 0, AUTH, b"+OK\r\n");
        // Another connection runs its own setup
        assert!(protocol.can_send(1));
        assert_eq!(protocol.next_request(1), Request::setup(AUTH.to_vec(), (1, 0)));
        assert!(!protocol.can_send(1));
        complete_step(&mut protocol, 0, 1, SELECT, b"+OK\r\n");
        complete_step(&mut protocol, 0, 2, HELLO, HELLO_REPLY);

        let insert = b"*3\r\n$3\r\nSET\r\n$5\r\nkey:7\r\n$3\r\nxxx\r\n";
        assert_eq!(protocol.next_request(0), Request::warmup(insert.to_vec(), (0, 3)));
        let get = b"*2\r\n$3\r\nGET\r\n$5\r\nkey:7\r\n";
        assert_eq!(protocol.next_request(0), Request::measurement(get.to_vec(), (0, 4)));
        // Pipelined RESP3 replies match in order
        let replies = b"+OK\r\n_\r\n";
        assert_eq!(protocol.parse_response(0, replies).unwrap(), Parsed::ok(5, (0, 3)));
        assert_eq!(protocol.parse_response(0, &replies[5..]).unwrap(), Parsed::ok(3, (0, 4)));

        // Connection 1 is still waiting for its AUTH reply
        assert!(!protocol.can_send(1));
        assert_eq!(protocol.parse_response(1, b"+OK\r\n").unwrap(), Parsed::ok(5, (1, 0)));
        assert_eq!(protocol.next_request(1), Request::setup(SELECT.to_vec(), (1, 1)));
    }

    #[test]
    fn transactions_start_after_setup() {
        let setup = ConnectionSetup::new(Some((None, "pw".into())), None, Some(3)).unwrap();
        let mut protocol = protocol(RedisOp::Multi, setup);
        complete_step(&mut protocol, 0, 0, b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n", b"+OK\r\n");
        complete_step(&mut protocol, 0, 1, HELLO, HELLO_REPLY);
        let multi = protocol.next_request(0);
        assert_eq!(multi, Request::measurement(b"*1\r\n$5\r\nMULTI\r\n".to_vec(), (0, 2)));
        assert_eq!(protocol.parse_response(0, b"+OK\r\n").unwrap(), Parsed::ok(5, (0, 2)));
    }

    #[test]
    fn setup_restarts_after_reset() {
        let setup = ConnectionSetup::new(None, Some(3), None).unwrap();
        let mut protocol = protocol(RedisOp::Get, setup);
        complete_step(&mut protocol, 0, 0, SELECT, b"+OK\r\n");
        assert!(!protocol.next_request(0).metadata.is_warmup);
        protocol.reset();
        complete_step(&mut protocol, 0, 0, SELECT, b"+OK\r\n");
    }

    #[test]
    fn without_setup_the_first_request_is_measured() {
        for setup in
            [ConnectionSetup::default(), ConnectionSetup::new(None, None, Some(2)).unwrap()]
        {
            let mut protocol = protocol(RedisOp::Get, setup);
            let get = b"*2\r\n$3\r\nGET\r\n$5\r\nkey:7\r\n";
            assert_eq!(protocol.next_request(0), Request::measurement(get.to_vec(), (0, 0)));
        }
    }

    #[test]
    fn setup_error_replies_fail_with_the_step_and_connection_but_no_password() {
        let cases: [(usize, &[u8], &str); 8] = [
            (
                0,
                b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
                "AUTH on connection 2 was rejected: WRONGPASS invalid username-password pair",
            ),
            (
                0,
                b"-ERR invalid password pw-123\r\n",
                "AUTH on connection 2 was rejected: ERR invalid password <redacted>",
            ),
            (
                0,
                b"!17\r\nERR blob pw-123!!\r\n",
                "AUTH on connection 2 was rejected: ERR blob <redacted>!!",
            ),
            (
                1,
                b"-NOAUTH Authentication required.\r\n",
                "SELECT on connection 2 was rejected: NOAUTH",
            ),
            (
                1,
                b"-ERR DB index is out of range\r\n",
                "SELECT on connection 2 was rejected: ERR DB index",
            ),
            (
                1,
                b"|1\r\n+key\r\n+value\r\n-NOPERM this user has no permissions\r\n",
                "SELECT on connection 2 was rejected: NOPERM",
            ),
            (
                2,
                b"-ERR unknown command 'HELLO', with args beginning with: '3' \r\n",
                "HELLO on connection 2 was rejected: ERR unknown command 'HELLO'",
            ),
            (
                2,
                b"-NOPROTO unsupported protocol version\r\n",
                "HELLO on connection 2 was rejected: NOPROTO",
            ),
        ];
        for (failing_step, error, expected) in cases {
            let mut protocol = protocol(RedisOp::Get, full_setup());
            for (seq, request) in [AUTH, SELECT].into_iter().enumerate().take(failing_step) {
                complete_step(&mut protocol, 2, seq as u64, request, b"+OK\r\n");
            }
            assert!(protocol.next_request(2).metadata.is_setup);
            let err = protocol.parse_response(2, error).unwrap_err().to_string();
            assert!(err.starts_with("Redis connection setup failed: "), "{err}");
            assert!(err.contains(expected), "{err}");
            assert!(!err.contains("pw-123"), "{err}");
        }
    }

    #[test]
    fn push_messages_during_setup_are_skipped() {
        let setup = ConnectionSetup::new(None, None, Some(3)).unwrap();
        let mut protocol = protocol(RedisOp::Get, setup);
        assert_eq!(protocol.next_request(0), Request::setup(HELLO.to_vec(), (0, 0)));
        let push = b">2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nkey:1\r\n";
        let data = [&push[..], HELLO_REPLY].concat();
        // The push alone, or the push and part of the reply, is incomplete
        assert_eq!(protocol.parse_response(0, push).unwrap(), Parsed::Incomplete);
        assert_eq!(
            protocol.parse_response(0, &data[..data.len() - 1]).unwrap(),
            Parsed::Incomplete
        );
        assert_eq!(protocol.push_messages(), 0, "counted once the reply completes");
        assert_eq!(protocol.parse_response(0, &data).unwrap(), Parsed::ok(data.len(), (0, 0)));
        assert_eq!(protocol.push_messages(), 1);
        assert!(protocol.can_send(0));
    }
}
