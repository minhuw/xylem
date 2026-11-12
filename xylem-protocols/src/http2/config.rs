//! HTTP/2 and gRPC protocol configuration

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// HTTP/2 protocol configuration: every request is the same, like HTTP/1.1's
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Http2Config {
    /// HTTP method: GET, POST or PUT (default: GET)
    #[serde(default = "default_method")]
    pub method: String,
    /// Request path (default: "/")
    #[serde(default = "default_path")]
    pub path: String,
    /// `:authority` of every request (default: the group's target, or
    /// "localhost" over a Unix socket)
    #[serde(default)]
    pub authority: Option<String>,
    /// Bytes of the body of a POST or PUT (default: 64)
    #[serde(default = "default_body_size")]
    pub body_size: usize,
}

fn default_method() -> String {
    "GET".to_string()
}

fn default_path() -> String {
    "/".to_string()
}

fn default_body_size() -> usize {
    64
}

impl Default for Http2Config {
    fn default() -> Self {
        Self {
            method: default_method(),
            path: default_path(),
            authority: None,
            body_size: default_body_size(),
        }
    }
}

/// gRPC protocol configuration: unary calls of one method
///
/// The request message is raw protobuf bytes from at most one of
/// `request_file`, `request_hex`, `request_base64` and `messages_file`;
/// without any, it is the empty message, all of whose fields have their
/// defaults. Relative paths are relative to the working directory.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    /// Full method name, "package.Service/Method", as in
    /// "tensorflow.serving.PredictionService/Predict"
    pub call: String,
    /// `:authority` of every call (default: the group's target, or
    /// "localhost" over a Unix socket)
    #[serde(default)]
    pub authority: Option<String>,
    /// File holding the serialized request message
    #[serde(default)]
    pub request_file: Option<PathBuf>,
    /// The serialized request message in hexadecimal
    #[serde(default)]
    pub request_hex: Option<String>,
    /// The serialized request message in standard base64
    #[serde(default)]
    pub request_base64: Option<String>,
    /// File holding several request messages, each with gRPC's 5-byte
    /// prefix (a zero compression flag and the big-endian length); each
    /// thread sends them in turn
    #[serde(default)]
    pub messages_file: Option<PathBuf>,
    /// Custom metadata sent with every call, by lowercase name
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}
