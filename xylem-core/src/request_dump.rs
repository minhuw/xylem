//! Request dump module for recording all requests sent during a benchmark.
//!
//! This module provides protocol and transport-agnostic request logging with support for:
//! - CSV output format with timestamp, thread_id, traffic_group_id, connection_id, request_data
//! - Time-based file rotation (minutely, hourly, daily)
//! - Size-based file rotation
//! - Thread-safe operation via per-thread writers
//!
//! # Example
//!
//! ```ignore
//! use xylem_core::request_dump::{RequestDumper, DumpConfig, RotationPolicy};
//!
//! let config = DumpConfig {
//!     directory: "/var/log/xylem".into(),
//!     prefix: "requests".to_string(),
//!     rotation: RotationPolicy::Hourly,
//!     max_files: Some(24), // Keep last 24 hours
//! };
//!
//! let dumper = RequestDumper::new(config)?;
//! dumper.record(thread_id, group_id, conn_id, &request_data, "req_001");
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Rotation policy for request dump files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum RotationPolicy {
    /// Never rotate - single file
    #[default]
    Never,
    /// Rotate every minute
    Minutely,
    /// Rotate every hour
    Hourly,
    /// Rotate every day
    Daily,
    /// Rotate when file exceeds size (in bytes)
    Size(u64),
}

/// Encoding method for request data in dump files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum DataEncoding {
    /// Hexadecimal encoding (safe for all binary data)
    #[default]
    Hex,
    /// Base64 encoding (more compact than hex)
    Base64,
    /// UTF-8 with lossy conversion (replaces invalid bytes with replacement char)
    /// Good for text protocols like HTTP, Redis text commands
    #[serde(rename = "utf8")]
    Utf8Lossy,
    /// Raw bytes as escaped string (e.g., \x00\x01)
    Escaped,
    /// No data encoding - only record metadata (data_len, no data column)
    None,
}

impl DataEncoding {
    /// Encode bytes according to the encoding method
    pub fn encode(&self, data: &[u8]) -> String {
        match self {
            DataEncoding::Hex => hex::encode(data),
            DataEncoding::Base64 => base64_encode(data),
            DataEncoding::Utf8Lossy => String::from_utf8_lossy(data).into_owned(),
            DataEncoding::Escaped => escape_bytes(data),
            DataEncoding::None => String::new(),
        }
    }

    /// Get the column header name for this encoding
    pub fn column_name(&self) -> &'static str {
        match self {
            DataEncoding::Hex => "data_hex",
            DataEncoding::Base64 => "data_base64",
            DataEncoding::Utf8Lossy => "data_utf8",
            DataEncoding::Escaped => "data_escaped",
            DataEncoding::None => "data",
        }
    }
}

/// Simple base64 encoding (no external dependency)
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}

/// Escape bytes as \xNN for non-printable characters
fn escape_bytes(data: &[u8]) -> String {
    let mut result = String::with_capacity(data.len() * 2);
    for &byte in data {
        if byte.is_ascii_graphic() || byte == b' ' {
            result.push(byte as char);
        } else {
            result.push_str(&format!("\\x{:02x}", byte));
        }
    }
    result
}

/// Escape a string field for CSV output (handles commas, quotes, newlines)
fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Configuration for request dumping
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DumpConfig {
    /// Directory to store dump files
    pub directory: PathBuf,
    /// Prefix for dump file names
    #[serde(default = "default_prefix")]
    pub prefix: String,
    /// Rotation policy
    #[serde(default)]
    pub rotation: RotationPolicy,
    /// Maximum number of rotated files to keep (None = unlimited)
    #[serde(default)]
    pub max_files: Option<usize>,
    /// Data encoding method
    #[serde(default)]
    pub encoding: DataEncoding,
}

fn default_prefix() -> String {
    "xylem_requests".to_string()
}

impl Default for DumpConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("."),
            prefix: "xylem_requests".to_string(),
            rotation: RotationPolicy::Never,
            max_files: None,
            encoding: DataEncoding::default(),
        }
    }
}

/// Internal state for a single dump file writer
struct DumpWriter {
    writer: BufWriter<File>,
    #[allow(dead_code)] // Kept for debugging/logging purposes
    file_path: PathBuf,
    bytes_written: u64,
    rotation_key: String, // For time-based rotation (e.g., "2024-01-15-14" for hourly)
}

impl DumpWriter {
    fn new(path: PathBuf) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("Failed to open dump file: {:?}", path))?;

        let bytes_written = file.metadata().map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            writer: BufWriter::with_capacity(64 * 1024, file), // 64KB buffer
            file_path: path,
            bytes_written,
            rotation_key: String::new(),
        })
    }

    fn write_header(&mut self, encoding: &DataEncoding) -> Result<()> {
        if self.bytes_written == 0 {
            let header = format!(
                "timestamp_ns,thread_id,group_id,conn_id,request_id,data_len,{}\n",
                encoding.column_name()
            );
            self.writer.write_all(header.as_bytes())?;
            self.bytes_written += header.len() as u64;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn write_record(
        &mut self,
        timestamp_ns: u64,
        thread_id: usize,
        group_id: usize,
        conn_id: usize,
        request_id: &str,
        data: &[u8],
        encoding: &DataEncoding,
    ) -> Result<usize> {
        let encoded_data = encoding.encode(data);

        // Escape request_id for CSV (handle commas, quotes, newlines)
        let escaped_request_id = escape_csv_field(request_id);

        // Escape the encoded data for CSV
        let escaped_data = escape_csv_field(&encoded_data);

        let line = format!(
            "{},{},{},{},{},{},{}\n",
            timestamp_ns,
            thread_id,
            group_id,
            conn_id,
            escaped_request_id,
            data.len(),
            escaped_data
        );

        self.writer.write_all(line.as_bytes())?;
        self.bytes_written += line.len() as u64;
        Ok(line.len())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// Thread-safe request dumper with rotation support
pub struct RequestDumper {
    config: DumpConfig,
    /// Per-thread writers (thread_id -> writer)
    writers: Mutex<std::collections::HashMap<usize, DumpWriter>>,
    /// Total records written across all threads
    total_records: AtomicU64,
}

impl RequestDumper {
    /// Create a new request dumper with the given configuration
    pub fn new(config: DumpConfig) -> Result<Arc<Self>> {
        // Ensure directory exists
        fs::create_dir_all(&config.directory)
            .with_context(|| format!("Failed to create dump directory: {:?}", config.directory))?;

        Ok(Arc::new(Self {
            config,
            writers: Mutex::new(std::collections::HashMap::new()),
            total_records: AtomicU64::new(0),
        }))
    }

    /// Record a request
    ///
    /// # Arguments
    /// * `thread_id` - The worker thread ID
    /// * `group_id` - The traffic group ID
    /// * `conn_id` - The connection ID
    /// * `data` - The raw request data
    /// * `request_id` - String representation of the request ID
    pub fn record(
        &self,
        thread_id: usize,
        group_id: usize,
        conn_id: usize,
        data: &[u8],
        request_id: &str,
    ) -> Result<()> {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos() as u64;

        let mut writers = self.writers.lock().unwrap();

        // Check if we need rotation
        let needs_rotation = self.check_rotation(&writers, thread_id);

        if needs_rotation {
            // Remove old writer to trigger rotation
            if let Some(mut old_writer) = writers.remove(&thread_id) {
                let _ = old_writer.flush();
            }
            // Clean up old files if max_files is set
            self.cleanup_old_files()?;
        }

        // Get or create writer for this thread
        let writer = if let Some(w) = writers.get_mut(&thread_id) {
            w
        } else {
            let path = self.generate_file_path(thread_id);
            let mut new_writer = DumpWriter::new(path)?;
            new_writer.rotation_key = self.current_rotation_key();
            new_writer.write_header(&self.config.encoding)?;
            writers.insert(thread_id, new_writer);
            writers.get_mut(&thread_id).unwrap()
        };

        writer.write_record(
            timestamp_ns,
            thread_id,
            group_id,
            conn_id,
            request_id,
            data,
            &self.config.encoding,
        )?;

        self.total_records.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Flush all writers
    pub fn flush(&self) -> Result<()> {
        let mut writers = self.writers.lock().unwrap();
        for writer in writers.values_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    /// Get total records written
    pub fn total_records(&self) -> u64 {
        self.total_records.load(Ordering::Relaxed)
    }

    /// Check if rotation is needed for a thread's writer
    fn check_rotation(
        &self,
        writers: &std::collections::HashMap<usize, DumpWriter>,
        thread_id: usize,
    ) -> bool {
        let writer = match writers.get(&thread_id) {
            Some(w) => w,
            None => return false,
        };

        match self.config.rotation {
            RotationPolicy::Never => false,
            RotationPolicy::Size(max_size) => writer.bytes_written() >= max_size,
            RotationPolicy::Minutely | RotationPolicy::Hourly | RotationPolicy::Daily => {
                let current_key = self.current_rotation_key();
                writer.rotation_key != current_key
            }
        }
    }

    /// Generate the current rotation key based on policy
    fn current_rotation_key(&self) -> String {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
        let secs = now.as_secs();

        match self.config.rotation {
            RotationPolicy::Never => String::new(),
            RotationPolicy::Minutely => {
                // Key changes every minute
                format!("{}", secs / 60)
            }
            RotationPolicy::Hourly => {
                // Key changes every hour
                format!("{}", secs / 3600)
            }
            RotationPolicy::Daily => {
                // Key changes every day
                format!("{}", secs / 86400)
            }
            RotationPolicy::Size(_) => {
                // For size-based rotation, use a counter embedded in filename
                String::new()
            }
        }
    }

    /// Generate file path for a thread
    fn generate_file_path(&self, thread_id: usize) -> PathBuf {
        let timestamp = Self::format_timestamp();
        let filename = format!("{}_t{}_{}.csv", self.config.prefix, thread_id, timestamp);
        self.config.directory.join(filename)
    }

    /// Format current timestamp for filename
    ///
    /// Uses Unix timestamp format (seconds.nanoseconds) for simplicity and correctness.
    /// This avoids complex date calculations and ensures unique, sortable filenames.
    fn format_timestamp() -> String {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);

        // Format as UNIX_SECS_NANOS (e.g., 1733412345_123456789)
        // This is simpler, always valid, and sorts chronologically
        format!("{}_{:09}", now.as_secs(), now.subsec_nanos())
    }

    /// Clean up old rotated files
    fn cleanup_old_files(&self) -> Result<()> {
        let max_files = match self.config.max_files {
            Some(n) => n,
            None => return Ok(()),
        };

        let pattern = format!("{}_t", self.config.prefix);
        let mut files: Vec<_> = fs::read_dir(&self.config.directory)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(&pattern))
            .collect();

        if files.len() <= max_files {
            return Ok(());
        }

        // Sort by modification time (oldest first)
        files.sort_by(|a, b| {
            let a_time = a.metadata().and_then(|m| m.modified()).ok();
            let b_time = b.metadata().and_then(|m| m.modified()).ok();
            a_time.cmp(&b_time)
        });

        // Remove oldest files
        let to_remove = files.len() - max_files;
        for entry in files.into_iter().take(to_remove) {
            let _ = fs::remove_file(entry.path());
        }

        Ok(())
    }
}

impl Drop for RequestDumper {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Builder for DumpConfig
pub struct DumpConfigBuilder {
    config: DumpConfig,
}

impl DumpConfigBuilder {
    pub fn new() -> Self {
        Self { config: DumpConfig::default() }
    }

    pub fn directory<P: AsRef<Path>>(mut self, dir: P) -> Self {
        self.config.directory = dir.as_ref().to_path_buf();
        self
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.config.prefix = prefix.into();
        self
    }

    pub fn rotation(mut self, policy: RotationPolicy) -> Self {
        self.config.rotation = policy;
        self
    }

    pub fn max_files(mut self, max: usize) -> Self {
        self.config.max_files = Some(max);
        self
    }

    pub fn encoding(mut self, encoding: DataEncoding) -> Self {
        self.config.encoding = encoding;
        self
    }

    pub fn build(self) -> DumpConfig {
        self.config
    }
}

impl Default for DumpConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_basic_dump() {
        let temp_dir = TempDir::new().unwrap();
        let config = DumpConfigBuilder::new().directory(temp_dir.path()).prefix("test").build();

        let dumper = RequestDumper::new(config).unwrap();

        // Record some requests
        dumper.record(0, 1, 10, b"GET /test", "req_001").unwrap();
        dumper.record(0, 1, 10, b"GET /another", "req_002").unwrap();
        dumper.flush().unwrap();

        assert_eq!(dumper.total_records(), 2);

        // Verify file exists and has content
        let files: Vec<_> = fs::read_dir(temp_dir.path()).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(files.len(), 1);

        let content = fs::read_to_string(files[0].path()).unwrap();
        assert!(content.contains("timestamp_ns,thread_id,group_id"));
        assert!(content.contains("req_001"));
        assert!(content.contains("req_002"));
    }

    #[test]
    fn test_multi_thread_dump() {
        let temp_dir = TempDir::new().unwrap();
        let config = DumpConfigBuilder::new().directory(temp_dir.path()).prefix("mt_test").build();

        let dumper = RequestDumper::new(config).unwrap();

        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let dumper = Arc::clone(&dumper);
                thread::spawn(move || {
                    for i in 0..100 {
                        dumper
                            .record(
                                thread_id,
                                0,
                                thread_id * 10,
                                format!("request_{}", i).as_bytes(),
                                &format!("req_{}_{}", thread_id, i),
                            )
                            .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        dumper.flush().unwrap();
        assert_eq!(dumper.total_records(), 400);

        // Should have 4 files (one per thread)
        let files: Vec<_> = fs::read_dir(temp_dir.path()).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(files.len(), 4);
    }

    #[test]
    fn test_size_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let config = DumpConfigBuilder::new()
            .directory(temp_dir.path())
            .prefix("size_test")
            .rotation(RotationPolicy::Size(500)) // Rotate at 500 bytes
            .build();

        let dumper = RequestDumper::new(config).unwrap();

        // Write enough data to trigger rotation
        for i in 0..50 {
            dumper
                .record(0, 0, 0, b"some request data here", &format!("req_{:03}", i))
                .unwrap();
        }
        dumper.flush().unwrap();

        // Should have multiple files due to rotation
        let files: Vec<_> = fs::read_dir(temp_dir.path()).unwrap().filter_map(|e| e.ok()).collect();
        assert!(files.len() > 1, "Expected rotation to create multiple files");
    }

    #[test]
    fn test_encoding_none() {
        let temp_dir = TempDir::new().unwrap();
        let config = DumpConfigBuilder::new()
            .directory(temp_dir.path())
            .prefix("no_data")
            .encoding(DataEncoding::None)
            .build();

        let dumper = RequestDumper::new(config).unwrap();
        dumper.record(0, 0, 0, b"secret data", "req_001").unwrap();
        dumper.flush().unwrap();

        let files: Vec<_> = fs::read_dir(temp_dir.path()).unwrap().filter_map(|e| e.ok()).collect();
        let content = fs::read_to_string(files[0].path()).unwrap();

        // Data should not be in the output (encoding is None)
        assert!(!content.contains("secret"));
        // Header should have "data" column
        assert!(content.contains(",data\n"));
    }

    #[test]
    fn test_encoding_utf8() {
        let temp_dir = TempDir::new().unwrap();
        let config = DumpConfigBuilder::new()
            .directory(temp_dir.path())
            .prefix("utf8_test")
            .encoding(DataEncoding::Utf8Lossy)
            .build();

        let dumper = RequestDumper::new(config).unwrap();
        dumper.record(0, 0, 0, b"GET /test HTTP/1.1\r\n", "req_001").unwrap();
        dumper.flush().unwrap();

        let files: Vec<_> = fs::read_dir(temp_dir.path()).unwrap().filter_map(|e| e.ok()).collect();
        let content = fs::read_to_string(files[0].path()).unwrap();

        // Header should have "data_utf8" column
        assert!(content.contains(",data_utf8\n"));
        // Data should be readable (with CSV escaping due to newlines)
        assert!(content.contains("GET /test HTTP/1.1"));
    }

    #[test]
    fn test_encoding_base64() {
        let temp_dir = TempDir::new().unwrap();
        let config = DumpConfigBuilder::new()
            .directory(temp_dir.path())
            .prefix("base64_test")
            .encoding(DataEncoding::Base64)
            .build();

        let dumper = RequestDumper::new(config).unwrap();
        dumper.record(0, 0, 0, b"Hello", "req_001").unwrap();
        dumper.flush().unwrap();

        let files: Vec<_> = fs::read_dir(temp_dir.path()).unwrap().filter_map(|e| e.ok()).collect();
        let content = fs::read_to_string(files[0].path()).unwrap();

        // Header should have "data_base64" column
        assert!(content.contains(",data_base64\n"));
        // "Hello" in base64 is "SGVsbG8="
        assert!(content.contains("SGVsbG8="));
    }

    #[test]
    fn test_encoding_escaped() {
        let temp_dir = TempDir::new().unwrap();
        let config = DumpConfigBuilder::new()
            .directory(temp_dir.path())
            .prefix("escaped_test")
            .encoding(DataEncoding::Escaped)
            .build();

        let dumper = RequestDumper::new(config).unwrap();
        dumper.record(0, 0, 0, b"Hi\x00\x01", "req_001").unwrap();
        dumper.flush().unwrap();

        let files: Vec<_> = fs::read_dir(temp_dir.path()).unwrap().filter_map(|e| e.ok()).collect();
        let content = fs::read_to_string(files[0].path()).unwrap();

        // Header should have "data_escaped" column
        assert!(content.contains(",data_escaped\n"));
        // Should have escaped null bytes
        assert!(content.contains("Hi\\x00\\x01"));
    }

    #[test]
    fn test_base64_encoding() {
        // Test the base64 encoding function directly
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn test_escape_bytes() {
        assert_eq!(escape_bytes(b"hello"), "hello");
        assert_eq!(escape_bytes(b"hi\x00"), "hi\\x00");
        assert_eq!(escape_bytes(b"\r\n"), "\\x0d\\x0a");
        assert_eq!(escape_bytes(b"a b"), "a b"); // space is preserved
    }

    #[test]
    fn test_escape_csv_field() {
        // No escaping needed
        assert_eq!(escape_csv_field("simple"), "simple");
        assert_eq!(escape_csv_field("with space"), "with space");

        // Comma requires quoting
        assert_eq!(escape_csv_field("a,b"), "\"a,b\"");

        // Quote requires quoting and doubling
        assert_eq!(escape_csv_field("say \"hi\""), "\"say \"\"hi\"\"\"");

        // Newlines require quoting
        assert_eq!(escape_csv_field("line1\nline2"), "\"line1\nline2\"");
        assert_eq!(escape_csv_field("line1\r\nline2"), "\"line1\r\nline2\"");
    }

    #[test]
    fn test_request_id_with_special_chars() {
        let temp_dir = TempDir::new().unwrap();
        let config = DumpConfigBuilder::new()
            .directory(temp_dir.path())
            .prefix("special_test")
            .build();

        let dumper = RequestDumper::new(config).unwrap();

        // Request ID with comma
        dumper.record(0, 0, 0, b"data", "req,001").unwrap();
        // Request ID with quote
        dumper.record(0, 0, 0, b"data", "req\"002").unwrap();
        dumper.flush().unwrap();

        let files: Vec<_> = fs::read_dir(temp_dir.path()).unwrap().filter_map(|e| e.ok()).collect();
        let content = fs::read_to_string(files[0].path()).unwrap();

        // Verify CSV is properly escaped
        assert!(content.contains("\"req,001\"")); // Comma escaped with quotes
        assert!(content.contains("\"req\"\"002\"")); // Quote escaped with doubled quotes
    }
}
