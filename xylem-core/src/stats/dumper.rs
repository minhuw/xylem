//! Statistics streaming to Parquet files
//!
//! Enables bounded-memory statistics collection for long-running experiments
//! by streaming completed time buckets to disk.

use super::tuple_collector::{StatsEntry, StatsKey};
use arrow::array::{ArrayRef, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// Row format for Parquet stats dump
#[derive(Debug, Clone)]
pub struct StatsRow {
    /// Time bucket (seconds since experiment start)
    pub time_bucket: u64,
    /// Traffic group ID
    pub group_id: u32,
    /// Connection ID
    pub connection_id: u32,
    /// Total request count in this bucket
    pub request_count: u64,
    /// Total transmitted bytes
    pub tx_bytes: u64,
    /// Total received bytes
    pub rx_bytes: u64,
    /// p50 latency in nanoseconds
    pub latency_p50_ns: u64,
    /// p99 latency in nanoseconds
    pub latency_p99_ns: u64,
    /// p999 latency in nanoseconds
    pub latency_p999_ns: u64,
    /// Mean latency in nanoseconds
    pub latency_mean_ns: u64,
    /// Min latency in nanoseconds
    pub latency_min_ns: u64,
    /// Max latency in nanoseconds
    pub latency_max_ns: u64,
    /// Number of latency samples in this bucket
    pub latency_sample_count: u64,
}

impl StatsRow {
    /// Create a stats row from a key and entry
    pub fn from_entry(key: &StatsKey, entry: &StatsEntry) -> Self {
        Self {
            time_bucket: key.time_bucket,
            group_id: key.group_id as u32,
            connection_id: key.connection_id as u32,
            request_count: entry.request_count,
            tx_bytes: entry.tx_bytes,
            rx_bytes: entry.rx_bytes,
            latency_p50_ns: entry.latency.percentile(0.5),
            latency_p99_ns: entry.latency.percentile(0.99),
            latency_p999_ns: entry.latency.percentile(0.999),
            latency_mean_ns: entry.latency.mean(),
            latency_min_ns: entry.latency.min(),
            latency_max_ns: entry.latency.max(),
            latency_sample_count: entry.latency.count(),
        }
    }
}

/// Create the Arrow schema for stats rows
fn create_stats_schema() -> Schema {
    Schema::new(vec![
        Field::new("time_bucket", DataType::UInt64, false),
        Field::new("group_id", DataType::UInt32, false),
        Field::new("connection_id", DataType::UInt32, false),
        Field::new("request_count", DataType::UInt64, false),
        Field::new("tx_bytes", DataType::UInt64, false),
        Field::new("rx_bytes", DataType::UInt64, false),
        Field::new("latency_p50_ns", DataType::UInt64, false),
        Field::new("latency_p99_ns", DataType::UInt64, false),
        Field::new("latency_p999_ns", DataType::UInt64, false),
        Field::new("latency_mean_ns", DataType::UInt64, false),
        Field::new("latency_min_ns", DataType::UInt64, false),
        Field::new("latency_max_ns", DataType::UInt64, false),
        Field::new("latency_sample_count", DataType::UInt64, false),
    ])
}

/// Streams statistics to a Parquet file
pub struct StatsDumper {
    writer: ArrowWriter<File>,
    /// Buffer of rows to write
    buffer: Vec<StatsRow>,
    /// Batch size for writing
    batch_size: usize,
    /// Path for debugging
    path: PathBuf,
}

impl StatsDumper {
    /// Create a new stats dumper
    ///
    /// # Arguments
    /// * `path` - Output file path
    /// * `batch_size` - Number of rows to buffer before writing (default: 1000)
    pub fn new(path: PathBuf, batch_size: Option<usize>) -> Result<Self, StatsDumperError> {
        let file = File::create(&path).map_err(StatsDumperError::Io)?;

        let schema = Arc::new(create_stats_schema());
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| StatsDumperError::Parquet(e.to_string()))?;

        Ok(Self {
            writer,
            buffer: Vec::with_capacity(batch_size.unwrap_or(1000)),
            batch_size: batch_size.unwrap_or(1000),
            path,
        })
    }

    /// Create a dumper with a thread-specific filename
    pub fn for_thread(base_path: &str, thread_id: usize) -> Result<Self, StatsDumperError> {
        let path = PathBuf::from(format!("{}_thread_{}.parquet", base_path, thread_id));
        Self::new(path, None)
    }

    /// Write a stats row
    pub fn write_row(
        &mut self,
        key: &StatsKey,
        entry: &StatsEntry,
    ) -> Result<(), StatsDumperError> {
        let row = StatsRow::from_entry(key, entry);
        self.buffer.push(row);

        if self.buffer.len() >= self.batch_size {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Write multiple stats rows
    pub fn write_rows(
        &mut self,
        entries: impl Iterator<Item = (StatsKey, StatsEntry)>,
    ) -> Result<(), StatsDumperError> {
        for (key, entry) in entries {
            self.write_row(&key, &entry)?;
        }
        Ok(())
    }

    /// Flush the internal buffer to disk
    fn flush_buffer(&mut self) -> Result<(), StatsDumperError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch = self.create_record_batch()?;
        self.writer
            .write(&batch)
            .map_err(|e| StatsDumperError::Parquet(e.to_string()))?;
        self.buffer.clear();

        Ok(())
    }

    /// Create a RecordBatch from buffered rows
    fn create_record_batch(&self) -> Result<RecordBatch, StatsDumperError> {
        let time_bucket: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.time_bucket)));
        let group_id: ArrayRef =
            Arc::new(UInt32Array::from_iter_values(self.buffer.iter().map(|r| r.group_id)));
        let connection_id: ArrayRef =
            Arc::new(UInt32Array::from_iter_values(self.buffer.iter().map(|r| r.connection_id)));
        let request_count: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.request_count)));
        let tx_bytes: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.tx_bytes)));
        let rx_bytes: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.rx_bytes)));
        let latency_p50_ns: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.latency_p50_ns)));
        let latency_p99_ns: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.latency_p99_ns)));
        let latency_p999_ns: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.latency_p999_ns)));
        let latency_mean_ns: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.latency_mean_ns)));
        let latency_min_ns: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.latency_min_ns)));
        let latency_max_ns: ArrayRef =
            Arc::new(UInt64Array::from_iter_values(self.buffer.iter().map(|r| r.latency_max_ns)));
        let latency_sample_count: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            self.buffer.iter().map(|r| r.latency_sample_count),
        ));

        let schema = Arc::new(create_stats_schema());
        RecordBatch::try_new(
            schema,
            vec![
                time_bucket,
                group_id,
                connection_id,
                request_count,
                tx_bytes,
                rx_bytes,
                latency_p50_ns,
                latency_p99_ns,
                latency_p999_ns,
                latency_mean_ns,
                latency_min_ns,
                latency_max_ns,
                latency_sample_count,
            ],
        )
        .map_err(|e| StatsDumperError::Arrow(e.to_string()))
    }

    /// Finalize and close the writer
    pub fn close(mut self) -> Result<(), StatsDumperError> {
        self.flush_buffer()?;
        self.writer.close().map_err(|e| StatsDumperError::Parquet(e.to_string()))?;
        tracing::info!("Stats dumped to {:?}", self.path);
        Ok(())
    }

    /// Get the output path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

/// Errors that can occur during stats dumping
#[derive(Debug)]
pub enum StatsDumperError {
    /// I/O error
    Io(std::io::Error),
    /// Parquet error
    Parquet(String),
    /// Arrow error
    Arrow(String),
}

impl std::fmt::Display for StatsDumperError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatsDumperError::Io(e) => write!(f, "I/O error: {}", e),
            StatsDumperError::Parquet(e) => write!(f, "Parquet error: {}", e),
            StatsDumperError::Arrow(e) => write!(f, "Arrow error: {}", e),
        }
    }
}

impl std::error::Error for StatsDumperError {}

/// Configuration for stats streaming
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Enable streaming to disk
    pub enabled: bool,
    /// Output path prefix (thread ID will be appended)
    pub path_prefix: String,
    /// Number of time buckets to keep in memory
    pub retention_buckets: usize,
    /// Batch size for Parquet writes
    pub batch_size: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path_prefix: "./stats".to_string(),
            retention_buckets: 2,
            batch_size: 1000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::collector::SamplingPolicy;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_stats_row_creation() {
        let key = StatsKey {
            time_bucket: 0,
            group_id: 1,
            connection_id: 2,
        };

        let mut entry = StatsEntry::new(&SamplingPolicy::default());
        entry.record_latency(Duration::from_micros(100));
        entry.record_latency(Duration::from_micros(200));
        entry.record_tx_bytes(1000);
        entry.record_rx_bytes(500);

        let row = StatsRow::from_entry(&key, &entry);

        assert_eq!(row.time_bucket, 0);
        assert_eq!(row.group_id, 1);
        assert_eq!(row.connection_id, 2);
        assert_eq!(row.request_count, 2);
        assert_eq!(row.tx_bytes, 1000);
        assert_eq!(row.rx_bytes, 500);
        assert!(row.latency_p50_ns > 0);
    }

    #[test]
    fn test_stats_dumper_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_stats.parquet");

        let mut dumper = StatsDumper::new(path.clone(), Some(10)).unwrap();

        // Write some rows
        for i in 0..5 {
            let key = StatsKey {
                time_bucket: i,
                group_id: 0,
                connection_id: 0,
            };
            let mut entry = StatsEntry::new(&SamplingPolicy::default());
            entry.record_latency(Duration::from_micros(100 + i * 10));
            entry.record_tx_bytes(100);

            dumper.write_row(&key, &entry).unwrap();
        }

        dumper.close().unwrap();

        // Verify file exists and has content
        assert!(path.exists());
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.len() > 0);
    }

    #[test]
    fn test_stats_dumper_batch_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_batch_stats.parquet");

        let mut dumper = StatsDumper::new(path.clone(), Some(5)).unwrap();

        // Write enough rows to trigger multiple flushes
        for i in 0..12 {
            let key = StatsKey {
                time_bucket: i,
                group_id: 0,
                connection_id: 0,
            };
            let mut entry = StatsEntry::new(&SamplingPolicy::default());
            entry.record_latency(Duration::from_micros(100));

            dumper.write_row(&key, &entry).unwrap();
        }

        dumper.close().unwrap();

        assert!(path.exists());
    }
}
