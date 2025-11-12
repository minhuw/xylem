//! Data import and verification for workload generation
//!
//! This module provides functionality to import test data from CSV files
//! and verify that stored data matches expected values.

use anyhow::{Context, Result};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Represents a single imported data entry
#[derive(Debug, Clone, Deserialize)]
pub struct ImportedEntry {
    /// Key name (e.g., "user:1000")
    pub key: String,
    /// Value data (stored as string in CSV, converted to bytes)
    pub value: String,
    /// Optional expiry time in seconds (0 or empty = no expiry)
    #[serde(default)]
    pub expiry: u64,
}

impl ImportedEntry {
    /// Get value as bytes
    pub fn value_bytes(&self) -> Vec<u8> {
        self.value.as_bytes().to_vec()
    }

    /// Check if entry has expiry
    pub fn has_expiry(&self) -> bool {
        self.expiry > 0
    }
}

/// Data importer that loads entries from CSV files
#[derive(Debug)]
pub struct DataImporter {
    /// All imported entries
    entries: Vec<ImportedEntry>,
    /// Index mapping keys to entry positions (for fast lookup)
    key_index: HashMap<String, usize>,
    /// Random number generator for selecting entries
    rng: SmallRng,
    /// Current index for sequential access
    current_idx: usize,
}

impl DataImporter {
    /// Create a new DataImporter from a CSV file
    ///
    /// # CSV Format
    /// ```csv
    /// key,value,expiry
    /// user:1000,alice,3600
    /// user:1001,bob,0
    /// session:abc,{"token":"xyz"},1800
    /// ```
    ///
    /// # Parameters
    /// - `path`: Path to CSV file
    ///
    /// # Returns
    /// DataImporter instance with loaded entries
    pub fn from_csv<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let mut reader = csv::Reader::from_path(path)
            .with_context(|| format!("Failed to open CSV file: {}", path.display()))?;

        let mut entries = Vec::new();
        let mut key_index = HashMap::new();

        for (line_num, result) in reader.deserialize().enumerate() {
            let entry: ImportedEntry = result.with_context(|| {
                format!("Failed to parse CSV line {} in file: {}", line_num + 2, path.display())
            })?;

            // Check for duplicate keys
            if key_index.contains_key(&entry.key) {
                anyhow::bail!(
                    "Duplicate key '{}' found at line {} in file: {}",
                    entry.key,
                    line_num + 2,
                    path.display()
                );
            }

            key_index.insert(entry.key.clone(), entries.len());
            entries.push(entry);
        }

        if entries.is_empty() {
            anyhow::bail!("CSV file is empty or contains no valid entries: {}", path.display());
        }

        tracing::info!("Loaded {} entries from CSV file: {}", entries.len(), path.display());

        Ok(Self {
            entries,
            key_index,
            rng: SmallRng::from_os_rng(),
            current_idx: 0,
        })
    }

    /// Create a DataImporter with explicit seed for reproducibility
    pub fn from_csv_with_seed<P: AsRef<Path>>(path: P, seed: Option<u64>) -> Result<Self> {
        let mut importer = Self::from_csv(path)?;
        if let Some(s) = seed {
            importer.rng = SmallRng::seed_from_u64(s);
        }
        Ok(importer)
    }

    /// Get a random entry
    pub fn next_random(&mut self) -> &ImportedEntry {
        let idx = self.rng.random_range(0..self.entries.len());
        &self.entries[idx]
    }

    /// Get the next entry sequentially (wraps around)
    pub fn next_sequential(&mut self) -> &ImportedEntry {
        let entry = &self.entries[self.current_idx];
        self.current_idx = (self.current_idx + 1) % self.entries.len();
        entry
    }

    /// Get an entry by key
    pub fn get_by_key(&self, key: &str) -> Option<&ImportedEntry> {
        self.key_index.get(key).map(|&idx| &self.entries[idx])
    }

    /// Get total number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if importer is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all entries (for verification setup)
    pub fn entries(&self) -> &[ImportedEntry] {
        &self.entries
    }

    /// Reset sequential index to start
    pub fn reset(&mut self) {
        self.current_idx = 0;
    }
}

/// Data verifier that checks if stored data matches expected values
pub struct DataVerifier {
    /// Expected values keyed by key name
    expected: HashMap<String, Vec<u8>>,
    /// Mismatches found during verification
    mismatches: Vec<VerificationMismatch>,
    /// Total verification attempts
    verification_count: u64,
    /// Successful verifications
    success_count: u64,
}

/// Represents a verification mismatch
#[derive(Debug, Clone)]
pub struct VerificationMismatch {
    /// Key that failed verification
    pub key: String,
    /// Expected value
    pub expected: Vec<u8>,
    /// Actual value received
    pub actual: Vec<u8>,
}

impl DataVerifier {
    /// Create a new DataVerifier from a DataImporter
    pub fn new(importer: &DataImporter) -> Self {
        let mut expected = HashMap::new();
        for entry in importer.entries() {
            expected.insert(entry.key.clone(), entry.value_bytes());
        }

        tracing::info!("DataVerifier initialized with {} expected entries", expected.len());

        Self {
            expected,
            mismatches: Vec::new(),
            verification_count: 0,
            success_count: 0,
        }
    }

    /// Verify a key-value pair
    ///
    /// # Returns
    /// - `true` if verification passed
    /// - `false` if verification failed (mismatch recorded)
    pub fn verify(&mut self, key: &str, actual: &[u8]) -> bool {
        self.verification_count += 1;

        match self.expected.get(key) {
            Some(expected) if expected == actual => {
                self.success_count += 1;
                true
            }
            Some(expected) => {
                tracing::warn!(
                    "Verification mismatch for key '{}': expected {} bytes, got {} bytes",
                    key,
                    expected.len(),
                    actual.len()
                );
                self.mismatches.push(VerificationMismatch {
                    key: key.to_string(),
                    expected: expected.clone(),
                    actual: actual.to_vec(),
                });
                false
            }
            None => {
                // Key not in expected set - could be OK if we're testing writes
                // Don't count as failure, just skip
                tracing::debug!("Key '{}' not in expected set, skipping verification", key);
                self.success_count += 1;
                true
            }
        }
    }

    /// Get verification statistics
    pub fn stats(&self) -> VerificationStats {
        VerificationStats {
            total_verifications: self.verification_count,
            successful: self.success_count,
            failed: self.mismatches.len() as u64,
            mismatch_rate: if self.verification_count > 0 {
                self.mismatches.len() as f64 / self.verification_count as f64
            } else {
                0.0
            },
        }
    }

    /// Get all mismatches
    pub fn mismatches(&self) -> &[VerificationMismatch] {
        &self.mismatches
    }

    /// Check if any mismatches occurred
    pub fn has_mismatches(&self) -> bool {
        !self.mismatches.is_empty()
    }

    /// Reset verification state
    pub fn reset(&mut self) {
        self.mismatches.clear();
        self.verification_count = 0;
        self.success_count = 0;
    }
}

/// Verification statistics
#[derive(Debug, Clone)]
pub struct VerificationStats {
    pub total_verifications: u64,
    pub successful: u64,
    pub failed: u64,
    pub mismatch_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn create_test_csv(path: &Path, entries: &[(&str, &str, u64)]) -> Result<()> {
        let mut file = std::fs::File::create(path)?;
        writeln!(file, "key,value,expiry")?;
        for (key, value, expiry) in entries {
            writeln!(file, "{},{},{}", key, value, expiry)?;
        }
        Ok(())
    }

    #[test]
    fn test_data_importer_basic() {
        let temp_dir = std::env::temp_dir();
        let csv_path = temp_dir.join("test_import_basic.csv");

        let test_data =
            vec![("user:1", "alice", 0), ("user:2", "bob", 3600), ("user:3", "charlie", 1800)];

        create_test_csv(&csv_path, &test_data).unwrap();

        let importer = DataImporter::from_csv(&csv_path).unwrap();
        assert_eq!(importer.len(), 3);
        assert!(!importer.is_empty());

        // Test lookup
        let entry = importer.get_by_key("user:2").unwrap();
        assert_eq!(entry.key, "user:2");
        assert_eq!(entry.value, "bob");
        assert_eq!(entry.expiry, 3600);

        std::fs::remove_file(csv_path).ok();
    }

    #[test]
    fn test_data_importer_random_access() {
        let temp_dir = std::env::temp_dir();
        let csv_path = temp_dir.join("test_import_random.csv");

        let test_data =
            vec![("key:1", "value1", 0), ("key:2", "value2", 0), ("key:3", "value3", 0)];

        create_test_csv(&csv_path, &test_data).unwrap();

        let mut importer = DataImporter::from_csv_with_seed(&csv_path, Some(42)).unwrap();

        // Get 10 random entries
        let mut keys_seen = std::collections::HashSet::new();
        for _ in 0..10 {
            let entry = importer.next_random();
            keys_seen.insert(entry.key.clone());
        }

        // Should see at least 2 different keys with 10 samples from 3 keys
        assert!(keys_seen.len() >= 2);

        std::fs::remove_file(csv_path).ok();
    }

    #[test]
    fn test_data_importer_sequential() {
        let temp_dir = std::env::temp_dir();
        let csv_path = temp_dir.join("test_import_sequential.csv");

        let test_data = vec![("key:1", "a", 0), ("key:2", "b", 0), ("key:3", "c", 0)];

        create_test_csv(&csv_path, &test_data).unwrap();

        let mut importer = DataImporter::from_csv(&csv_path).unwrap();

        assert_eq!(importer.next_sequential().key, "key:1");
        assert_eq!(importer.next_sequential().key, "key:2");
        assert_eq!(importer.next_sequential().key, "key:3");
        assert_eq!(importer.next_sequential().key, "key:1"); // Wraps around

        std::fs::remove_file(csv_path).ok();
    }

    #[test]
    fn test_data_verifier() {
        let temp_dir = std::env::temp_dir();
        let csv_path = temp_dir.join("test_verifier.csv");

        let test_data = vec![("key:1", "value1", 0), ("key:2", "value2", 0)];

        create_test_csv(&csv_path, &test_data).unwrap();

        let importer = DataImporter::from_csv(&csv_path).unwrap();
        let mut verifier = DataVerifier::new(&importer);

        // Correct verification
        assert!(verifier.verify("key:1", b"value1"));
        assert_eq!(verifier.stats().successful, 1);

        // Incorrect verification
        assert!(!verifier.verify("key:1", b"wrong_value"));
        assert_eq!(verifier.stats().failed, 1);
        assert!(verifier.has_mismatches());

        // Key not in set (should pass)
        assert!(verifier.verify("unknown:key", b"anything"));

        let stats = verifier.stats();
        assert_eq!(stats.total_verifications, 3);
        assert_eq!(stats.successful, 2);
        assert_eq!(stats.failed, 1);

        std::fs::remove_file(csv_path).ok();
    }

    #[test]
    fn test_duplicate_key_error() {
        let temp_dir = std::env::temp_dir();
        let csv_path = temp_dir.join("test_duplicate.csv");

        let test_data = vec![("key:1", "value1", 0), ("key:1", "value2", 0)];

        create_test_csv(&csv_path, &test_data).unwrap();

        let result = DataImporter::from_csv(&csv_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Duplicate key"));

        std::fs::remove_file(csv_path).ok();
    }

    #[test]
    fn test_empty_csv_error() {
        let temp_dir = std::env::temp_dir();
        let csv_path = temp_dir.join("test_empty.csv");

        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "key,value,expiry").unwrap();
        drop(file);

        let result = DataImporter::from_csv(&csv_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));

        std::fs::remove_file(csv_path).ok();
    }
}
