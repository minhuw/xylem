/// Integration test for data import feature
use anyhow::Result;
use std::fs::File;
use std::io::Write;
use tempfile::TempDir;

#[test]
#[ignore] // Requires Redis server running
fn test_data_import_end_to_end() -> Result<()> {
    // Create temporary directory for test files
    let temp_dir = TempDir::new()?;
    let csv_path = temp_dir.path().join("test_data.csv");

    // Create test CSV file
    let mut csv_file = File::create(&csv_path)?;
    writeln!(csv_file, "key,value,expiry")?;
    writeln!(csv_file, "test:1,value1,0")?;
    writeln!(csv_file, "test:2,value2,0")?;
    writeln!(csv_file, "test:3,value3,0")?;
    csv_file.flush()?;

    // Load CSV using DataImporter
    let importer = xylem_core::workload::DataImporter::from_csv(&csv_path)?;

    // Verify data was loaded correctly
    assert_eq!(importer.len(), 3);

    // Verify we can access the data
    let entry1 = importer.get_by_key("test:1").expect("Key test:1 should exist");
    assert_eq!(entry1.key, "test:1");
    assert_eq!(entry1.value_bytes(), b"value1");
    assert_eq!(entry1.expiry, 0);

    let entry2 = importer.get_by_key("test:2").expect("Key test:2 should exist");
    assert_eq!(entry2.value_bytes(), b"value2");

    Ok(())
}

#[test]
fn test_data_import_csv_parsing() -> Result<()> {
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Create temporary CSV file
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "key,value,expiry")?;
    writeln!(temp_file, "user:100,alice,0")?;
    writeln!(temp_file, "user:101,bob,3600")?;
    writeln!(temp_file, "session:abc,tokendata,1800")?;
    temp_file.flush()?;

    // Load CSV
    let importer = xylem_core::workload::DataImporter::from_csv(temp_file.path())?;

    // Verify all entries
    assert_eq!(importer.len(), 3);

    let alice = importer.get_by_key("user:100").unwrap();
    assert_eq!(alice.value_bytes(), b"alice");
    assert_eq!(alice.expiry, 0);

    let bob = importer.get_by_key("user:101").unwrap();
    assert_eq!(bob.value_bytes(), b"bob");
    assert_eq!(bob.expiry, 3600);

    let session = importer.get_by_key("session:abc").unwrap();
    assert_eq!(session.value_bytes(), b"tokendata");
    assert_eq!(session.expiry, 1800);

    Ok(())
}

#[test]
fn test_data_import_random_access() -> Result<()> {
    use std::collections::HashSet;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Create CSV with multiple entries
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "key,value,expiry")?;
    for i in 0..10 {
        writeln!(temp_file, "key:{},value{},0", i, i)?;
    }
    temp_file.flush()?;

    // Load with seed for reproducibility
    let mut importer =
        xylem_core::workload::DataImporter::from_csv_with_seed(temp_file.path(), Some(42))?;

    // Access entries randomly
    let mut seen_keys = HashSet::new();
    for _ in 0..20 {
        let entry = importer.next_random();
        seen_keys.insert(entry.key.clone());
    }

    // Should have seen multiple different keys (not just one)
    assert!(seen_keys.len() > 1, "Random access should return different keys");

    Ok(())
}

#[test]
fn test_data_import_sequential_access() -> Result<()> {
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Create CSV
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "key,value,expiry")?;
    writeln!(temp_file, "key:0,val0,0")?;
    writeln!(temp_file, "key:1,val1,0")?;
    writeln!(temp_file, "key:2,val2,0")?;
    temp_file.flush()?;

    let mut importer = xylem_core::workload::DataImporter::from_csv(temp_file.path())?;

    // Sequential access should return entries in order
    let entry0 = importer.next_sequential();
    assert_eq!(entry0.key, "key:0");

    let entry1 = importer.next_sequential();
    assert_eq!(entry1.key, "key:1");

    let entry2 = importer.next_sequential();
    assert_eq!(entry2.key, "key:2");

    // Should wrap around
    let entry3 = importer.next_sequential();
    assert_eq!(entry3.key, "key:0");

    Ok(())
}

#[test]
fn test_data_verifier() -> Result<()> {
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Create CSV
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "key,value,expiry")?;
    writeln!(temp_file, "key1,value1,0")?;
    writeln!(temp_file, "key2,value2,0")?;
    temp_file.flush()?;

    let importer = xylem_core::workload::DataImporter::from_csv(temp_file.path())?;
    let mut verifier = xylem_core::workload::DataVerifier::new(&importer);

    // Verify correct values
    assert!(verifier.verify("key1", b"value1"));
    assert!(verifier.verify("key2", b"value2"));

    // Verify incorrect values
    assert!(!verifier.verify("key1", b"wrong_value"));

    // Non-existent keys are treated as success (for write testing scenarios)
    assert!(verifier.verify("key3", b"value3"));

    // Check statistics
    let stats = verifier.stats();
    assert_eq!(stats.successful, 3); // key1 correct, key2 correct, key3 missing (counted as success)
    assert_eq!(stats.failed, 1); // key1 wrong_value
    assert!(stats.mismatch_rate > 0.0);

    Ok(())
}

#[test]
fn test_redis_protocol_generate_set_with_imported_data() {
    use xylem_protocols::{FixedCommandSelector, RedisOp, RedisProtocol};

    let selector = Box::new(FixedCommandSelector::new(RedisOp::Set));
    let mut protocol = RedisProtocol::new(selector);

    // Generate SET command with string key
    let (request, id) = protocol.generate_set_with_imported_data(0, "user:1000", b"alice");

    assert_eq!(id, (0, 0));

    // Verify RESP format
    let request_str = String::from_utf8_lossy(&request);
    assert!(request_str.contains("*3")); // Array with 3 elements
    assert!(request_str.contains("SET"));
    assert!(request_str.contains("user:1000"));
    assert!(request_str.contains("alice"));
}

#[test]
fn test_request_generator_with_imported_data() -> Result<()> {
    use std::io::Write;
    use tempfile::NamedTempFile;
    use xylem_core::workload::{DataImporter, FixedSize, RateControl, RequestGenerator};

    // Create CSV
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "key,value,expiry")?;
    writeln!(temp_file, "key1,value1,0")?;
    writeln!(temp_file, "key2,value2,0")?;
    temp_file.flush()?;

    // Load data
    let importer = DataImporter::from_csv(temp_file.path())?;

    // Create generator with imported data
    let value_size_gen = Box::new(FixedSize::new(64));
    let mut generator =
        RequestGenerator::with_imported_data(importer, RateControl::ClosedLoop, value_size_gen);

    // Verify generator reports using imported data
    assert!(generator.is_using_imported_data());

    // Generate requests
    let (key1, value1) = generator.next_request_from_import();
    assert!(!key1.is_empty());
    assert!(!value1.is_empty());

    let (key2, value2) = generator.next_request_from_import();
    assert!(!key2.is_empty());
    assert!(!value2.is_empty());

    Ok(())
}
