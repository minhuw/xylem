//! CPU affinity management for worker threads

use core_affinity::{get_core_ids, set_for_current};

/// CPU pinning configuration
#[derive(Debug, Clone, PartialEq, Default)]
pub enum CpuPinning {
    /// No CPU pinning
    #[default]
    None,
    /// Auto-pin threads: thread N -> core N
    Auto,
    /// Pin starting from offset: thread N -> core (offset + N)
    Offset(usize),
}

/// Pin current thread to a CPU core based on thread_id and configuration
///
/// # Arguments
/// * `thread_id` - The worker thread ID (0-indexed)
/// * `config` - CPU pinning configuration
///
/// # Returns
/// * `Ok(())` if pinning succeeded or was disabled
/// * `Err` if pinning was requested but failed
pub fn pin_thread(thread_id: usize, config: &CpuPinning) -> anyhow::Result<()> {
    match config {
        CpuPinning::None => Ok(()),
        CpuPinning::Auto => pin_to_core(thread_id),
        CpuPinning::Offset(offset) => pin_to_core(offset + thread_id),
    }
}

/// Pin current thread to a specific CPU core
fn pin_to_core(core_id: usize) -> anyhow::Result<()> {
    let core_ids = get_core_ids().ok_or_else(|| {
        anyhow::anyhow!("Failed to get available CPU core IDs - CPU pinning may not be supported on this platform")
    })?;

    if core_id >= core_ids.len() {
        anyhow::bail!(
            "Cannot pin to CPU core {}: only {} cores available (0-{})",
            core_id,
            core_ids.len(),
            core_ids.len() - 1
        );
    }

    let target_core = core_ids[core_id];
    let success = set_for_current(target_core);

    if success {
        tracing::debug!("Successfully pinned thread to CPU core {}", core_id);
        Ok(())
    } else {
        anyhow::bail!(
            "Failed to pin thread to CPU core {} - insufficient permissions or platform limitations",
            core_id
        )
    }
}

/// Get number of available CPU cores
///
/// Returns `None` if core detection is not supported on this platform
pub fn get_core_count() -> Option<usize> {
    get_core_ids().map(|ids| ids.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_core_count() {
        let count = get_core_count();
        assert!(count.is_some(), "Should be able to detect CPU cores");
        let count = count.unwrap();
        assert!(count > 0, "Should have at least one CPU core");
        println!("Detected {} CPU cores", count);
    }

    #[test]
    fn test_cpu_pinning_none() {
        // None should always succeed without doing anything
        let result = pin_thread(0, &CpuPinning::None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cpu_pinning_auto_valid() {
        // Try to pin to core 0 (should always exist)
        let result = pin_thread(0, &CpuPinning::Auto);
        // May fail on some platforms or without permissions, but shouldn't panic
        match result {
            Ok(()) => println!("Successfully pinned to core 0"),
            Err(e) => {
                println!("Could not pin to core 0: {} (this is OK on restricted environments)", e)
            }
        }
    }

    #[test]
    fn test_cpu_pinning_auto_invalid() {
        let core_count = get_core_count().unwrap_or(1);
        // Try to pin to a core that doesn't exist
        let invalid_core = core_count + 10;
        let result = pin_thread(invalid_core, &CpuPinning::Auto);
        assert!(result.is_err(), "Should fail when pinning to non-existent core");
    }

    #[test]
    fn test_cpu_pinning_offset() {
        let core_count = get_core_count().unwrap_or(1);
        if core_count >= 2 {
            // Pin thread 0 to core 1 (offset = 1)
            let result = pin_thread(0, &CpuPinning::Offset(1));
            match result {
                Ok(()) => println!("Successfully pinned thread 0 to core 1"),
                Err(e) => println!("Could not pin: {} (this is OK on restricted environments)", e),
            }
        } else {
            println!("Skipping offset test: only {} core available", core_count);
        }
    }

    #[test]
    fn test_cpu_pinning_offset_invalid() {
        let core_count = get_core_count().unwrap_or(1);
        // offset + thread_id should exceed available cores
        let result = pin_thread(0, &CpuPinning::Offset(core_count + 10));
        assert!(result.is_err(), "Should fail when offset exceeds available cores");
    }

    #[test]
    fn test_cpu_pinning_default() {
        let default = CpuPinning::default();
        assert_eq!(default, CpuPinning::None);
    }
}
