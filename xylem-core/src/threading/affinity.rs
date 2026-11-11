//! CPU affinity management for worker threads

use core_affinity::{get_core_ids, set_for_current, CoreId};

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
    match target_core_id(thread_id, config)? {
        Some(core_id) => pin_to_core(core_id),
        None => Ok(()),
    }
}

fn target_core_id(thread_id: usize, config: &CpuPinning) -> anyhow::Result<Option<usize>> {
    match config {
        CpuPinning::None => Ok(None),
        CpuPinning::Auto => Ok(Some(thread_id)),
        CpuPinning::Offset(offset) => offset.checked_add(thread_id).map(Some).ok_or_else(|| {
            anyhow::anyhow!("CPU core ID overflow: offset {} + thread ID {}", offset, thread_id)
        }),
    }
}

fn find_core(core_ids: &[CoreId], core_id: usize) -> anyhow::Result<CoreId> {
    core_ids.iter().copied().find(|core| core.id == core_id).ok_or_else(|| {
        anyhow::anyhow!(
            "Cannot pin to CPU core {}: available CPU IDs are {:?}",
            core_id,
            core_ids.iter().map(|core| core.id).collect::<Vec<_>>()
        )
    })
}

/// Pin current thread to a specific CPU core
fn pin_to_core(core_id: usize) -> anyhow::Result<()> {
    let core_ids = get_core_ids().ok_or_else(|| {
        anyhow::anyhow!("Failed to get available CPU core IDs - CPU pinning may not be supported on this platform")
    })?;
    let target_core = find_core(&core_ids, core_id)?;
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

/// Get the actual IDs of CPUs available to the current thread.
///
/// Returns `None` if core detection is not supported on this platform.
pub fn get_available_core_ids() -> Option<Vec<usize>> {
    get_core_ids().map(|ids| ids.into_iter().map(|core| core.id).collect())
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
    fn sparse_cpu_ids_are_matched_by_id() {
        let cores: Vec<_> = [9, 10, 11, 12].into_iter().map(|id| CoreId { id }).collect();
        for id in [9, 10, 11, 12] {
            assert_eq!(find_core(&cores, id).unwrap().id, id);
        }
        assert!(find_core(&cores, 3).is_err());
    }

    #[test]
    fn empty_cpu_set_rejects_requested_id() {
        let error = find_core(&[], 0).unwrap_err().to_string();
        assert!(error.contains("[]"), "{error}");
    }

    #[test]
    fn pinning_targets_and_offset_overflow() {
        assert_eq!(CpuPinning::default(), CpuPinning::None);
        assert!(pin_thread(5, &CpuPinning::None).is_ok());
        assert_eq!(target_core_id(5, &CpuPinning::None).unwrap(), None);
        assert_eq!(target_core_id(9, &CpuPinning::Auto).unwrap(), Some(9));
        assert_eq!(target_core_id(2, &CpuPinning::Offset(9)).unwrap(), Some(11));
        assert_eq!(target_core_id(0, &CpuPinning::Offset(usize::MAX)).unwrap(), Some(usize::MAX));
        assert!(target_core_id(1, &CpuPinning::Offset(usize::MAX)).is_err());
        assert!(pin_thread(1, &CpuPinning::Offset(usize::MAX)).is_err());
    }
}
