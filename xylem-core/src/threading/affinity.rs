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

/// The CPU that `config` pins thread `thread_id` to, if any
pub(crate) fn target_core_id(
    thread_id: usize,
    config: &CpuPinning,
) -> anyhow::Result<Option<usize>> {
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

/// A thread's NUMA memory policy, as the kernel reports it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPolicy {
    /// "none" for the kernel's default (memory from the node of the CPU
    /// that first touches it), else the mode: "bind", "preferred",
    /// "interleave", "local", "preferred_many" or "weighted_interleave"
    pub mode: &'static str,
    /// The policy's nodes, ascending; empty for "none" and "local"
    pub nodes: Vec<usize>,
}

impl MemoryPolicy {
    /// The kernel's default policy
    pub fn none() -> Self {
        Self { mode: "none", nodes: Vec::new() }
    }
}

/// Bind the calling thread's future memory allocations to NUMA node `node`
/// (`set_mempolicy(MPOL_BIND)`)
///
/// Allocates nothing until it fails. Pages the thread touched before keep
/// their place.
pub fn bind_memory_to_node(node: usize) -> anyhow::Result<()> {
    numa::bind_memory(node).map_err(|e| {
        anyhow::anyhow!(
            "Cannot bind memory to NUMA node {node}: set_mempolicy(MPOL_BIND) failed: {}{e}",
            numa::errno_name(&e).map(|name| format!("{name}: ")).unwrap_or_default()
        )
    })
}

/// Return the calling thread to the kernel's default memory policy
pub fn unbind_memory() -> std::io::Result<()> {
    numa::unbind_memory()
}

/// A thread's memory policy exactly as the kernel reported it, to set again
pub(crate) struct SavedMemoryPolicy(numa::Saved);

impl SavedMemoryPolicy {
    /// The calling thread's policy
    pub(crate) fn save() -> std::io::Result<Self> {
        numa::save().map(Self)
    }

    /// Give the calling thread this policy again
    pub(crate) fn restore(&self) -> std::io::Result<()> {
        numa::restore(&self.0)
    }
}

/// The NUMA node of CPU `cpu`
pub fn cpu_numa_node(cpu: usize) -> std::io::Result<usize> {
    numa::cpu_node(cpu)
}

/// The online NUMA nodes of this host, ascending; empty when the kernel has
/// no NUMA support
pub fn numa_nodes() -> std::io::Result<Vec<usize>> {
    numa::online_nodes()
}

/// The calling thread's memory policy (`get_mempolicy`)
pub fn memory_policy() -> std::io::Result<MemoryPolicy> {
    numa::memory_policy()
}

#[cfg(target_os = "linux")]
mod numa {
    use super::MemoryPolicy;
    use std::io;

    /// Bits in the node masks passed to the kernel, at least its MAX_NUMNODES
    const MASK_BITS: usize = 1024;
    const WORD_BITS: usize = libc::c_ulong::BITS as usize;
    type NodeMask = [libc::c_ulong; MASK_BITS / WORD_BITS];
    /// `maxnode`: the kernel reads one bit less than it is given
    const MAX_NODE: libc::c_ulong = MASK_BITS as libc::c_ulong + 1;
    /// Mode flags `get_mempolicy` reports ORed into the mode
    const MODE_FLAGS: libc::c_int =
        libc::MPOL_F_STATIC_NODES | libc::MPOL_F_RELATIVE_NODES | libc::MPOL_F_NUMA_BALANCING;

    pub(super) fn cpu_node(cpu: usize) -> io::Result<usize> {
        let dir = format!("/sys/devices/system/cpu/cpu{cpu}");
        for entry in std::fs::read_dir(&dir)? {
            let name = entry?.file_name();
            let node = name.to_str().and_then(|name| name.strip_prefix("node"));
            if let Some(Ok(node)) = node.map(str::parse) {
                return Ok(node);
            }
        }
        Err(io::Error::new(io::ErrorKind::NotFound, format!("{dir} has no node link")))
    }

    pub(super) fn online_nodes() -> io::Result<Vec<usize>> {
        match std::fs::read_to_string("/sys/devices/system/node/online") {
            Ok(list) => parse_list(list.trim()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    /// A sysfs list such as "0-1,4"
    pub(super) fn parse_list(list: &str) -> io::Result<Vec<usize>> {
        let invalid = || io::Error::new(io::ErrorKind::InvalidData, format!("node list {list:?}"));
        let mut nodes = Vec::new();
        for range in list.split(',').filter(|range| !range.is_empty()) {
            let (first, last) = range.split_once('-').unwrap_or((range, range));
            let first: usize = first.parse().map_err(|_| invalid())?;
            let last: usize = last.parse().map_err(|_| invalid())?;
            nodes.extend(first..=last);
        }
        Ok(nodes)
    }

    pub(super) fn bind_memory(node: usize) -> io::Result<()> {
        if node >= MASK_BITS {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        let mut mask: NodeMask = [0; MASK_BITS / WORD_BITS];
        mask[node / WORD_BITS] |= 1 << (node % WORD_BITS);
        // SAFETY: the mask holds MAX_NODE - 1 bits, all the kernel reads
        let result = unsafe {
            libc::syscall(libc::SYS_set_mempolicy, libc::MPOL_BIND, mask.as_ptr(), MAX_NODE)
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn unbind_memory() -> io::Result<()> {
        // SAFETY: MPOL_DEFAULT takes no node mask
        let result = unsafe {
            libc::syscall(
                libc::SYS_set_mempolicy,
                libc::MPOL_DEFAULT,
                std::ptr::null::<libc::c_ulong>(),
                0 as libc::c_ulong,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// A mode, its flags included, and its node mask
    pub(crate) struct Saved {
        mode: libc::c_int,
        mask: NodeMask,
    }

    pub(super) fn save() -> io::Result<Saved> {
        let mut saved = Saved {
            mode: 0,
            mask: [0; MASK_BITS / WORD_BITS],
        };
        // SAFETY: the kernel writes one int to mode and at most MAX_NODE - 1
        // bits to the mask; no address, no flags
        let result = unsafe {
            libc::syscall(
                libc::SYS_get_mempolicy,
                &mut saved.mode as *mut libc::c_int,
                saved.mask.as_mut_ptr(),
                MAX_NODE,
                std::ptr::null_mut::<libc::c_void>(),
                0 as libc::c_ulong,
            )
        };
        if result == 0 {
            Ok(saved)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn restore(saved: &Saved) -> io::Result<()> {
        // The kernel reports this flag, but takes it only with MPOL_BIND
        let mode = if saved.mode & !MODE_FLAGS == libc::MPOL_BIND {
            saved.mode
        } else {
            saved.mode & !libc::MPOL_F_NUMA_BALANCING
        };
        // SAFETY: the mask holds MAX_NODE - 1 bits, all the kernel reads
        let result =
            unsafe { libc::syscall(libc::SYS_set_mempolicy, mode, saved.mask.as_ptr(), MAX_NODE) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn memory_policy() -> io::Result<MemoryPolicy> {
        let Saved { mode, mask } = save()?;
        let mode = match mode & !MODE_FLAGS {
            0 => return Ok(MemoryPolicy::none()),
            1 => "preferred",
            2 => "bind",
            3 => "interleave",
            4 => "local",
            5 => "preferred_many",
            6 => "weighted_interleave",
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown memory policy mode {other}"),
                ))
            }
        };
        let nodes = (0..MASK_BITS)
            .filter(|node| mask[node / WORD_BITS] & (1 << (node % WORD_BITS)) != 0)
            .collect();
        Ok(MemoryPolicy { mode, nodes })
    }

    pub(super) fn errno_name(error: &io::Error) -> Option<&'static str> {
        Some(match error.raw_os_error()? {
            libc::EPERM => "EPERM",
            libc::EINVAL => "EINVAL",
            libc::ENOSYS => "ENOSYS",
            libc::EFAULT => "EFAULT",
            libc::ENOMEM => "ENOMEM",
            _ => return None,
        })
    }
}

#[cfg(not(target_os = "linux"))]
mod numa {
    use super::MemoryPolicy;
    use std::io;

    fn unsupported() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, "NUMA memory policies need Linux")
    }

    pub(super) fn cpu_node(_cpu: usize) -> io::Result<usize> {
        Err(unsupported())
    }

    pub(super) fn online_nodes() -> io::Result<Vec<usize>> {
        Ok(Vec::new())
    }

    pub(super) fn bind_memory(_node: usize) -> io::Result<()> {
        Err(unsupported())
    }

    pub(super) fn unbind_memory() -> io::Result<()> {
        Ok(())
    }

    pub(crate) struct Saved;

    pub(super) fn save() -> io::Result<Saved> {
        Ok(Saved)
    }

    pub(super) fn restore(_saved: &Saved) -> io::Result<()> {
        Ok(())
    }

    pub(super) fn memory_policy() -> io::Result<MemoryPolicy> {
        Ok(MemoryPolicy::none())
    }

    pub(super) fn errno_name(_error: &io::Error) -> Option<&'static str> {
        None
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

    #[cfg(target_os = "linux")]
    #[test]
    fn node_lists_parse_like_sysfs() {
        assert_eq!(numa::parse_list("0").unwrap(), vec![0]);
        assert_eq!(numa::parse_list("0-1").unwrap(), vec![0, 1]);
        assert_eq!(numa::parse_list("0-2,5,7-8").unwrap(), vec![0, 1, 2, 5, 7, 8]);
        assert_eq!(numa::parse_list("").unwrap(), Vec::<usize>::new());
        assert!(numa::parse_list("0-x").is_err());
    }

    /// The node that holds the page at `address`
    #[cfg(target_os = "linux")]
    fn page_node(address: *const u8) -> usize {
        const MPOL_F_NODE: libc::c_ulong = 1;
        const MPOL_F_ADDR: libc::c_ulong = 2;
        let mut node: libc::c_int = -1;
        // SAFETY: with these flags the kernel only writes the node to `node`
        let result = unsafe {
            libc::syscall(
                libc::SYS_get_mempolicy,
                &mut node as *mut libc::c_int,
                std::ptr::null_mut::<libc::c_ulong>(),
                0 as libc::c_ulong,
                address,
                MPOL_F_NODE | MPOL_F_ADDR,
            )
        };
        assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
        node as usize
    }

    /// Binding beats first touch: a thread bound to a node other than its
    /// CPU's, where the process may allocate, gets its new pages there
    #[cfg(target_os = "linux")]
    #[test]
    fn bound_thread_allocates_on_its_node() {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let allowed = status
            .lines()
            .find_map(|line| line.strip_prefix("Mems_allowed_list:"))
            .map(|list| numa::parse_list(list.trim()).unwrap())
            .unwrap();
        let before = memory_policy().unwrap();
        std::thread::spawn(move || {
            // SAFETY: no arguments
            let cpu = unsafe { libc::sched_getcpu() };
            let local = numa::cpu_node(cpu as usize).unwrap();
            let node = allowed.iter().copied().find(|&node| node != local).unwrap_or(local);
            numa::bind_memory(node).unwrap();
            assert_eq!(memory_policy().unwrap(), MemoryPolicy { mode: "bind", nodes: vec![node] });
            // Large enough to be a fresh mapping, touched only now
            let pages = vec![1u8; 4 << 20];
            assert_eq!(page_node(pages.as_ptr()), node);
            assert_eq!(page_node(pages[(4 << 20) - 1..].as_ptr()), node);
            let saved = SavedMemoryPolicy::save().unwrap();
            unbind_memory().unwrap();
            assert_eq!(memory_policy().unwrap(), MemoryPolicy::none());
            saved.restore().unwrap();
            assert_eq!(memory_policy().unwrap(), MemoryPolicy { mode: "bind", nodes: vec![node] });
        })
        .join()
        .unwrap();
        // Other threads keep their policy
        assert_eq!(memory_policy().unwrap(), before);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn binding_failures_name_the_errno() {
        // No host has node 1023 online, so the kernel refuses it
        let error = std::thread::spawn(|| numa::bind_memory(1023).unwrap_err()).join().unwrap();
        assert_eq!(numa::errno_name(&error), Some("EINVAL"));
        let error = std::thread::spawn(|| bind_memory_to_node(1023).unwrap_err().to_string())
            .join()
            .unwrap();
        assert!(
            error.starts_with(
                "Cannot bind memory to NUMA node 1023: set_mempolicy(MPOL_BIND) failed: EINVAL: "
            ),
            "{error}"
        );
        assert!(cpu_numa_node(usize::MAX).is_err());
    }
}
