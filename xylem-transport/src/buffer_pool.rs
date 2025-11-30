//! Buffer pool for efficient memory reuse
//!
//! Uses `opool` for lock-free buffer pooling to avoid allocation overhead
//! on the hot path (send/recv).

use opool::{Pool, PoolAllocator, RefGuard};
use std::ops::Deref;

/// Default buffer size (8KB - matches typical recv buffer)
pub const DEFAULT_BUFFER_SIZE: usize = 8192;

/// Default pool capacity (number of pre-allocated buffers)
pub const DEFAULT_POOL_CAPACITY: usize = 1024;

/// Allocator for fixed-size byte buffers
pub struct BufferAllocator {
    size: usize,
}

impl BufferAllocator {
    pub const fn new(size: usize) -> Self {
        Self { size }
    }
}

impl PoolAllocator<Vec<u8>> for BufferAllocator {
    #[inline]
    fn allocate(&self) -> Vec<u8> {
        Vec::with_capacity(self.size)
    }

    #[inline]
    fn reset(&self, obj: &mut Vec<u8>) {
        obj.clear();
    }

    #[inline]
    fn is_valid(&self, obj: &Vec<u8>) -> bool {
        obj.capacity() >= self.size
    }
}

/// A pooled buffer that auto-returns to the pool when dropped
pub type PooledBuffer<'a> = RefGuard<'a, BufferAllocator, Vec<u8>>;

/// Thread-safe buffer pool for reusable byte buffers
pub struct BufferPool {
    pool: Pool<BufferAllocator, Vec<u8>>,
    buffer_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool with default settings
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_POOL_CAPACITY, DEFAULT_BUFFER_SIZE)
    }

    /// Create a buffer pool with custom capacity and buffer size
    pub fn with_capacity(capacity: usize, buffer_size: usize) -> Self {
        let allocator = BufferAllocator::new(buffer_size);
        Self {
            pool: Pool::new(capacity, allocator),
            buffer_size,
        }
    }

    /// Get a buffer from the pool
    #[inline]
    pub fn get(&self) -> PooledBuffer<'_> {
        self.pool.get()
    }

    /// Get the configured buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

/// A buffer that can be either pooled or owned
///
/// This allows APIs to work with both pooled and non-pooled buffers
/// during the transition period.
pub enum MaybePooledBuffer<'a> {
    Pooled(PooledBuffer<'a>),
    Owned(Vec<u8>),
}

impl<'a> MaybePooledBuffer<'a> {
    /// Create from a pooled buffer
    pub fn pooled(buf: PooledBuffer<'a>) -> Self {
        Self::Pooled(buf)
    }

    /// Create from an owned buffer
    pub fn owned(buf: Vec<u8>) -> Self {
        Self::Owned(buf)
    }

    /// Get the data as a slice
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Pooled(buf) => buf,
            Self::Owned(buf) => buf.as_slice(),
        }
    }

    /// Get the length of the data
    pub fn len(&self) -> usize {
        match self {
            Self::Pooled(buf) => buf.len(),
            Self::Owned(buf) => buf.len(),
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert to owned Vec<u8> (copies if pooled)
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Pooled(buf) => buf.to_vec(),
            Self::Owned(buf) => buf,
        }
    }
}

impl<'a> Deref for MaybePooledBuffer<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'a> AsRef<[u8]> for MaybePooledBuffer<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Global buffer pool for transport layer
///
/// Using a global pool allows sharing buffers across all connections
/// without passing pool references everywhere.
pub mod global {
    use super::*;
    use std::sync::OnceLock;

    static GLOBAL_POOL: OnceLock<BufferPool> = OnceLock::new();

    /// Initialize the global buffer pool with custom settings
    ///
    /// Must be called before any get() calls, otherwise defaults are used.
    pub fn init(capacity: usize, buffer_size: usize) {
        let _ = GLOBAL_POOL.get_or_init(|| BufferPool::with_capacity(capacity, buffer_size));
    }

    /// Get a buffer from the global pool
    #[inline]
    pub fn get() -> PooledBuffer<'static> {
        GLOBAL_POOL.get_or_init(BufferPool::new).get()
    }

    /// Get the global pool reference
    pub fn pool() -> &'static BufferPool {
        GLOBAL_POOL.get_or_init(BufferPool::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::with_capacity(4, 1024);

        let mut buf = pool.get();
        assert!(buf.capacity() >= 1024);
        assert_eq!(buf.len(), 0); // reset() clears it

        buf.extend_from_slice(b"hello");
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn test_buffer_reuse() {
        let pool = BufferPool::with_capacity(4, 1024);

        // Get and use a buffer
        {
            let mut buf = pool.get();
            buf.extend_from_slice(b"test data");
        }
        // Buffer returned to pool

        // Get another buffer (should be reused)
        let buf2 = pool.get();
        assert!(buf2.capacity() >= 1024);
        assert_eq!(buf2.len(), 0); // Should be reset
    }

    #[test]
    fn test_maybe_pooled_buffer() {
        let pool = BufferPool::with_capacity(4, 1024);

        // Test pooled variant
        let mut pooled = pool.get();
        pooled.extend_from_slice(b"pooled");
        let maybe = MaybePooledBuffer::pooled(pooled);
        assert_eq!(maybe.as_slice(), b"pooled");

        // Test owned variant
        let owned = vec![1, 2, 3];
        let maybe = MaybePooledBuffer::owned(owned);
        assert_eq!(maybe.as_slice(), &[1, 2, 3]);
    }
}
