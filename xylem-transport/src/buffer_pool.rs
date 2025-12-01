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

    #[test]
    fn test_buffer_pool_default() {
        let pool = BufferPool::default();
        let buf = pool.get();
        assert!(buf.capacity() >= DEFAULT_BUFFER_SIZE);
        assert_eq!(pool.buffer_size(), DEFAULT_BUFFER_SIZE);
    }

    #[test]
    fn test_buffer_pool_custom_size() {
        let pool = BufferPool::with_capacity(8, 256);
        assert_eq!(pool.buffer_size(), 256);

        let buf = pool.get();
        assert!(buf.capacity() >= 256);
    }

    #[test]
    fn test_maybe_pooled_buffer_len_and_is_empty() {
        let pool = BufferPool::with_capacity(4, 1024);

        // Empty pooled buffer
        let pooled = pool.get();
        let maybe = MaybePooledBuffer::pooled(pooled);
        assert_eq!(maybe.len(), 0);
        assert!(maybe.is_empty());

        // Non-empty owned buffer
        let owned = vec![1, 2, 3, 4, 5];
        let maybe = MaybePooledBuffer::owned(owned);
        assert_eq!(maybe.len(), 5);
        assert!(!maybe.is_empty());
    }

    #[test]
    fn test_maybe_pooled_buffer_into_vec() {
        let pool = BufferPool::with_capacity(4, 1024);

        // Pooled buffer converts to owned
        let mut pooled = pool.get();
        pooled.extend_from_slice(b"test");
        let maybe = MaybePooledBuffer::pooled(pooled);
        let vec = maybe.into_vec();
        assert_eq!(vec, b"test");

        // Owned buffer passes through
        let owned = vec![1, 2, 3];
        let maybe = MaybePooledBuffer::owned(owned);
        let vec = maybe.into_vec();
        assert_eq!(vec, &[1, 2, 3]);
    }

    #[test]
    fn test_maybe_pooled_buffer_deref() {
        let pool = BufferPool::with_capacity(4, 1024);

        let mut pooled = pool.get();
        pooled.extend_from_slice(b"deref test");
        let maybe = MaybePooledBuffer::pooled(pooled);

        // Test Deref
        let slice: &[u8] = &maybe;
        assert_eq!(slice, b"deref test");
    }

    #[test]
    fn test_maybe_pooled_buffer_as_ref() {
        let owned = vec![5, 6, 7, 8];
        let maybe = MaybePooledBuffer::owned(owned);

        // Test AsRef
        let slice: &[u8] = maybe.as_ref();
        assert_eq!(slice, &[5, 6, 7, 8]);
    }

    #[test]
    fn test_buffer_allocator_is_valid() {
        let allocator = BufferAllocator::new(1024);

        // Valid buffer
        let valid_buf = Vec::with_capacity(1024);
        assert!(allocator.is_valid(&valid_buf));

        // Also valid (larger capacity)
        let larger_buf = Vec::with_capacity(2048);
        assert!(allocator.is_valid(&larger_buf));

        // Invalid buffer (too small capacity)
        let small_buf = Vec::with_capacity(512);
        assert!(!allocator.is_valid(&small_buf));
    }

    #[test]
    fn test_buffer_pool_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let pool = Arc::new(BufferPool::with_capacity(64, 128));
        let mut handles = vec![];

        for _ in 0..8 {
            let pool_clone = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let mut buf = pool_clone.get();
                    buf.extend_from_slice(b"concurrent");
                    assert_eq!(buf.len(), 10);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
