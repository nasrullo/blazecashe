use bytes::BytesMut;
use parking_lot::Mutex;
use std::sync::Arc;

pub struct MemoryPool {
    buffers: Arc<Mutex<Vec<BytesMut>>>,
    buffer_size: usize,
}

impl MemoryPool {
    pub fn new(buffer_size: usize, initial_count: usize) -> Self {
        let mut buffers = Vec::with_capacity(initial_count);
        for _ in 0..initial_count {
            buffers.push(BytesMut::with_capacity(buffer_size));
        }

        Self {
            buffers: Arc::new(Mutex::new(buffers)),
            buffer_size,
        }
    }

    pub fn get_buffer(&self) -> BytesMut {
        let mut buffers = self.buffers.lock();
        buffers
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.buffer_size))
    }

    pub fn return_buffer(&self, mut buffer: BytesMut) {
        if buffer.capacity() >= self.buffer_size {
            buffer.clear();
            let mut buffers = self.buffers.lock();
            if buffers.len() < 100 {
                // Limit pool size
                buffers.push(buffer);
            }
        }
    }
}

// Global memory pool instance
lazy_static::lazy_static! {
    pub static ref GLOBAL_POOL: MemoryPool = MemoryPool::new(8192, 50);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_buffer_has_requested_capacity() {
        let pool = MemoryPool::new(1024, 2);
        let buf = pool.get_buffer();
        assert!(buf.capacity() >= 1024);
    }

    #[test]
    fn return_buffer_clears_before_reuse() {
        let pool = MemoryPool::new(256, 0);

        let mut b = pool.get_buffer();
        b.extend_from_slice(&[1,2,3,4,5]);
        assert_eq!(b.len(), 5);

        pool.return_buffer(b);

        // Next get should yield a cleared buffer (len 0),
        // likely the same buffer from the pool in this single-threaded test.
        let b2 = pool.get_buffer();
        assert_eq!(b2.len(), 0);
        assert!(b2.capacity() >= 256);
    }
}
