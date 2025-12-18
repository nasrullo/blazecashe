use crate::utils::Result;
use crate::FnvHashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

type SharedFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

#[allow(dead_code)]
pub struct SingleFlight {
    calls: Arc<Mutex<FnvHashMap<String, SharedFuture<Vec<u8>>>>>,
    writes: Arc<Mutex<FnvHashMap<String, SharedFuture<()>>>>,
}

impl SingleFlight {
    pub fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(FnvHashMap::default())),
            writes: Arc::new(Mutex::new(FnvHashMap::default())),
        }
    }

    pub async fn do_call<F, Fut>(&self, key: &str, f: F) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        let key = key.to_string();

        // Simple deduplication: check if key is being processed
        {
            let mut calls = self.calls.lock().await;
            if calls.contains_key(&key) {
                // Key is being processed, execute directly (simplified)
                drop(calls);
                return f().await;
            }
            // Mark key as being processed
            calls.insert(key.clone(), Box::pin(async { Ok(vec![]) }));
        }

        // Execute the function
        let result = f().await;

        // Clean up
        self.calls.lock().await.remove(&key);

        result
    }

    pub async fn do_write<F, Fut>(&self, key: &str, _value: &[u8], f: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let key = key.to_string();

        // Simple deduplication: check if key is being written
        {
            let mut writes = self.writes.lock().await;
            if writes.contains_key(&key) {
                // Key is being written, execute directly (simplified)
                drop(writes);
                return f().await;
            }
            // Mark key as being written
            writes.insert(key.clone(), Box::pin(async { Ok(()) }));
        }

        // Execute the function
        let result = f().await;

        // Clean up
        self.writes.lock().await.remove(&key);

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_singleflight_read_basic() {
        let flight = SingleFlight::new();
        let result = flight
            .do_call("test", || async { Ok(b"data".to_vec()) })
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"data");
    }

    #[tokio::test]
    async fn test_singleflight_write_basic() {
        let flight = SingleFlight::new();
        let result = flight.do_write("test", b"value", || async { Ok(()) }).await;
        assert!(result.is_ok());
    }
}
