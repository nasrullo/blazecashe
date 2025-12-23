use chrono::Utc;

pub fn current_timestamp() -> u64 {
    Utc::now().timestamp() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn test_current_timestamp() {
        let timestamp = current_timestamp();
        let system_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Timestamp should be within 5 seconds of system time
        assert!(timestamp <= system_time + 5);
        assert!(timestamp >= system_time - 5);
    }

    #[test]
    fn test_current_timestamp_increases() {
        let timestamp1 = current_timestamp();
        thread::sleep(Duration::from_millis(100));
        let timestamp2 = current_timestamp();
        
        // Timestamp should be equal or greater (allowing for clock adjustments)
        assert!(timestamp2 >= timestamp1 - 1);
    }
}
