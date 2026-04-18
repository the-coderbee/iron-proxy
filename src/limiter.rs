use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};


#[derive(Clone)]
pub struct RateLimiter {
    limits: Arc<Mutex<HashMap<IpAddr, usize>>>,
    max_requests: usize,
}

impl RateLimiter {
    // constructor
    pub fn new(max_requests: usize) -> Self {
        Self {
            limits: Arc::new(Mutex::new(HashMap::new())),
            max_requests,
        }
    }

    // core logic. returns true if they are blocked
    pub fn is_blocked(&self, ip: IpAddr) -> bool {
        let mut limits = self.limits.lock().unwrap();
        let count = limits.entry(ip).or_insert(0);
        *count += 1;

        *count > self.max_requests
    }
}
