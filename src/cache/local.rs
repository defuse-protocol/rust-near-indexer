use std::sync::Mutex;

use cached::{Cached, SizedCache};

use crate::types::{ParentTransactionHashString, ReceiptOrDataId};

pub struct LocalCache {
    cache: Mutex<SizedCache<ReceiptOrDataId, ParentTransactionHashString>>,
}

impl LocalCache {
    pub fn new(size: usize) -> Self {
        Self {
            cache: Mutex::new(SizedCache::with_size(size)),
        }
    }
}

impl super::TxCache for LocalCache {
    fn get(&self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString> {
        let mut cache = self.cache.lock().unwrap();
        cache.cache_get(key).cloned()
    }

    fn set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let mut cache = self.cache.lock().unwrap();
        cache.cache_set(key, value);
    }
}
