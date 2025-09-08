use std::sync::Mutex;

use cached::{Cached, SizedCache};

use crate::types::{ParentTransactionHashString, ReceiptOrDataId};

pub struct LocalCache {
    // This is a main pipe for receipt-tx cache for the data we definitely want to store
    watchlist_cache: Mutex<SizedCache<ReceiptOrDataId, ParentTransactionHashString>>,
    // This is a secondary pipe for receipt-tx cache for the data we might want to store
    // (e.g. we observed a receipt that involves any account of interest, but the parent tx
    // did not involve any account of interest, so we didn't store it initially)
    potential_cache: Mutex<SizedCache<ReceiptOrDataId, ParentTransactionHashString>>,
}

impl LocalCache {
    pub fn new(size: usize) -> Self {
        Self {
            watchlist_cache: Mutex::new(SizedCache::with_size(size)),
            potential_cache: Mutex::new(SizedCache::with_size(size)),
        }
    }
}

impl super::TxCache for LocalCache {
    fn get(&self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString> {
        let mut cache = self.watchlist_cache.lock().unwrap();
        cache.cache_get(key).cloned()
    }

    fn set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let mut cache = self.watchlist_cache.lock().unwrap();
        cache.cache_set(key, value);
    }

    fn potential_get(&self, key: &ReceiptOrDataId) -> Option<ParentTransactionHashString> {
        let mut cache = self.potential_cache.lock().unwrap();
        cache.cache_get(key).cloned()
    }

    fn potential_set(&self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        let mut cache = self.potential_cache.lock().unwrap();
        cache.cache_set(key, value);
    }
}
