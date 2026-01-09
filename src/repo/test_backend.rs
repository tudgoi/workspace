use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use super::{Backend, Hash, RepoError};

#[derive(Clone)]
pub struct TestBackend {
    store: Arc<Mutex<BTreeMap<[u8; 32], Vec<u8>>>>,
    refs: Arc<Mutex<BTreeMap<String, Hash>>>,
}

impl TestBackend {
    pub fn new() -> Self {
        TestBackend {
            store: Arc::new(Mutex::new(BTreeMap::new())),
            refs: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl Backend for TestBackend {
    fn read(&self, hash: &Hash) -> Result<Vec<u8>, RepoError> {
        let store = self.store.lock().unwrap();
        store
            .get(&hash.0)
            .cloned()
            .ok_or_else(|| RepoError::Backend("hash not found".to_string()))
    }

    fn write(&self, hash: &Hash, blob: &[u8]) -> Result<(), RepoError> {
        let mut store = self.store.lock().unwrap();
        store.insert(hash.0, blob.to_vec());
        Ok(())
    }

    fn set_ref(&self, name: &str, hash: &Hash) -> Result<(), RepoError> {
        let mut refs = self.refs.lock().unwrap();
        refs.insert(name.to_string(), hash.clone());
        Ok(())
    }

    fn get_ref(&self, name: &str) -> Result<Option<Hash>, RepoError> {
        let refs = self.refs.lock().unwrap();
        Ok(refs.get(name).cloned())
    }

    fn list_refs(&self) -> Result<Vec<(String, Hash)>, RepoError> {
        let refs = self.refs.lock().unwrap();
        Ok(refs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn delete_nodes(&self, hashes: &[Hash]) -> Result<usize, RepoError> {
        let mut store = self.store.lock().unwrap();
        let mut deleted = 0;
        for h in hashes {
            if store.remove(&h.0).is_some() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    fn list_all_node_hashes(&self) -> Result<Vec<Hash>, RepoError> {
        let store = self.store.lock().unwrap();
        Ok(store.keys().map(|k| Hash(*k)).collect())
    }

    fn vacuum(&self) -> Result<(), RepoError> {
        Ok(())
    }

    fn stats(&self) -> Result<(usize, std::collections::BTreeMap<usize, usize>), RepoError> {
        let store = self.store.lock().unwrap();
        let mut distribution = std::collections::BTreeMap::new();
        for blob in store.values() {
            *distribution.entry(blob.len()).or_insert(0) += 1;
        }
        Ok((store.len(), distribution))
    }
}
