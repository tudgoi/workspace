use crate::repo::{Backend, Hash, Repo, RepoError};
use std::{collections::BTreeMap, sync::{Arc, Mutex}};

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
}

#[test]
fn test_repo() {
    let backend = TestBackend::new();
    let mut repo = Repo::new(backend);

    // key level 0
    let k1: Vec<u8> = "name-0".as_bytes().to_vec();
    let k2: Vec<u8> = "age-0".as_bytes().to_vec();

    assert_eq!(repo.read(&k1).unwrap(), None);

    repo.write(k1.clone(), "val1".as_bytes().to_vec()).unwrap();
    assert_eq!(repo.read(&k1).unwrap(), Some("val1".as_bytes().to_vec()));

    repo.write(k2.clone(), "val2".as_bytes().to_vec()).unwrap();
    assert_eq!(repo.read(&k2).unwrap(), Some("val2".as_bytes().to_vec()));
    assert_eq!(repo.read(&k1).unwrap(), Some("val1".as_bytes().to_vec()));

    repo.write(k1.clone(), "val1_updated".as_bytes().to_vec()).unwrap();
    assert_eq!(repo.read(&k1).unwrap(), Some("val1_updated".as_bytes().to_vec()));
}