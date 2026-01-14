use crate::repo::{
    RepoError, ToRepoError,
    backend::{Backend, KeyType},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;

#[derive(Debug, Error, Clone, Copy)]
pub enum TestBackendError {
    #[error("test backend error")]
    Test,
}

impl ToRepoError for TestBackendError {
    fn to_repo_error(self) -> RepoError {
        RepoError::Backend(Box::new(self))
    }
}

#[derive(Clone)]
pub struct TestBackend {
    data: Arc<Mutex<BTreeMap<String, BTreeMap<String, Vec<u8>>>>>,
}

impl TestBackend {
    pub fn new() -> Self {
        TestBackend {
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl Backend for TestBackend {
    type Error = TestBackendError;

    fn get(&self, key_type: KeyType, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let data = self.data.lock().unwrap();
        Ok(data
            .get(&key_type.to_string())
            .and_then(|map| map.get(key).cloned()))
    }

    fn set(&self, key_type: KeyType, key: &str, value: &[u8]) -> Result<(), Self::Error> {
        let mut data = self.data.lock().unwrap();
        data.entry(key_type.to_string())
            .or_default()
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn list(&self, key_type: KeyType) -> Result<Vec<String>, Self::Error> {
        let data = self.data.lock().unwrap();
        Ok(data
            .get(&key_type.to_string())
            .map(|map| map.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn delete(&self, key_type: KeyType, keys: &[&str]) -> Result<usize, Self::Error> {
        let mut data = self.data.lock().unwrap();
        if let Some(map) = data.get_mut(&key_type.to_string()) {
            let mut count = 0;
            for k in keys {
                if map.remove(*k).is_some() {
                    count += 1;
                }
            }
            Ok(count)
        } else {
            Ok(0)
        }
    }

    fn vacuum(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn stats(
        &self,
        key_type: KeyType,
    ) -> Result<(usize, std::collections::BTreeMap<usize, usize>), Self::Error> {
        let data = self.data.lock().unwrap();
        let mut distribution = std::collections::BTreeMap::new();
        let count = if let Some(map) = data.get(&key_type.to_string()) {
            for blob in map.values() {
                *distribution.entry(blob.len()).or_insert(0) += 1;
            }
            map.len()
        } else {
            0
        };
        Ok((count, distribution))
    }
}
