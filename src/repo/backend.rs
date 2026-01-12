use std::fmt::Display;

use crate::repo::RepoError;


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyType {
    Node,
    Ref,
}

impl Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyType::Node => write!(f, "node"),
            KeyType::Ref => write!(f, "ref"),
        }
    }
}

pub trait Backend {
    fn get(&self, key_type: KeyType, key: &str) -> Result<Option<Vec<u8>>, RepoError>;
    fn set(&self, key_type: KeyType, key: &str, value: &[u8]) -> Result<(), RepoError>;
    fn list(&self, key_type: KeyType) -> Result<Vec<String>, RepoError>;
    fn delete(&self, key_type: KeyType, keys: &[&str]) -> Result<usize, RepoError>;
    fn vacuum(&self) -> Result<(), RepoError>;
    fn stats(&self, key_type: KeyType) -> Result<(usize, std::collections::BTreeMap<usize, usize>), RepoError>;
}
