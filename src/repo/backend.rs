use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyType {
    Node,
    Ref,
    Secret,
}

impl Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyType::Node => write!(f, "node"),
            KeyType::Ref => write!(f, "ref"),
            KeyType::Secret => write!(f, "secret"),
        }
    }
}

pub trait Backend {
    type Error: std::fmt::Debug + Display + Send + Sync + 'static;

    fn get(&self, key_type: KeyType, key: &str) -> Result<Option<Vec<u8>>, Self::Error>;
    fn set(&self, key_type: KeyType, key: &str, value: &[u8]) -> Result<(), Self::Error>;
    fn list(&self, key_type: KeyType) -> Result<Vec<String>, Self::Error>;
    fn delete(&self, key_type: KeyType, keys: &[&str]) -> Result<usize, Self::Error>;
    fn vacuum(&self) -> Result<(), Self::Error>;
    fn stats(
        &self,
        key_type: KeyType,
    ) -> Result<(usize, std::collections::BTreeMap<usize, usize>), Self::Error>;
}
