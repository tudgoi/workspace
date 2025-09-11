use anyhow::{Context, Result};

use crate::{data, ingest::Ingestor};

pub struct StdinIngestor {
}

impl StdinIngestor {
    pub fn new() -> Result<Self> {
        Ok(StdinIngestor { })
    }
}

impl Ingestor for StdinIngestor {
    async fn query(&self, input: &str) -> Result<Vec<data::Person>> {
        Ok(serde_json::from_str(input)
            .with_context(|| format!("could not parse JSON for person"))?)
    }
}
