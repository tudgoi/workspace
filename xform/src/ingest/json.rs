use anyhow::{Context, Result};

use crate::{data, ingest::Ingestor};

pub struct JsonIngestor {
}

impl JsonIngestor {
    pub fn new() -> Result<Self> {
        Ok(JsonIngestor { })
    }
}

impl Ingestor for JsonIngestor {
    async fn query(&self, input: &str) -> Result<Vec<data::Person>> {
        Ok(serde_json::from_str(input)
            .with_context(|| format!("could not parse JSON for person"))?)
    }
}
