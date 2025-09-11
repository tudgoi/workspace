use std::{collections::HashMap, path::Path};
use anyhow::{Context, Result, bail};
use tokio::io::AsyncReadExt;

use crate::{data, ingest::gemini::GeminiIngestor, repo, Source};

mod gemini;

#[tokio::main]
pub async fn run(db_path: &Path, source: Source) -> Result<()> {
    let ingestor = match source {
        Source::Wikidata => bail!("wikidata source not yet implemented"),
        Source::Gemini => GeminiIngestor::new(),
    }?;

    let mut repo = repo::Repository::new(db_path)
        .with_context(|| "could not open repository for ingestion")?;

    let mut query = String::new();
    tokio::io::stdin().read_to_string(&mut query).await
        .with_context(|| "could not read from stdin")?;
    let persons = ingestor.query(&query).await
        .with_context(|| format!("could not query persons from {:?}", source))?;
    for (id, person) in &persons {
        repo.save_person(&id, &person, None)
            .with_context(|| format!("could not save person {} ({})", id, person.name))?;
    }

    Ok(())
}

trait Ingestor {
    async fn query(&self, query: &str) -> Result<HashMap<String, data::Person>>;
}
