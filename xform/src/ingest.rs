use anyhow::Result;

use crate::IngestionSource;

pub fn run(source: IngestionSource) -> Result<()> {
    match source {
        IngestionSource::Wikidata => ingest_wikidata()
    }
}

fn ingest_wikidata() -> Result<()> {
    println!("ingesting from wikidata");

    Ok(())
}