use std::path::Path;

use anyhow::{Context, Result};
use serde_derive::Deserialize;

use crate::{data, repo, IngestionSource};

#[tokio::main]
pub async fn run(db_path: &Path, source: IngestionSource) -> Result<()> {
    match source {
        IngestionSource::Wikidata => ingest_wikidata(db_path).await,
    }
}

#[derive(Deserialize, Debug)]
struct SparqlResponse {
    results: SparqlResults,
}

#[derive(Deserialize, Debug)]
struct SparqlResults {
    bindings: Vec<SparqlBinding>,
}

#[derive(Deserialize, Debug)]
struct SparqlBinding {
    item: SparqlItem,
}

#[derive(Deserialize, Debug)]
struct SparqlItem {
    value: String,
}

async fn find_wikidata_id(name: &str) -> Result<Option<String>> {
    let query = format!(
        "SELECT ?item WHERE {{
          ?item wdt:P31 wd:Q5;
                rdfs:label \"{}\"@en.
        }}",
        name
    );

    let client = reqwest::Client::new();
    let response = client
        .get("https://query.wikidata.org/sparql")
        .query(&[("format", "json"), ("query", &query)])
        .header("User-Agent", "tudgoi-xform/0.1")
        .send().await
        .with_context(|| "failed to send request to wikidata")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "Wikidata query failed with status: {}",
            response.status()
        );
    }

    let sparql_response: SparqlResponse = response
        .json().await
        .with_context(|| "failed to parse wikidata response")?;

    if let Some(binding) = sparql_response.results.bindings.first() {
        if let Some(id) = binding.item.value.split('/').last() {
            return Ok(Some(id.to_string()));
        }
    }

    Ok(None)
}

async fn ingest_wikidata(db_path: &Path) -> Result<()> {
    let mut repo =
        repo::Repository::new(db_path).with_context(|| "could not open repository for ingestion")?;
    let persons = repo
        .query_all_persons()
        .with_context(|| "could not query all persons for ingestion")?;

    println!("Checking for persons without Wikidata entry...");
    for (id, person) in persons {
        let has_wikidata = person.contacts.map_or(false, |contacts| {
            contacts.contains_key(&data::ContactType::Wikidata)
        });
        if !has_wikidata {
            println!("- Checking {} ({})", person.name, id);
            match find_wikidata_id(&person.name).await {
                Ok(Some(wikidata_id)) => {
                    println!("  Found Wikidata ID: {}", wikidata_id);
                    repo.save_person_contact(&id, &data::ContactType::Wikidata, &wikidata_id)
                        .with_context(|| {
                            format!("failed to save wikidata contact for person {}", id)
                        })?;
                }
                Ok(None) => {
                    println!("  No Wikidata ID found for {}", person.name);
                }
                Err(e) => eprintln!("  Error finding wikidata ID for {}: {:?}", person.name, e),
            }
        }
    }

    Ok(())
}