use std::path::Path;

use anyhow::{Context, Result};
use reqwest::Client;
use serde_derive::Deserialize;

use crate::{
    context, data, dto, repo, Field, Source
};

#[tokio::main]
pub async fn run(db_path: &Path, source: Source, fields: Vec<Field>) -> Result<()> {
    let mut repo = repo::Repository::new(db_path)
        .with_context(|| "could not open repository for ingestion")?;
    
    let dto_fields: Vec<dto::Field> = fields
        .iter()
        .map(|f| match f {
            Field::Wikidata => dto::Field::Wikidata,
        })
        .collect();
    let persons = repo.query_persons_without(dto_fields)?;
    
    let source = match source {
        Source::Wikidata => Wikidata::new()
    };
    
    for field in &fields {
        for person in &persons {
            match field {
                Field::Wikidata => {
                    augment_wikidata_id(person, &mut repo, &source).await?;
                }
            }
        }
    }
    
    Ok(())
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

trait AugmentationSource {
    async fn query_wikidata_id(&self, name: &str) -> Result<Option<String>>;
}

struct Wikidata {
    client: Client,
}

impl AugmentationSource for Wikidata {
    async fn query_wikidata_id(&self, name: &str) -> Result<Option<String>> {
        let query = format!(
            "SELECT ?item WHERE {{
          ?item wdt:P31 wd:Q5;
                rdfs:label \"{}\"@en.
        }}",
            name
        );

        let response = self
            .client
            .get("https://query.wikidata.org/sparql")
            .query(&[("format", "json"), ("query", &query)])
            .header("User-Agent", "tudgoi-xform/0.1")
            .send()
            .await
            .with_context(|| "failed to send request to wikidata")?;

        if !response.status().is_success() {
            anyhow::bail!("Wikidata query failed with status: {}", response.status());
        }

        let sparql_response: SparqlResponse = response
            .json()
            .await
            .with_context(|| "failed to parse wikidata response")?;

        if let Some(binding) = sparql_response.results.bindings.first() {
            if let Some(id) = binding.item.value.split('/').last() {
                return Ok(Some(id.to_string()));
            }
        }

        Ok(None)
    }
}

impl Wikidata {
    fn new() -> Self {
        let client = Client::new();
        Wikidata { client }
    }
}

async fn augment_wikidata_id(
    person: &context::Person,
    repo: &mut repo::Repository,
    source: &Wikidata,
) -> Result<()> {
    println!("augmenting Wikidata ID for {} ({})...", person.name, person.id);
    let wikidata_id = source.query_wikidata_id(&person.name).await?;
    if let Some(wikidata_id) = wikidata_id {
        println!("- found {}", wikidata_id);
        repo.save_person_contact(&person.id, &data::ContactType::Wikidata, &wikidata_id)?;
    } else {
        println!("- no Wikidata ID found");
    }

    Ok(())
}
