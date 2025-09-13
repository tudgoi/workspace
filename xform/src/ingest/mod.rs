use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::io::AsyncReadExt;

use crate::{
    Source, data,
    ingest::{gemini::GeminiIngestor, json::JsonIngestor},
    repo,
};

mod gemini;
mod json;

pub enum IngestorEnum {
    Gemini(GeminiIngestor),
    Json(JsonIngestor),
}

impl Ingestor for IngestorEnum {
    async fn query(&self, input: &str) -> Result<Data> {
        match self {
            IngestorEnum::Gemini(i) => i.query(input).await,
            IngestorEnum::Json(i) => i.query(input).await,
        }
    }
}

#[tokio::main]
pub async fn run(db_path: &Path, source: Source) -> Result<()> {
    let ingestor = match source {
        Source::Wikidata => bail!("wikidata source not yet implemented"),
        Source::Gemini => IngestorEnum::Gemini(GeminiIngestor::new()?),
        Source::Json => IngestorEnum::Json(JsonIngestor::new()?),
    };

    let mut repo = repo::Repository::new(db_path)
        .with_context(|| "could not open repository for ingestion")?;

    let mut input = String::new();
    tokio::io::stdin()
        .read_to_string(&mut input)
        .await
        .with_context(|| "could not read from stdin")?;
    let data = ingestor
        .query(&input)
        .await
        .with_context(|| format!("could not query from {:?}", source))?;
    
    for office in &data.offices {
        repo.save_office(&office.id, &office.value)
            .with_context(|| format!("could not save office {}", office.id))?;
    }

    for person in &data.persons {
        println!("ingesting {} ({})...", person.value.name, person.id);
        repo.save_person(&person.id, &person.value.name, None, None, None)
            .with_context(|| format!("could not ingest person {} ({})", person.id, person.value.name))?;

        if let Some(tenures) = person.value.tenures.as_ref() {
            repo.save_tenures_for_person(&person.id, tenures)
                .with_context(|| format!("could not ingest tenures"))?;
        }
    }

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersonWithId {
    pub id: String,
    pub value: data::Person,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OfficeWithId {
    pub id: String,
    pub value: data::Office,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Data {
    pub offices: Vec<OfficeWithId>,
    pub persons: Vec<PersonWithId>,
}

trait Ingestor {
    async fn query(&self, input: &str) -> Result<Data>;
}

#[allow(dead_code)]
fn build_id_from_person_name(name: &str) -> String {
    let parts: Vec<&str> = name.split_whitespace().collect();
    if parts.is_empty() {
        return String::new();
    }

    let first_name = parts[0];
    let initials: String = parts
        .iter()
        .skip(1)
        .filter_map(|p| p.chars().next())
        .collect();

    let max_len = 8;
    let initials_len = initials.len();

    if first_name.len() + initials_len <= max_len {
        format!("{}{}", first_name, initials).to_lowercase()
    } else {
        let first_name_len = max_len.saturating_sub(initials_len);
        let truncated_first_name = first_name.chars().take(first_name_len).collect::<String>();
        format!("{}{}", truncated_first_name, initials).to_lowercase()
    }
}

#[allow(dead_code)]
fn derive_id_from_office_name(name: &str) -> String {
    name.split_whitespace()
        .filter_map(|s| s.chars().next())
        .collect::<String>()
        .to_lowercase()
}
