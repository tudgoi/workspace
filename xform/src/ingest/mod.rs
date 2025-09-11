use anyhow::{Context, Result, bail};
use std::path::Path;
use tokio::io::AsyncReadExt;

use crate::{
    Source, context, data,
    ingest::{gemini::GeminiIngestor, stdin::StdinIngestor},
    repo,
};

mod gemini;
mod stdin;

pub enum IngestorEnum {
    Gemini(GeminiIngestor),
    Stdin(StdinIngestor),
}

impl Ingestor for IngestorEnum {
    async fn query(&self, input: &str) -> Result<Vec<data::Person>> {
        match self {
            IngestorEnum::Gemini(i) => i.query(input).await,
            IngestorEnum::Stdin(i) => i.query(input).await,
        }
    }
}

#[tokio::main]
pub async fn run(db_path: &Path, source: Source) -> Result<()> {
    let ingestor = match source {
        Source::Wikidata => bail!("wikidata source not yet implemented"),
        Source::Gemini => IngestorEnum::Gemini(GeminiIngestor::new()?),
        Source::Stdin => IngestorEnum::Stdin(StdinIngestor::new()?),
    };

    let mut repo = repo::Repository::new(db_path)
        .with_context(|| "could not open repository for ingestion")?;

    let mut input = String::new();
    tokio::io::stdin()
        .read_to_string(&mut input)
        .await
        .with_context(|| "could not read from stdin")?;
    let persons = ingestor
        .query(&input)
        .await
        .with_context(|| format!("could not query persons from {:?}", source))?;

    for person in &persons {
        let id = &build_id_from_person_name(&person.name);

        println!("ingesting {} ({})...", person.name, id);
        repo.save_person(id, &person.name, None, None, None)
            .with_context(|| format!("could not save person {} ({})", id, person.name))?;

        if let Some(tenures) = person.tenures.as_ref() {
            for tenure in tenures {
                // check for fuzzy match
                let offices = repo.query_office_with_name(&tenure.office)?;
                let office_id = if let Some(office) = offices.first() {
                    office.id.clone()
                } else {
                    // derive office id
                    let office_id = derive_id_from_office_name(&tenure.office);
                    // check if office id already exists
                    if repo.query_office_with_id(&office_id)?.is_none() {
                        // insert new office
                        let office = context::Office {
                            id: office_id.clone(),
                            name: tenure.office.clone(),
                        };
                        println!(" - inserting office {} ({})", office.name, office.id);
                        repo.save_office(&office)?;
                    }

                    office_id
                };
                let tenure = data::Tenure {
                    office: office_id,
                    start: tenure.start.clone(),
                    end: tenure.end.clone(),
                    additional_charge: tenure.additional_charge,
                };
                println!(" - saving tenure as {}", tenure.office);
                repo.save_tenure_for_person(id, &tenure)
                    .with_context(|| format!("could not save tenure"))?;
            }
        }
    }

    Ok(())
}

trait Ingestor {
    async fn query(&self, input: &str) -> Result<Vec<data::Person>>;
}

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

fn derive_id_from_office_name(name: &str) -> String {
    name.split_whitespace()
        .filter_map(|s| s.chars().next())
        .collect::<String>()
        .to_lowercase()
}
