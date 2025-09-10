use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use crate::{Field, Source, data, repo};
use wikibase::mediawiki::api::Api;

#[tokio::main]
pub async fn run(db_path: &Path, source: Source, fields: Vec<Field>) -> Result<()> {
    let mut repo = repo::Repository::new(db_path)
        .with_context(|| "could not open repository for ingestion")?;

    let source = match source {
        Source::Wikidata => Wikidata::new().await,
    };

    for field in &fields {
        match field {
            Field::Wikidata => {
                augment_wikidata_id(&mut repo, &source).await?;
            }
        }
    }

    Ok(())
}

trait AugmentationSource {
    async fn query_wikidata_id(&self, name: &str) -> Result<Option<String>>;
}

struct Wikidata {
    api: Api,
}

impl AugmentationSource for Wikidata {
    async fn query_wikidata_id(&self, name: &str) -> Result<Option<String>> {
        let params: HashMap<String, String> = [
            ("action".to_string(), "wbsearchentities".to_string()),
            ("search".to_string(), name.to_string()),
            ("language".to_string(), "en".to_string()),
            ("limit".to_string(), "1".to_string()),
            ("type".to_string(), "item".to_string()),
        ]
        .iter()
        .cloned()
        .collect();
        let res = self.api.get_query_api_json(&params).await?;
        if let Some(r) = res["search"].as_array().and_then(|s| s.first()) {
            if let Some(id) = r["id"].as_str() {
                return Ok(Some(id.to_string()));
            }
        }
        Ok(None)
    }
}

impl Wikidata {
    async fn new() -> Self {
        let api = Api::new("https://www.wikidata.org/w/api.php").await.unwrap();
        Wikidata { api }
    }
}

async fn augment_wikidata_id(repo: &mut repo::Repository, source: &Wikidata) -> Result<()> {
    let persons_to_augment = repo.query_persons_without_contact(data::ContactType::Wikidata)?;

    for person in persons_to_augment {
        println!(
            "augmenting Wikidata ID for {} ({})...",
            person.name, person.id
        );
        let wikidata_id = source.query_wikidata_id(&person.name).await?;
        if let Some(wikidata_id) = wikidata_id {
            println!("- found {}", wikidata_id);
            repo.save_person_contact(&person.id, &data::ContactType::Wikidata, &wikidata_id)?;
        } else {
            println!("- no Wikidata ID found");
        }
    }
    Ok(())
}
