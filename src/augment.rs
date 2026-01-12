use std::collections::HashMap;
use std::path::Path;

use crate::{
    Field, LibrarySql, Source, context, data, dto,
    record::{Key, PersonPath, RecordRepo},
};
use anyhow::{Result, bail};
use async_trait::async_trait;
use rusqlite::Connection;
use wikibase::mediawiki::api::Api;

#[tokio::main]
pub async fn run(db_path: &Path, source: Source, fields: Vec<Field>) -> Result<()> {
    let mut conn = Connection::open(db_path)?;

    let source: Box<dyn Augmentor> = match source {
        Source::Wikidata => Box::new(WikidataAugmentor::new().await),
        Source::Gemini => bail!("gemini augmentor not yet implemented"),
        Source::Json => bail!("json augmentor not yet implemented"),
        Source::Old => unimplemented!("old augmentor not yet implemented"),
    };

    for field in &fields {
        match field {
            Field::Wikidata => {
                augment_wikidata_id(&mut conn, source.as_ref()).await?;
            }
            Field::Photo => {
                augment_photo(&mut conn, source.as_ref()).await?;
            }
            Field::Wikipedia => {
                augment_wikipedia(&mut conn, source.as_ref()).await?;
            }
        }
    }

    Ok(())
}

#[async_trait]
trait Augmentor {
    async fn query_wikidata_id(&self, name: &str) -> Result<Option<String>>;
    async fn query_photo(&self, id: &str) -> Result<Option<data::Photo>>;
    async fn query_wikipedia(&self, id: &str) -> Result<Option<String>>;
}

struct WikidataAugmentor {
    api: Api,
}

#[async_trait]
impl Augmentor for WikidataAugmentor {
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
        if let Some(r) = res["search"].as_array().and_then(|s| s.first())
            && let Some(id) = r["id"].as_str()
        {
            return Ok(Some(id.to_string()));
        }
        Ok(None)
    }

    async fn query_photo(&self, id: &str) -> Result<Option<data::Photo>> {
        let params: HashMap<String, String> = [
            ("action".to_string(), "wbgetentities".to_string()),
            ("ids".to_string(), id.to_string()),
            ("props".to_string(), "claims".to_string()),
        ]
        .iter()
        .cloned()
        .collect();
        let res = self.api.get_query_api_json(&params).await?;

        if let Some(entity) = res["entities"].as_object().and_then(|e| e.get(id))
            && let Some(claims) = entity.get("claims")
            && let Some(claim) = claims.get("P18")
            && let Some(claim) = claim.as_array().and_then(|a| a.first())
            && let Some(mainsnak) = claim.get("mainsnak")
            && let Some(datavalue) = mainsnak.get("datavalue")
            && let Some(value) = datavalue.get("value")
            && let Some(file_name) = value.as_str()
        {
            let attribution = self.fetch_file_attribution(file_name).await?;
            let url = self.fetch_file_url(file_name).await?;

            return Ok(Some(data::Photo {
                url,
                attribution: Some(attribution),
            }));
        }

        Ok(None)
    }

    async fn query_wikipedia(&self, id: &str) -> Result<Option<String>> {
        let params: HashMap<String, String> = [
            ("action".to_string(), "wbgetentities".to_string()),
            ("ids".to_string(), id.to_string()),
            ("props".to_string(), "sitelinks/urls".to_string()),
        ]
        .iter()
        .cloned()
        .collect();
        let res = self.api.get_query_api_json(&params).await?;

        if let Some(entity) = res["entities"].as_object().and_then(|e| e.get(id))
            && let Some(sitelinks) = entity.get("sitelinks")
            && let Some(enwiki) = sitelinks.get("enwiki")
            && let Some(url) = enwiki.get("url")
            && let Some(url_str) = url.as_str()
            && let Some(title) = url_str.split('/').next_back()
            && !title.is_empty()
        {
            return Ok(Some(title.to_string()));
        }

        Ok(None)
    }
}

impl WikidataAugmentor {
    async fn new() -> Self {
        let api = Api::new("https://www.wikidata.org/w/api.php")
            .await
            .unwrap();
        WikidataAugmentor { api }
    }

    async fn fetch_file_attribution(&self, file_name: &str) -> Result<String> {
        let params: HashMap<String, String> = [
            ("action".to_string(), "query".to_string()),
            ("titles".to_string(), format!("File:{}", file_name)),
            ("prop".to_string(), "imageinfo".to_string()),
            ("iiprop".to_string(), "extmetadata".to_string()),
        ]
        .iter()
        .cloned()
        .collect();

        let res = self.api.get_query_api_json(&params).await?;

        if let Some(page) = res["query"]["pages"]
            .as_object()
            .and_then(|p| p.values().next())
            && let Some(imageinfo) = page["imageinfo"].as_array().and_then(|i| i.first())
            && let Some(extmetadata) = imageinfo.get("extmetadata")
        {
            let artist = if let Some(artist) = extmetadata.get("Artist") {
                artist["value"].as_str()
            } else {
                None
            };
            let license_short_name = if let Some(short_name) = extmetadata.get("LicenseShortName") {
                short_name["value"].as_str()
            } else {
                None
            };
            let license_url = if let Some(url) = extmetadata.get("LicenseUrl") {
                url["value"].as_str()
            } else {
                None
            };

            let mut attribution_parts: Vec<String> = Vec::new();
            if let Some(artist_str) = artist {
                attribution_parts.push(artist_str.to_string());
            }

            let license_str = match (license_short_name, license_url) {
                (Some(name), Some(url)) => Some(format!("{} <{}>", name, url)),
                (Some(name), None) => Some(name.to_string()),
                (None, Some(url)) => Some(format!("<{}>", url)),
                (None, None) => None,
            };
            if let Some(license_str) = license_str {
                attribution_parts.push(license_str);
            }

            attribution_parts.push("via Wikimedia Commons".to_string());
            return Ok(attribution_parts.join(", "));
        }

        bail!("could not fetch attribution for file")
    }

    async fn fetch_file_url(&self, file_name: &str) -> Result<String> {
        let params: HashMap<String, String> = [
            ("action".to_string(), "query".to_string()),
            ("titles".to_string(), format!("File:{}", file_name)),
            ("prop".to_string(), "imageinfo".to_string()),
            ("iiprop".to_string(), "url".to_string()),
        ]
        .iter()
        .cloned()
        .collect();

        let res = self.api.get_query_api_json(&params).await?;

        println!(
            "response from wikidata: {}",
            serde_json::to_string_pretty(&res)?
        );

        if let Some(page) = res["query"]["pages"]
            .as_object()
            .and_then(|p| p.values().next())
            && let Some(imageinfo) = page["imageinfo"].as_array().and_then(|i| i.first())
            && let Some(url) = imageinfo.get("url")
            && let Some(url_str) = url.as_str()
        {
            return Ok(url_str.to_string());
        }

        bail!("could not fetch url for file")
    }
}

async fn augment_photo(conn: &mut Connection, source: &dyn Augmentor) -> Result<()> {
    let repo = RecordRepo::new(conn);

    let mut map: HashMap<String, String> = HashMap::new();
    conn.get_entities_with_contact_without_photo(
        &dto::EntityType::Person,
        &data::ContactType::Wikidata,
        |row| {
            map.insert(row.get(0)?, row.get(1)?);
            Ok(())
        },
    )?;

    for (wikidata_id, person_id) in map {
        println!("augmenting photo for {}:{}...", wikidata_id, person_id);
        let photo = source.query_photo(&wikidata_id).await?;
        if let Some(photo) = photo {
            println!("- found {}", photo.url);
            repo.working()?.save(Key::<PersonPath, ()>::new(&person_id).photo(), &photo)?;
        } else {
            println!("- no photo found");
        }
    }
    Ok(())
}

async fn augment_wikidata_id(conn: &mut Connection, source: &dyn Augmentor) -> Result<()> {
    let repo = RecordRepo::new(conn);
    let mut persons_to_augment: Vec<context::Person> = Vec::new();
    conn.get_entities_without_contact(
        &dto::EntityType::Person,
        &data::ContactType::Wikidata,
        |row| {
            persons_to_augment.push(context::Person {
                id: row.get(0)?,
                name: row.get(1)?,
            });

            Ok(())
        },
    )?;

    for person in persons_to_augment {
        println!(
            "augmenting Wikidata ID for {} ({})...",
            person.name, person.id
        );
        let wikidata_id = source.query_wikidata_id(&person.name).await?;
        if let Some(wikidata_id) = wikidata_id {
            println!("- found {}", wikidata_id);
            repo.working()?.save(
                Key::<PersonPath, ()>::new(&person.id).contact(data::ContactType::Wikidata),
                &wikidata_id,
            )?;
        } else {
            println!("- no Wikidata ID found");
        }
    }
    Ok(())
}

async fn augment_wikipedia(conn: &mut Connection, source: &dyn Augmentor) -> Result<()> {
    let repo = RecordRepo::new(conn);
    let mut map: HashMap<String, String> = HashMap::new();
    conn.get_entities_with_contact_without_contact(
        &dto::EntityType::Person,
        &data::ContactType::Wikidata,
        &data::ContactType::Wikipedia,
        |row| {
            map.insert(row.get(0)?, row.get(1)?);

            Ok(())
        },
    )?;

    for (wikidata_id, person_id) in map {
        println!("augmenting wikipedia for {}:{}...", wikidata_id, person_id);
        let wikipedia_url = source.query_wikipedia(&wikidata_id).await?;
        if let Some(wikipedia_url) = wikipedia_url {
            println!("- found {}", wikipedia_url);
            repo.working()?.save(
                Key::<PersonPath, ()>::new(&person_id).contact(data::ContactType::Wikipedia),
                &wikipedia_url,
            )?;
        } else {
            println!("- no wikipedia page found");
        }
    }
    Ok(())
}
