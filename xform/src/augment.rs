use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use crate::{Field, Source, data, repo};
use wikibase::mediawiki::api::Api;

#[tokio::main]
pub async fn run(db_path: &Path, source: Source, fields: Vec<Field>) -> Result<()> {
    let mut repo = repo::Repository::new(db_path)
        .with_context(|| "could not open repository for ingestion")?;

    let source = match source {
        Source::Wikidata => WikidataAugmentor::new().await,
        Source::Gemini => bail!("gemini augmentor not yet implemented"),
        Source::Json => bail!("json agumentor not yet implemented"),
    };

    for field in &fields {
        match field {
            Field::Wikidata => {
                augment_wikidata_id(&mut repo, &source).await?;
            },
            Field::Photo => {
                augment_photo(&mut repo, &source).await?;
            }
        }
    }

    Ok(())
}

trait Augmentor {
    async fn query_wikidata_id(&self, name: &str) -> Result<Option<String>>;
    async fn query_photo(&self, id: &str) -> Result<Option<data::Photo>>;
}

struct WikidataAugmentor {
    api: Api,
}

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
        if let Some(r) = res["search"].as_array().and_then(|s| s.first()) {
            if let Some(id) = r["id"].as_str() {
                return Ok(Some(id.to_string()));
            }
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

        if let Some(entity) = res["entities"].as_object().and_then(|e| e.get(id)) {
            if let Some(claims) = entity.get("claims") {
                if let Some(claim) = claims.get("P18") {
                    if let Some(claim) = claim.as_array().and_then(|a| a.first()) {
                        if let Some(mainsnak) = claim.get("mainsnak") {
                            if let Some(datavalue) = mainsnak.get("datavalue") {
                                if let Some(value) = datavalue.get("value") {
                                    if let Some(file_name) = value.as_str() {
                                        let attribution = self.fetch_file_attribution(file_name).await?;
                                        let url = self.fetch_file_url(file_name).await?;

                                        return Ok(Some(data::Photo {
                                            url,
                                            attribution: Some(attribution),
                                        }));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }
}

impl WikidataAugmentor {
    async fn new() -> Self {
        let api = Api::new("https://www.wikidata.org/w/api.php").await.unwrap();
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

        println!("response from wikidata: {}", serde_json::to_string_pretty(&res)?);

        if let Some(page) = res["query"]["pages"].as_object().and_then(|p| p.values().next()) {
            if let Some(imageinfo) = page["imageinfo"].as_array().and_then(|i| i.first()) {
                if let Some(extmetadata) = imageinfo.get("extmetadata") {
                    let artist = if let Some(artist) = extmetadata.get("Artist") {
                        artist["value"].as_str()
                    } else { None };
                    let license_short_name = if let Some(short_name) = extmetadata.get("LicenseShortName") {
                        short_name["value"].as_str()
                    } else { None };
                    let license_url = if let Some(url) = extmetadata.get("LicenseUrl") {
                        url["value"].as_str()
                    } else { None };

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
            }
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

        println!("response from wikidata: {}", serde_json::to_string_pretty(&res)?);

        if let Some(page) = res["query"]["pages"].as_object().and_then(|p| p.values().next()) {
            if let Some(imageinfo) = page["imageinfo"].as_array().and_then(|i| i.first()) {
                if let Some(url) = imageinfo.get("url") {
                    if let Some(url) = url.as_str() {
                        return Ok(url.to_string());
                    }
                }
            }
        }

        bail!("could not fetch url for file")
    }
}

async fn augment_photo(repo: &mut repo::Repository, source: &WikidataAugmentor) -> Result<()> {
    let persons_to_augment = repo.query_persons_without_photo()?;

    for person in persons_to_augment {
        println!(
            "augmenting photo for {} ({})...",
            person.name, person.id
        );
        let wikidata_id = repo.query_contact_for_person(&person.id, data::ContactType::Wikidata)?;
        let photo = source.query_photo(&wikidata_id).await?;
        if let Some(photo) = photo {
            println!("- found {}", photo.url);
            repo.save_person_photo(&person.id, &photo)?;
        } else {
            println!("- no Wikidata ID found");
        }
    }
    Ok(())
}

async fn augment_wikidata_id(repo: &mut repo::Repository, source: &WikidataAugmentor) -> Result<()> {
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
