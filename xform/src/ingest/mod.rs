use anyhow::{Context, Result, ensure};
use std::path::Path;
use wikibase::{entity, entity_type};

use crate::{Source, data, dto, graph, ingest::old::OldIngestor, repo};

mod old;

#[tokio::main]
pub async fn run(db_path: &Path, source: Source, dir_path: Option<&Path>) -> Result<()> {
    let mut repo = repo::Repository::new(db_path)
        .with_context(|| "could not open repository for ingestion")?;

    match source {
        Source::Wikidata => unimplemented!("wikidata source not yet implemented"),
        Source::Gemini => {
            unimplemented!("Gemini ingestor not supported")
        }
        Source::Json => {
            unimplemented!("Json ingestor not supported")
        }
        Source::Old => {
            let dir_path =
                dir_path.unwrap_or_else(|| unimplemented!("Reading from input not supported"));

            let ingestor = OldIngestor::new(dir_path)?;
            for result in ingestor {
                let entities =
                    result.with_context(|| format!("could not query from {:?}", source))?;
                for entity in entities {
                    let entity: graph::Entity = entity.into();
                    ingest(&mut repo, entity)?;
                }
            }
        }
    };

    Ok(())
}

fn ingest(repo: &mut repo::Repository, entity: graph::Entity) -> Result<()> {
    let entity_type = entity
        .get_type()
        .with_context(|| format!("entity should have a type"))?;
    let entity_type: dto::EntityType = entity_type.clone().into();
    let id = ingest_entity(repo, &entity_type, entity.get_id(), entity.get_name())?;

    for property in entity.0.values() {
        match property {
            // ignore the ones we handled earlier
            graph::Property::Type(_) | graph::Property::Id(_) | graph::Property::Name(_) => {}

            // handle these now
            graph::Property::Tenure(items) => {
                ensure!(
                    entity_type == dto::EntityType::Person,
                    "Tenure is only allowed for a Person"
                );
                let office: graph::Entity = items.to_vec().into();
                let office_id = ingest_entity(
                    repo,
                    &dto::EntityType::Office,
                    office.get_id(),
                    office.get_name(),
                )?;
                repo.insert_tenure(&id, &office_id)?;
            }
            graph::Property::Photo { url, attribution } => {
                if !repo.entity_photo_exists(&entity_type, &id)? {
                    repo.insert_entity_photo(&entity_type, &id, url, attribution.as_deref())
                        .with_context(|| format!("Failed to ingest photo"))?;
                }
            }
            graph::Property::Contact(contact_type, value) => {
                if !repo.entity_contact_exists(&entity_type, &id, contact_type)? {
                    repo.insert_entity_contact(&entity_type, &id, contact_type, value)
                        .with_context(|| format!("failed to ingest contact"))?;
                }
            }
            graph::Property::Supervisor(relation, supervising_office) => {
                ensure!(
                    entity_type != dto::EntityType::Office,
                    "Supervisor allowed only for Office"
                );
                let supervising_office: graph::Entity = supervising_office.to_vec().into();

                if !repo.office_supervisor_exists(&id, relation)? {
                    let supervising_office_id = ingest_entity(
                        repo,
                        &dto::EntityType::Office,
                        supervising_office.get_id(),
                        supervising_office.get_name(),
                    )?;

                    repo.insert_office_supervisor(&id, relation, &supervising_office_id)?;
                }
            }
        }
    }

    Ok(())
}

fn ingest_entity(
    repo: &mut repo::Repository,
    entity_type: &dto::EntityType,
    id: Option<&str>,
    name: Option<&str>,
) -> Result<String> {
    if let Some(id) = id {
        // id provided. insert if it doesn't already exist
        if !repo.entity_exists(entity_type, id)? {
            // The entity doesn't exist. So we lets insert.
            let name = name
                .with_context(|| format!("entity {:?}:{} doesn't have a name", entity_type, id))?;

            repo.insert_entity(entity_type, &id, name)?;
        }

        Ok(id.to_string())
    } else {
        // id not provided. FTS by name and use it or else insert new
        let name =
            name.with_context(|| format!("entity should have a name if id is not provided"))?;

        let entity = repo
            .search_entities(name, Some(&entity_type))
            .with_context(|| format!("could not search for `{}`", name))?
            .into_iter()
            .next();
        if let Some(entity) = entity {
            Ok(entity.id)
        } else {
            let id = match entity_type {
                dto::EntityType::Person => derive_id_from_person_name(name),
                dto::EntityType::Office => derive_id_from_office_name(name),
            };
            repo.insert_entity(entity_type, &id, name)
                .with_context(|| format!("could not insert entity {:?}:{}", entity_type, id))?;

            Ok(id)
        }
    }
}

fn augment_entity() {}

fn ingest_photo(
    repo: &mut repo::Repository,
    entity_type: &graph::EntityType,
    id: &str,
    url: &str,
    attribution: Option<&str>,
) -> Result<()> {
    Ok(())
}

fn ingest_address(
    repo: &mut repo::Repository,
    entity_type: &graph::EntityType,
    id: &str,
    contact_type: data::ContactType,
    address: &str,
) -> Result<()> {
    match entity_type {
        graph::EntityType::Person => repo.save_person_contact(id, &contact_type, address),
        graph::EntityType::Office => repo.save_office_contact(id, &contact_type, address),
    }
}

fn ingest_email(
    repo: &mut repo::Repository,
    entity_type: &graph::EntityType,
    id: &str,
    contact_type: data::ContactType,
    email: &str,
) -> Result<()> {
    match entity_type {
        graph::EntityType::Person => repo.save_person_contact(id, &contact_type, email),
        graph::EntityType::Office => repo.save_office_contact(id, &contact_type, email),
    }
}

fn ingest_website(
    repo: &mut repo::Repository,
    entity_type: &graph::EntityType,
    id: &str,
    contact_type: data::ContactType,
    website: &str,
) -> Result<()> {
    match entity_type {
        graph::EntityType::Person => repo.save_person_contact(id, &contact_type, website),
        graph::EntityType::Office => repo.save_office_contact(id, &contact_type, website),
    }
}

fn ingest_wikipedia(
    repo: &mut repo::Repository,
    entity_type: &graph::EntityType,
    id: &str,
    contact_type: data::ContactType,
    wikipedia: &str,
) -> Result<()> {
    match entity_type {
        graph::EntityType::Person => repo.save_person_contact(id, &contact_type, wikipedia),
        graph::EntityType::Office => repo.save_office_contact(id, &contact_type, wikipedia),
    }
}

fn derive_id_from_person_name(name: &str) -> String {
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
