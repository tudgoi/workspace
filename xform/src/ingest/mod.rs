use anyhow::{Context, Result, ensure};
use std::path::Path;

use crate::{dto, graph, ingest::{derive::derive_id, old::OldIngestor}, repo, Source};

mod old;
mod derive;

#[tokio::main]
pub async fn run(db_path: &Path, source: Source, dir_path: Option<&Path>) -> Result<()> {
    let mut ingestor = Ingestor::new(db_path)?;

    ingestor.ingest(source, dir_path).await
}

struct Ingestor {
    repo: repo::Repository,
}

impl Ingestor {
    fn new(db_path: &Path) -> Result<Self> {
        let repo = repo::Repository::new(db_path)
            .with_context(|| "could not open repository for ingestion")?;
        
        Ok(Self {
            repo,
        })
    }
    
    async fn ingest(&mut self, source: Source, dir_path: Option<&Path>) -> Result<()> {
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
                    if let Err(e) = self.ingest_entity(entity).await {
                        println!("ingestion failed: {}", e)
                    }
                }
            }
        }
    }
    Ok(())
    }

async fn ingest_entity(&mut self, entity: graph::Entity) -> Result<()> {
    let entity_type = entity
        .get_type()
        .with_context(|| format!("entity should have a type"))?;
    let entity_type: dto::EntityType = entity_type.clone().into();
    let id = self.ingest_entity_id_or_name(&entity_type, entity.get_id(), entity.get_name()).await?;

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
                let office_id = self.ingest_entity_id_or_name(
                    &dto::EntityType::Office,
                    office.get_id(),
                    office.get_name(),
                ).await?;
                self.repo.insert_person_office_tenure(&id, &office_id)?;
            }
            graph::Property::Photo { url, attribution } => {
                if !self.repo.entity_photo_exists(&entity_type, &id)? {
                    self.repo.insert_entity_photo(&entity_type, &id, url, attribution.as_deref())
                        .with_context(|| format!("Failed to ingest photo"))?;
                }
            }
            graph::Property::Contact(contact_type, value) => {
                if !self.repo.exists_entity_contact(&entity_type, &id, contact_type)? {
                    self.repo.insert_entity_contact(&entity_type, &id, contact_type, value)
                        .with_context(|| format!("failed to ingest contact"))?;
                }
            }
            graph::Property::Supervisor(relation, supervising_office) => {
                ensure!(
                    entity_type == dto::EntityType::Office,
                    "{:?} does not support {:?}",
                    entity_type,
                    relation
                );
                let supervising_office: graph::Entity = supervising_office.to_vec().into();

                if !self.repo.exists_office_supervisor(&id, relation)? {
                    let supervising_office_id = self.ingest_entity_id_or_name(
                        &dto::EntityType::Office,
                        supervising_office.get_id(),
                        supervising_office.get_name(),
                    ).await?;

                    self.repo.insert_office_supervisor(&id, relation, &supervising_office_id)?;
                }
            }
        }
    }

    Ok(())
}

async fn ingest_entity_id_or_name(
    &mut self,
    entity_type: &dto::EntityType,
    id: Option<&str>,
    name: Option<&str>,
) -> Result<String> {
    if let Some(id) = id {
        // id provided. insert if it doesn't already exist
        if !self.repo.exists_entity(entity_type, id)? {
            // The entity doesn't exist. So we lets insert.
            let name = name
                .with_context(|| format!("entity {:?}:{} doesn't have a name", entity_type, id))?;

            self.repo.insert_entity(entity_type, &id, name)?;
        }

        Ok(id.to_string())
    } else {
        // id not provided. FTS by name and use it or else insert new
        let name =
            name.with_context(|| format!("entity should have a name if id is not provided"))?;

        let entity = self.repo
            .search_entity(name, Some(&entity_type))
            .with_context(|| format!("could not search for `{}`", name))?
            .into_iter()
            .next();
        if let Some(entity) = entity {
            Ok(entity.id)
        } else {
            let id = derive_id(entity_type, name);
            self.repo.insert_entity(entity_type, &id, name)
                .with_context(|| format!("could not insert entity {:?}:{}", entity_type, id))?;

            Ok(id)
        }
    }
}
}
