use anyhow::{Context, Result, ensure};
use rusqlite::Connection;
use std::path::Path;

use crate::{
    LibrarySql, Source,
    dto::{self, Entity},
    graph,
    ingest::{derive::derive_id, old::OldIngestor},
    record::{Key, PersonPath, OfficePath, RecordRepo},
};
use rusqlite::OptionalExtension;

mod derive;
mod old;

#[tokio::main]
pub async fn run(db_path: &Path, source: Source, dir_path: Option<&Path>) -> Result<()> {
    let mut ingestor = Ingestor::new(db_path)?;

    ingestor.ingest(source, dir_path).await
}

struct Ingestor {
    conn: Connection,
}

impl Ingestor {
    fn new(db_path: &Path) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        Ok(Self { conn })
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
                        if let Err(e) = ingest_entity(&mut self.conn, entity).await {
                            println!("ingestion failed: {}", e)
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

async fn ingest_entity(conn: &mut Connection, entity: graph::Entity) -> Result<()> {
    let entity_type = entity.get_type().context("entity should have a type")?;
    let entity_type: dto::EntityType = entity_type.clone().into();
    let id =
        ingest_entity_id_or_name(conn, &entity_type, entity.get_id(), entity.get_name()).await?;

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
                let office_id = ingest_entity_id_or_name(
                    conn,
                    &dto::EntityType::Office,
                    office.get_id(),
                    office.get_name(),
                )
                .await?;
                RecordRepo::new(conn).root()?.save(
                    Key::<PersonPath, ()>::new(&id).tenure(&office_id, None),
                    &None,
                )?;
            }
            graph::Property::Photo { url, attribution } => {
                if !conn.exists_entity_photo(&entity_type, &id, |row| row.get(0))? {
                    let photo = crate::data::Photo {
                        url: url.clone(),
                        attribution: attribution.clone(),
                    };
                    let mut repo = RecordRepo::new(conn);
                    match entity_type {
                        dto::EntityType::Person => {
                            repo.root()?.save(Key::<PersonPath, ()>::new(&id).photo(), &photo)?;
                        }
                        dto::EntityType::Office => {
                            repo.root()?.save(Key::<OfficePath, ()>::new(&id).photo(), &photo)?;
                        }
                    }
                }
            }
            graph::Property::Contact(contact_type, value) => {
                if !conn.exists_entity_contact(&entity_type, &id, contact_type, |row| row.get(0))? {
                    let mut repo = RecordRepo::new(conn);
                    match entity_type {
                        dto::EntityType::Person => {
                            repo.root()?.save(
                                Key::<PersonPath, ()>::new(&id).contact(contact_type.clone()),
                                value,
                            )?;
                        }
                        dto::EntityType::Office => {
                            repo.root()?.save(
                                Key::<OfficePath, ()>::new(&id).contact(contact_type.clone()),
                                value,
                            )?;
                        }
                    }
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

                if !conn.exists_office_supervisor(&id, relation, |row| row.get(0))? {
                    let supervising_office_id = ingest_entity_id_or_name(
                        conn,
                        &dto::EntityType::Office,
                        supervising_office.get_id(),
                        supervising_office.get_name(),
                    )
                    .await?;

                    RecordRepo::new(conn).root()?.save(
                        Key::<OfficePath, ()>::new(&id).supervisor(relation.clone()),
                        &supervising_office_id,
                    )?;
                }
            }
        }
    }

    Ok(())
}

fn escape_for_fts(input: &str) -> String {
    let mut s = String::from("\"");
    for c in input.chars() {
        if c == '"' {
            s.push_str("\"\""); // escape quotes by doubling
        } else {
            s.push(c);
        }
    }
    s.push('"');
    s
}

async fn ingest_entity_id_or_name(
    conn: &mut Connection,
    entity_type: &dto::EntityType,
    id: Option<&str>,
    name: Option<&str>,
) -> Result<String> {
    if let Some(id) = id {
        // id provided. insert if it doesn't already exist
        if !conn.exists_entity(entity_type, id, |row| row.get(0))? {
            // The entity doesn't exist. So we lets insert.
            let name = name
                .with_context(|| format!("entity {:?}:{} doesn't have a name", entity_type, id))?;

            let mut repo = RecordRepo::new(conn);
            match entity_type {
                dto::EntityType::Person => {
                    repo.root()?.save(Key::<PersonPath, ()>::new(id).name(), &name.to_string())?;
                }
                dto::EntityType::Office => {
                    repo.root()?.save(Key::<OfficePath, ()>::new(id).name(), &name.to_string())?;
                }
            }
        }

        Ok(id.to_string())
    } else {
        // id not provided. FTS by name and use it or else insert new
        let name = name.context("entity should have a name if id is not provided")?;
        let query = escape_for_fts(name);
        let entity = conn
            .search_entity(Some(entity_type), &query, |row| {
                Ok(Entity {
                    typ: row.get(0)?,
                    id: row.get(1)?,
                    name: row.get(2)?,
                })
            })
            .optional()?;
        if let Some(entity) = entity {
            Ok(entity.id)
        } else {
            let id = derive_id(entity_type, name);
            let mut repo = RecordRepo::new(conn);
            match entity_type {
                dto::EntityType::Person => {
                    repo.root()?.save(Key::<PersonPath, ()>::new(&id).name(), &name.to_string())?;
                }
                dto::EntityType::Office => {
                    repo.root()?.save(Key::<OfficePath, ()>::new(&id).name(), &name.to_string())?;
                }
            }

            Ok(id)
        }
    }
}
