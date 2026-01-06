use anyhow::{Context, Result};
use rusqlite::OptionalExtension;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write,
    path::Path,
};

use crate::{
    LibrarySql,
    data::{self, ContactType, Tenure},
    dto,
};

pub fn run(db: &Path, output: &Path) -> Result<()> {
    // Create output directories
    let person_dir = output.join("person");
    fs::create_dir_all(&person_dir)
        .with_context(|| format!("could not create person directory at {:?}", person_dir))?;

    let office_dir = output.join("office");
    fs::create_dir_all(&office_dir)
        .with_context(|| format!("could not create office directory at {:?}", office_dir))?;

    // Open repository
    let conn = rusqlite::Connection::open(db)
        .with_context(|| format!("could not open database at {:?}", db))?;

    // Export persons
    let mut ids: Vec<String> = Vec::new();
    conn.get_entity_ids(&dto::EntityType::Person, |row| {
        ids.push(row.get(0)?);
        Ok(())
    })?;

    for id in ids {
        let name = conn.get_entity_name(&dto::EntityType::Person, &id, |row| row.get(0))?;
        let photo = conn
            .get_entity_photo(&dto::EntityType::Person, &id, |row| {
                Ok(data::Photo {
                    url: row.get(0)?,
                    attribution: row.get(1)?,
                })
            })
            .optional()?;
        let mut tenures = Vec::new();
        conn.get_tenures(&id, |row| {
            tenures.push(Tenure {
                office_id: row.get(0)?,
                start: row.get(1)?,
                end: row.get(2)?,
            });

            Ok(())
        })?;
        let mut contacts: BTreeMap<ContactType, String> = BTreeMap::new();
        conn.get_entity_contacts(&dto::EntityType::Person, &id, |row| {
            contacts.insert(row.get(0)?, row.get(1)?);

            Ok(())
        })?;
        let person_data = data::Person {
            name,
            photo,
            contacts: if contacts.is_empty() {
                None
            } else {
                Some(contacts)
            },
            tenures: if tenures.is_empty() {
                None
            } else {
                Some(tenures)
            },
        };
        let toml_string =
            toml::to_string_pretty(&person_data).context("could not serialize person to TOML")?;

        let file_path = person_dir.join(format!("{}.toml", id));
        let mut file = File::create(&file_path)
            .with_context(|| format!("could not create {:?}", file_path))?;
        file.write_all(toml_string.as_bytes())
            .with_context(|| format!("could not write to {:?}", file_path))?;
    }

    // Export offices
    let mut ids: Vec<String> = Vec::new();
    conn.get_entity_ids(&dto::EntityType::Office, |row| {
        ids.push(row.get(0)?);
        Ok(())
    })?;

    for id in ids {
        let name = conn.get_entity_name(&dto::EntityType::Office, &id, |row| row.get(0))?;
        let photo = conn
            .get_entity_photo(&dto::EntityType::Office, &id, |row| {
                Ok(data::Photo {
                    url: row.get(0)?,
                    attribution: row.get(1)?,
                })
            })
            .optional()?;
        let mut contacts: BTreeMap<ContactType, String> = BTreeMap::new();
        conn
            .get_entity_contacts(&dto::EntityType::Office, &id, |row| {
                contacts.insert(row.get(0)?, row.get(1)?);

                Ok(())
            })?;
        let mut supervisors: BTreeMap<data::SupervisingRelation, String> = BTreeMap::new();
        conn.get_office_supervising_offices(&id, |row| {
            supervisors.insert(row.get(0)?, row.get(1)?);
            Ok(())
        })?;
        let office_data = data::Office {
            name,
            photo,
            contacts: if contacts.is_empty() {
                None
            } else {
                Some(contacts)
            },
            supervisors: if supervisors.is_empty() {
                None
            } else {
                Some(supervisors)
            },
        };

        let toml_string =
            toml::to_string_pretty(&office_data).context("could not serialize office to TOML")?;

        let file_path = office_dir.join(format!("{}.toml", id));
        let mut file = File::create(&file_path)
            .with_context(|| format!("could not create {:?}", file_path))?;
        file.write_all(toml_string.as_bytes())
            .with_context(|| format!("could not write to {:?}", file_path))?;
    }

    println!(
        "Successfully exported data to `{}`",
        output.to_string_lossy()
    );

    Ok(())
}
