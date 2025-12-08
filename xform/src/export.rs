use anyhow::{Context, Result};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

use crate::{data::{self, Tenure}, repo, LibrarySql};

pub fn run(db: &Path, output: &Path) -> Result<()> {
    // Create output directories
    let person_dir = output.join("person");
    fs::create_dir_all(&person_dir)
        .with_context(|| format!("could not create person directory at {:?}", person_dir))?;

    let office_dir = output.join("office");
    fs::create_dir_all(&office_dir)
        .with_context(|| format!("could not create office directory at {:?}", office_dir))?;

    // Open repository
    let mut conn = rusqlite::Connection::open(db)
        .with_context(|| format!("could not open database at {:?}", db))?;
    let repo = repo::Repository::new(&mut conn)
        .with_context(|| format!("could not open repository at {:?}", db))?;

    // Export persons
    let persons = repo
        .list_all_person_ids()
        .with_context(|| "could not query all persons")?;
    for id in persons {
        let person_dto = repo
            .get_person(&id)?
            .with_context(|| format!("person {} not found", id))?;

        let mut tenures = Vec::new();
        repo.conn.get_tenures(&id, |row| {
            tenures.push(Tenure {
                office_id: row.get(0)?,
                start: row.get(1)?,
                end: row.get(2)?,
            });
            
            Ok(())
        })?;

        let person_data = data::Person {
            name: person_dto.name,
            photo: person_dto.photo,
            contacts: person_dto.contacts.filter(|c| !c.is_empty()),
            tenures: if tenures.is_empty() {
                None
            } else {
                Some(tenures)
            },
        };
        let toml_string =
            toml::to_string_pretty(&person_data).context("could not serialize person to TOML")?;

        let file_path = person_dir.join(format!("{}.toml", id));
        let mut file =
            File::create(&file_path).with_context(|| format!("could not create {:?}", file_path))?;
        file.write_all(toml_string.as_bytes())
            .with_context(|| format!("could not write to {:?}", file_path))?;
    }

    // Export offices
    let offices = repo
        .list_all_office_data()
        .with_context(|| "could not query all offices")?;

    for (id, office_data) in offices {
        let toml_string =
            toml::to_string_pretty(&office_data).context("could not serialize office to TOML")?;

        let file_path = office_dir.join(format!("{}.toml", id));
        let mut file =
            File::create(&file_path).with_context(|| format!("could not create {:?}", file_path))?;
        file.write_all(toml_string.as_bytes())
            .with_context(|| format!("could not write to {:?}", file_path))?;
    }

    println!(
        "Successfully exported data to `{}`",
        output.to_string_lossy()
    );

    Ok(())
}