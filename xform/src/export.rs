use anyhow::{Context, Result};
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

use crate::repo;

pub fn run(db: &Path, output: &Path) -> Result<()> {
    // Create output directories
    let person_dir = output.join("person");
    fs::create_dir_all(&person_dir)
        .with_context(|| format!("could not create person directory at {:?}", person_dir))?;

    let office_dir = output.join("office");
    fs::create_dir_all(&office_dir)
        .with_context(|| format!("could not create office directory at {:?}", office_dir))?;

    // Open repository
    let repo = repo::Repository::new(db)
        .with_context(|| format!("could not open repository at {:?}", db))?;

    // Export persons
    let persons = repo
        .query_all_persons()
        .with_context(|| "could not query all persons")?;
    for (id, person_data) in persons {
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
        .query_all_offices()
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