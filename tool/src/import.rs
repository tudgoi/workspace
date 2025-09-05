use anyhow::bail;
use anyhow::{Context, Result, ensure};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::from_toml_file;
use super::{data, repository};

fn get_updated(file_path: &Path) -> Result<String> {
    let path_str = file_path.to_str()
        .context("failed to convert path to string")?;
    let result = Command::new("git")
        .arg("log")
        .arg("-1")
        .arg("--format=%ad")
        .arg("--date=short")
        .arg(path_str)
        .output();
    let output = result
        .with_context(|| format!("could not get last updated date for {:?}", file_path))?;
    if !output.status.success() {
        let error_message = std::str::from_utf8(&output.stderr)
            .unwrap_or("Unknown error")
            .to_string();

        bail!("Git command failed with error: {}", error_message);
    }
    let date_str = str::from_utf8(&output.stdout)
        .with_context(|| format!("could not read output of git command"))?;
    
    Ok(date_str.trim().to_string())
}

pub fn run(source: PathBuf, output: PathBuf) -> Result<()> {
    ensure!(!output.exists(), "output DB already exists at {:?}", output);

    // setup sqlite DB
    let conn = Connection::open(output.as_path())
        .with_context(|| format!("could not create sqlite DB at {:?}", output))?;

    repository::setup_database(&conn)?;

    // process person
    let data_dir = source.join("person");
    let paths = data_dir
        .read_dir()
        .with_context(|| format!("could not open person directory {:?}", data_dir))?;

    for path in paths {
        let file_entry =
            path.with_context(|| format!("could not read person data directory {:?}", data_dir))?;
        let file_path = file_entry.path();
        let file_stem = file_path
            .file_stem()
            .with_context(|| format!("invalid file name {:?} in person directory", file_path))?;
        let id = file_stem.to_str().context(format!(
            "could not convert filename {:?} to string",
            file_stem
        ))?;
        
        let updated = get_updated(file_entry.path().as_path())
            .with_context(|| format!("could not get last updated date for {:?}", file_entry.path()))?;

        let person: data::Person =
            from_toml_file(file_entry.path()).with_context(|| format!("could not load person"))?;
        repository::save_person(&conn, id, &person, &updated)?;
    }

    // process office
    let data_dir = source.join("office");
    let paths = data_dir
        .read_dir()
        .with_context(|| format!("could not open office directory {:?}", data_dir))?;

    for path in paths {
        let file_entry =
            path.with_context(|| format!("could not read office data directory {:?}", data_dir))?;
        let file_path = file_entry.path();
        let file_stem = file_path
            .file_stem()
            .with_context(|| format!("invalid file name {:?} in office directory", file_path))?;
        let id = file_stem.to_str().context(format!(
            "could not convert filename {:?} to string",
            file_stem
        ))?;

        let office: data::Office = from_toml_file(file_entry.path())
            .with_context(|| format!("failed to parse template"))?;
        repository::save_office(&conn, id, &office)?;
    }

    Ok(())
}
