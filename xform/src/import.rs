use anyhow::bail;
use anyhow::{Context, Result, ensure};
use std::path::Path;
use std::process::Command;

use crate::SchemaSql;
use crate::LibrarySql;

use super::from_toml_file;
use super::{data, repo};

fn get_commit_date(file_path: &Path) -> Result<Option<String>> {
    let path_str = file_path
        .to_str()
        .context("failed to convert path to string")?;

    // First, check for local or staged changes.
    let status_output = Command::new("git")
        .arg("status")
        .arg("--porcelain")
        .arg(path_str)
        .output()
        .with_context(|| format!("could not get git status for {:?}", file_path))?;

    if !status_output.status.success() {
        let error_message = std::str::from_utf8(&status_output.stderr)
            .unwrap_or("Unknown git status error")
            .to_string();
        bail!("Git status command failed with error: {}", error_message);
    }

    // If there is any output, it means there are uncommitted changes.
    if !status_output.stdout.is_empty() {
        return Ok(None);
    }

    let result = Command::new("git")
        .arg("log")
        .arg("-1")
        .arg("--format=%ad")
        .arg("--date=short")
        .arg(path_str)
        .output();
    let output =
        result.with_context(|| format!("could not get last commit date for {:?}", file_path))?;
    if !output.status.success() {
        let error_message = std::str::from_utf8(&output.stderr)
            .unwrap_or("Unknown error")
            .to_string();

        bail!("Git command failed with error: {}", error_message);
    }
    let date_str = str::from_utf8(&output.stdout)
        .with_context(|| format!("could not read output of git command"))?
        .trim();

    if date_str.is_empty() {
        Ok(None)
    } else {
        Ok(Some(date_str.trim().to_string()))
    }
}

pub fn run(source: &Path, output: &Path) -> Result<()> {
    ensure!(!output.exists(), "output DB already exists at {:?}", output);

    // setup sqlite DB
    let mut conn = rusqlite::Connection::open(output)
        .with_context(|| format!("could not create sqlite DB at {:?}", output))?;

    conn.create_entity_tables()
        .with_context(|| format!("could not create entity schema"))?;

    conn.create_property_tables()
        .with_context(|| format!("could not create property schema"))?;

    let mut repo = repo::Repository::new(&mut conn)
        .with_context(|| format!("could not create sqlite DB at {:?}", output))?;

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

        let commit_date = get_commit_date(file_entry.path().as_path()).with_context(|| {
            format!("could not get last commit date for {:?}", file_entry.path())
        })?;

        let office: data::Office =
            from_toml_file(file_entry.path()).with_context(|| format!("could not load office"))?;
        repo.insert_office_data(id, &office, commit_date.as_deref())?;
    }

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

        let commit_date = get_commit_date(file_entry.path().as_path()).with_context(|| {
            format!("could not get last commit date for {:?}", file_entry.path())
        })?;

        let person: data::Person =
            from_toml_file(file_entry.path()).with_context(|| format!("could not load person"))?;
        repo.insert_person_data(id, &person, commit_date.as_deref())?;
    }

    repo.conn.enable_commit_tracking()
        .with_context(|| format!("could not enable commit tracking"))?;

    Ok(())
}
