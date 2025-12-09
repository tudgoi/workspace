use anyhow::bail;
use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use rusqlite::{Transaction};
use std::path::Path;
use std::process::Command;

use crate::SchemaSql;
use crate::{LibrarySql, dto};

use super::data;
use super::from_toml_file;

fn get_commit_date(file_path: &Path) -> Result<Option<NaiveDate>> {
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
        Ok(Some(NaiveDate::parse_from_str(
            date_str.trim(),
            "%Y-%m-%d",
        )?))
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

    let mut tx = conn.transaction()?;

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
        insert_office_data(&mut tx, id, &office, commit_date.as_ref())?;
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
        insert_person_data(&mut tx, id, &person, commit_date.as_ref())?;
    }

    tx.enable_commit_tracking()
        .with_context(|| format!("could not enable commit tracking"))?;

    tx.commit()?;

    Ok(())
}

pub fn insert_person_data(
    tx: &mut Transaction,
    id: &str,
    person: &data::Person,
    commit_date: Option<&NaiveDate>,
) -> Result<()> {
    tx.new_entity(&dto::EntityType::Person, id, &person.name)?;

    if let Some(data::Photo { url, attribution }) = &person.photo {
        tx.save_entity_photo(
            &dto::EntityType::Person,
            id,
            url,
            attribution.as_ref().map(String::as_str),
        )?;
    }
    // Insert contacts if they exist
    if let Some(contacts) = &person.contacts {
        for (contact_type, value) in contacts {
            tx.save_entity_contact(&dto::EntityType::Person, id, contact_type, value)?;
        }
    }

    // Insert tenures if they exist
    if let Some(tenures) = &person.tenures {
        for tenure in tenures {
            tx.save_tenure(
                id,
                &tenure.office_id,
                tenure
                    .start
                    .as_ref()
                    .map(|d| NaiveDate::parse_from_str(&d, "%Y-%m-%d"))
                    .transpose()?
                    .as_ref(),
                tenure
                    .end
                    .as_ref()
                    .map(|d| NaiveDate::parse_from_str(&d, "%Y-%m-%d"))
                    .transpose()?
                    .as_ref(),
            )?;
        }
    }

    if let Some(date) = commit_date {
        tx.save_entity_commit(&dto::EntityType::Person, id, date)?;
    }

    Ok(())
}

fn insert_office_data(
    tx: &mut Transaction,
    id: &str,
    office: &data::Office,
    commit_date: Option<&NaiveDate>,
) -> Result<()> {
    tx.new_entity(&dto::EntityType::Office, id, &office.name)?;

    if let Some(data::Photo { url, attribution }) = &office.photo {
        tx.save_entity_photo(
            &dto::EntityType::Office,
            id,
            url,
            attribution.as_ref().map(String::as_str),
        )?;
    }

    // Insert supervisors if they exist
    if let Some(supervisors) = &office.supervisors {
        for (relation, supervisor_office_id) in supervisors {
            tx.save_office_supervisor(id, relation, supervisor_office_id)?;
        }
    }

    // Insert contacts if they exist
    if let Some(contacts) = &office.contacts {
        for (contact_type, value) in contacts {
            tx.save_entity_contact(&dto::EntityType::Office, id, contact_type, value)
                .with_context(|| {
                    format!(
                        "could not insert contact {}:{} for office id: {}",
                        contact_type, value, id
                    )
                })?;
        }
    }

    if let Some(date) = commit_date {
        tx.save_entity_commit(&dto::EntityType::Office, id, date)?;
    }

    Ok(())
}
