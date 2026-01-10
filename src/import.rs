use anyhow::bail;
use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use rusqlite::Transaction;
use std::path::Path;
use std::process::Command;

use crate::SchemaSql;
use crate::record::{Key, OfficePath, PersonPath, RecordRepo};
use crate::{LibrarySql, dto};

use super::data;
use super::from_toml_file;

fn get_commit_date(repo_path: &Path, file_path: &Path) -> Result<Option<NaiveDate>> {
    let path_str = file_path
        .strip_prefix(repo_path)?
        .to_str()
        .context("failed to convert path to string")?;

    // First, check for local or staged changes.
    let status_output = Command::new("git")
        .current_dir(repo_path)
        .arg("status")
        .arg("--porcelain")
        .arg("--")
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
        .arg("--")
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
        .context("could not read output of git command")?
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
        .context("could not create entity schema")?;

    conn.create_property_tables()
        .context("could not create property schema")?;

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

        let commit_date =
            get_commit_date(source, file_entry.path().as_path()).with_context(|| {
                format!("could not get last commit date for {:?}", file_entry.path())
            })?;

        let office: data::Office =
            from_toml_file(file_entry.path()).context("could not load office")?;
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

        let commit_date =
            get_commit_date(source, file_entry.path().as_path()).with_context(|| {
                format!("could not get last commit date for {:?}", file_entry.path())
            })?;

        let person: data::Person =
            from_toml_file(file_entry.path()).context("could not load person")?;
        insert_person_data(&mut tx, id, &person, commit_date.as_ref())?;
    }

    tx.enable_commit_tracking()
        .context("could not enable commit tracking")?;

    tx.commit()?;

    Ok(())
}

pub fn insert_person_data(
    tx: &mut Transaction,
    id: &str,
    person: &data::Person,
    commit_date: Option<&NaiveDate>,
) -> Result<()> {
    let mut repo = RecordRepo::new(tx);
    let person_path = Key::<PersonPath, ()>::new(id);

    repo.save(person_path.name(), &person.name)?;

    if let Some(photo) = &person.photo {
        repo.save(person_path.photo(), photo)?;
    }
    // Insert contacts if they exist
    if let Some(contacts) = &person.contacts {
        for (contact_type, value) in contacts {
            repo.save(person_path.contact(contact_type.clone()), value)?;
        }
    }

    // Insert tenures if they exist
    if let Some(tenures) = &person.tenures {
        for tenure in tenures {
            let start = tenure
                .start
                .as_ref()
                .map(|d| d.parse::<NaiveDate>())
                .transpose()?;
            let end = tenure
                .end
                .as_ref()
                .map(|d| d.parse::<NaiveDate>())
                .transpose()?;
            repo.save(person_path.tenure(&tenure.office_id, start), &end)?;
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
    let mut repo = RecordRepo::new(tx);
    let office_path = Key::<OfficePath, ()>::new(id);

    repo.save(office_path.name(), &office.name)?;

    if let Some(photo) = &office.photo {
        repo.save(office_path.photo(), photo)?;
    }

    // Insert supervisors if they exist
    if let Some(supervisors) = &office.supervisors {
        for (relation, supervisor_office_id) in supervisors {
            repo.save(
                office_path.supervisor(relation.clone()),
                supervisor_office_id,
            )?;
        }
    }

    // Insert contacts if they exist
    if let Some(contacts) = &office.contacts {
        for (contact_type, value) in contacts {
            repo.save(office_path.contact(contact_type.clone()), value)?;
        }
    }

    if let Some(date) = commit_date {
        tx.save_entity_commit(&dto::EntityType::Office, id, date)?;
    }

    Ok(())
}
