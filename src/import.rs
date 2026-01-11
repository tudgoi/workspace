use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use rusqlite::Transaction;
use std::path::Path;

use crate::SchemaSql;
use crate::record::{Key, OfficePath, PersonPath, RecordRepo};

use super::data;
use super::from_toml_file;

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

        let office: data::Office =
            from_toml_file(file_entry.path()).context("could not load office")?;
        insert_office_data(&mut tx, id, &office)?;
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

        let person: data::Person =
            from_toml_file(file_entry.path()).context("could not load person")?;
        insert_person_data(&mut tx, id, &person)?;
    }

    RecordRepo::new(&tx).commit()?;

    tx.commit()?;

    Ok(())
}

pub fn insert_person_data(tx: &mut Transaction, id: &str, person: &data::Person) -> Result<()> {
    let mut repo = RecordRepo::new(tx);
    let person_path = Key::<PersonPath, ()>::new(id);

    repo.root()?.save(person_path.name(), &person.name)?;

    if let Some(photo) = &person.photo {
        repo.root()?.save(person_path.photo(), photo)?;
    }
    // Insert contacts if they exist
    if let Some(contacts) = &person.contacts {
        for (contact_type, value) in contacts {
            repo.root()?.save(person_path.contact(contact_type.clone()), value)?;
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
            repo.root()?.save(person_path.tenure(&tenure.office_id, start), &end)?;
        }
    }

    Ok(())
}

fn insert_office_data(tx: &mut Transaction, id: &str, office: &data::Office) -> Result<()> {
    let mut repo = RecordRepo::new(tx);
    let office_path = Key::<OfficePath, ()>::new(id);

    repo.root()?.save(office_path.name(), &office.name)?;

    if let Some(photo) = &office.photo {
        repo.root()?.save(office_path.photo(), photo)?;
    }

    // Insert supervisors if they exist
    if let Some(supervisors) = &office.supervisors {
        for (relation, supervisor_office_id) in supervisors {
            repo.root()?.save(
                office_path.supervisor(relation.clone()),
                supervisor_office_id,
            )?;
        }
    }

    // Insert contacts if they exist
    if let Some(contacts) = &office.contacts {
        for (contact_type, value) in contacts {
            repo.root()?.save(office_path.contact(contact_type.clone()), value)?;
        }
    }

    Ok(())
}
