use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use rusqlite::Transaction;
use std::path::Path;

use crate::SchemaSql;
use crate::data::Data;
use crate::record::{Key, OfficePath, PersonPath, RecordRepo};

use super::data;

pub fn init(output: &Path) -> Result<()> {
    ensure!(!output.exists(), "output DB already exists at {:?}", output);

    // setup sqlite DB
    let conn = rusqlite::Connection::open(output)
        .with_context(|| format!("could not create sqlite DB at {:?}", output))?;

    conn.create_entity_tables()
        .context("could not create entity schema")?;

    conn.create_property_tables()
        .context("could not create property schema")?;

    RecordRepo::new(&conn).init()?;

    Ok(())
}

pub fn run(source: &Path, output: &Path) -> Result<()> {
    let mut conn = rusqlite::Connection::open(output)
        .with_context(|| format!("could not open sqlite DB at {:?}", output))?;

    let mut tx = conn.transaction()?;
    
    let data = Data::open(source)?;
    
    for result in data.offices() {
        let (id, office) = result?;
        insert_office_data(&mut tx, &id, &office)?;
    }

    for result in data.persons() {
        let (id, person) = result?;
        insert_person_data(&mut tx, &id, &person)?;
    }

    RecordRepo::new(&tx).commit()?;

    tx.commit()?;

    Ok(())
}

pub fn insert_person_data(tx: &mut Transaction, id: &str, person: &data::Person) -> Result<()> {
    let repo = RecordRepo::new(tx);
    let person_path = Key::<PersonPath, ()>::new(id);

    repo.working()?.save(person_path.name(), &person.name)?;

    if let Some(photo) = &person.photo {
        repo.working()?.save(person_path.photo(), photo)?;
    }
    // Insert contacts if they exist
    if let Some(contacts) = &person.contacts {
        for (contact_type, value) in contacts {
            repo.working()?
                .save(person_path.contact(contact_type.clone()), value)?;
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
            repo.working()?
                .save(person_path.tenure(&tenure.office_id, start), &end)?;
        }
    }

    Ok(())
}

fn insert_office_data(tx: &mut Transaction, id: &str, office: &data::Office) -> Result<()> {
    let repo = RecordRepo::new(tx);
    let office_path = Key::<OfficePath, ()>::new(id);

    repo.working()?.save(office_path.name(), &office.name)?;

    if let Some(photo) = &office.photo {
        repo.working()?.save(office_path.photo(), photo)?;
    }

    // Insert supervisors if they exist
    if let Some(supervisors) = &office.supervisors {
        for (relation, supervisor_office_id) in supervisors {
            repo.working()?.save(
                office_path.supervisor(relation.clone()),
                supervisor_office_id,
            )?;
        }
    }

    // Insert contacts if they exist
    if let Some(contacts) = &office.contacts {
        for (contact_type, value) in contacts {
            repo.working()?
                .save(office_path.contact(contact_type.clone()), value)?;
        }
    }

    Ok(())
}
