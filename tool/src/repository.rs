use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_variant::to_variant_name;

use crate::{data, dto};

pub fn setup_database(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE person (
            id    TEXT PRIMARY KEY,
            data  TEXT NOT NULL,
            updated TEXT NOT NULL
        )",
        (),
    )
    .with_context(|| "could not create `person` table")?;

    conn.execute(
        "CREATE TABLE office (
            id    TEXT PRIMARY KEY,
            data  TEXT NOT NULL
        )",
        (),
    )
    .with_context(|| "could not create `office` table")?;

    conn.execute(
        "CREATE TABLE supervisor (
            office_id TEXT NOT NULL,
            relation TEXT NOT NULL,
            supervisor_office_id TEXT NOT NULL
        )",
        (),
    )
    .with_context(|| "could not create `supervisor` table")?;

    conn.execute(
        "CREATE TABLE tenure (
            person_id TEXT NOT NULL,
            office_id TEXT NOT NULL,
            start TEXT,
            end TEXT
        )",
        (),
    )
    .with_context(|| "could not create `tenure` table")?;

    conn.execute(
        "
        CREATE VIEW incumbent (
            office_id,
            person_id
        ) AS SELECT office_id, person_id
        FROM tenure
        WHERE end IS NULL",
        (),
    )
    .with_context(|| "could not create view `incumbent`")?;

    Ok(())
}

pub fn save_person(conn: &Connection, id: &str, person: &data::Person, updated: &str) -> Result<()> {
    let json =
        serde_json::to_string(person).with_context(|| "could not convert person to JSON")?;
    conn.execute(
        "INSERT INTO person (id, data, updated) VALUES (?1, ?2, ?3)",
        (id, json, updated),
    )
    .with_context(|| format!("could not insert person {} into DB", id))?;

    if let Some(tenures) = &person.tenures {
        for tenure in tenures {
            conn.execute(
                "INSERT INTO tenure (person_id, office_id, start, end) VALUES (?1, ?2, ?3, ?4)",
                (id, &tenure.office, &tenure.start, &tenure.end),
            )
            .with_context(|| format!("could not insert tenure into DB for {}", id))?;
        }
    }
    Ok(())
}

pub fn save_office(conn: &Connection, id: &str, office: &data::Office) -> Result<()> {
    let json = serde_json::to_string(office).with_context(|| "could not convert office to JSON")?;
    conn.execute("INSERT INTO office (id, data) VALUES (?1, ?2)", (id, json))
        .with_context(|| format!("could not insert office {} into DB", id))?;

    if let Some(supervisors) = &office.supervisors {
        for (name, value) in supervisors.iter() {
            conn.execute(
                "INSERT INTO supervisor (office_id, relation, supervisor_office_id) VALUES (?1, ?2, ?3)",
                (id, to_variant_name(name)?, value),
            ).with_context(|| "could not insert supervisor into DB")?;
        }
    }
    Ok(())
}

pub fn query_counts(conn: &Connection) -> Result<dto::Counts> {
    Ok(dto::Counts {
        persons: conn.query_row("SELECT COUNT(*) FROM person", [], |row| row.get(0))?,
        offices: conn.query_row("SELECT COUNT(*) FROM office", [], |row| row.get(0))?,
    })
}

pub fn query_offices_for_person(conn: &Connection, person_id: &str) -> Result<Vec<dto::Office>> {
    let mut stmt = conn.prepare(
        "
        SELECT o.id, o.data
        FROM incumbent AS i
        INNER JOIN office AS o ON o.id=i.office_id
        WHERE i.person_id=?1
        ORDER BY o.id
    ",
    )?;
    let iter = stmt.query_map([person_id], |row| {
        let id: String = row.get(0)?;
        let data: String = row.get(1)?;
        Ok((id, data))
    })?;
    let mut offices = Vec::new();
    for result in iter {
        let (id, data_str) = result?;
        let office: data::Office = serde_json::from_str(&data_str)?;
        offices.push(dto::Office { id, data: office });
    }

    Ok(offices)
}

pub fn query_for_all_persons<F>(conn: &Connection, mut process: F) -> Result<()>
where
    F: FnMut(dto::PersonOffice) -> Result<()>,
{
    let mut stmt = conn
        .prepare(
            "
        SELECT p.id, p.data, p.updated
        FROM person AS p
        ORDER BY p.id
    ",
        )
        .with_context(|| "could not create statement for reading person table")?;
    let iter = stmt
        .query_map([], |row| {
            let person_id: String = row.get(0)?;
            let person_data: String = row.get(1)?;
            let updated: String = row.get(2)?;

            let person: data::Person = serde_json::from_str(&person_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
            Ok(dto::Person {
                id: person_id,
                data: person,
                updated,
            })
        })
        .with_context(|| "querying person table failed")?;

    for result in iter {
        let person = result?;
        let person_id = person.id.clone();
        let offices = query_offices_for_person(conn, &person.id)?;

        let person_office = dto::PersonOffice {
            person,
            offices: if offices.is_empty() {
                None
            } else {
                Some(offices)
            },
        };
        process(person_office)
            .with_context(|| format!("could not process person `{}`", person_id))?;
    }

    Ok(())
}

pub fn query_incumbent(conn: &Connection, office_id: &str) -> Result<dto::Officer> {
    let mut stmt = conn
        .prepare(
            "
        SELECT o.id, o.data, p.id, p.data, p.updated
        FROM office AS o
        LEFT JOIN incumbent AS i ON i.office_id = o.id
        LEFT JOIN person AS p ON p.id = i.person_id
        WHERE o.id = ?1
    ",
        )
        .with_context(|| format!("could not query incumbent for {:?}", office_id))?;
    let mut iter = stmt.query_map([office_id], |row| {
        let office_id: String = row.get(0)?;
        let office_data: String = row.get(1)?;
        let person_id: Option<String> = row.get(2)?;
        let person_data: Option<String> = row.get(3)?;
        let person_updated: Option<String> = row.get(4)?;

        let office: data::Office = serde_json::from_str(&office_data)
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e)))?;
        let person = if let (Some(person_id), Some(person_data), Some(person_updated)) =
            (person_id, person_data, person_updated)
        {
            let person = serde_json::from_str(&person_data)
                .map_err(|e| rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e)))?;
            Some(dto::Person {
                id: person_id,
                data: person,
                updated: person_updated,
            })
        } else {
            None
        };

        Ok(dto::Officer {
            office: dto::Office {
                id: office_id,
                data: office,
            },
            person,
        })
    })?;

    let dto = iter
        .next()
        .context(format!("could not query person for office {:?}", office_id))?
        .with_context(|| "could not read incumbent from DB")?;

    Ok(dto)
}

pub fn query_subordinates(
    conn: &Connection,
    office_id: &str,
    relation: &str,
) -> Result<Vec<dto::Officer>> {
    let mut stmt = conn.prepare(
        "
        SELECT o.data, p.id, p.data, p.updated
        FROM supervisor AS s
        LEFT JOIN incumbent AS i ON i.office_id = s.office_id
        INNER JOIN office AS o ON o.id = s.office_id
        LEFT JOIN person AS p ON p.id = i.person_id
        WHERE s.supervisor_office_id = ?1 AND s.relation = ?2
        ORDER BY p.id
    ",
    )?;
    let iter = stmt.query_map([office_id, relation], |row| {
        let office_data: String = row.get(0)?;
        let person_id: Option<String> = row.get(1)?;
        let person_data: Option<String> = row.get(2)?;
        let person_updated: Option<String> = row.get(3)?;

        let office: data::Office = serde_json::from_str(&office_data)
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e)))?;
        let person = if let (Some(person_id), Some(person_data), Some(person_updated)) = (person_id, person_data, person_updated) {
            let person = serde_json::from_str(&person_data)
                .map_err(|e| rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e)))?;

            Some(dto::Person { id: person_id, data: person, updated: person_updated })
        } else {
            None
        };

        Ok(dto::Officer {
            office: dto::Office { id: office_id.to_string(), data: office },
            person,
        })
    })
    .with_context(|| "could not query supervisor")?;

    let mut dtos = Vec::new();
    for result in iter {
        dtos.push(result?);
    }
    Ok(dtos)
}