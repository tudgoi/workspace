use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_variant::to_variant_name;

use crate::{data, dto};

pub fn setup_database(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE person (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            photo_url TEXT,
            photo_attribution TEXT,

            contacts_address TEXT,
            contacts_phone TEXT,
            contacts_email TEXT,
            contacts_website TEXT,
            contacts_wikipedia TEXT,
            contacts_x TEXT,
            contacts_youtube TEXT,
            contacts_facebook TEXT,
            contacts_instagram TEXT,

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

pub fn save_person(
    conn: &Connection,
    id: &str,
    person: &data::Person,
    updated: &str,
) -> Result<()> {
    let (photo_url, photo_attribution) = if let Some(photo) = &person.photo {
        (Some(photo.url.as_str()), photo.attribution.as_deref())
    } else {
        (None, None)
    };

    let (
        contacts_address,
        contacts_phone,
        contacts_email,
        contacts_website,
        contacts_wikipedia,
        contacts_x,
        contacts_youtube,
        contacts_facebook,
        contacts_instagram,
    ) = if let Some(contacts) = &person.contacts {
        (
            contacts.address.as_deref(),
            contacts.phone.as_deref(),
            contacts.email.as_deref(),
            contacts.website.as_deref(),
            contacts.wikipedia.as_deref(),
            contacts.x.as_deref(),
            contacts.youtube.as_deref(),
            contacts.facebook.as_deref(),
            contacts.instagram.as_deref(),
        )
    } else {
        // Return a tuple of Nones if contacts is None
        (None, None, None, None, None, None, None, None, None)
    };

    conn.execute(
        "
        INSERT INTO person (
            id, name,
            photo_url, photo_attribution,
            contacts_address, contacts_phone, contacts_email,
            contacts_website, contacts_wikipedia,
            contacts_x, contacts_youtube, contacts_facebook, contacts_instagram,
            updated
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        (
            id, &person.name,
            photo_url, photo_attribution,
            contacts_address, contacts_phone, contacts_email,
            contacts_website, contacts_wikipedia,
            contacts_x, contacts_youtube, contacts_facebook, contacts_instagram,
            updated,
        ),
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

pub fn query_for_all_persons<F>(conn: &Connection, mut process: F) -> Result<()>
where
    F: FnMut(dto::PersonOffice) -> Result<()>,
{
    let mut stmt = conn
        .prepare(
            "
        SELECT
            id, name,
            photo_url, photo_attribution,
            contacts_address, contacts_phone, contacts_email,
            contacts_website, contacts_wikipedia,
            contacts_x, contacts_youtube, contacts_facebook, contacts_instagram,
            updated
        FROM person
        ORDER BY id
    ",
        )
        .with_context(|| "could not create statement for reading person table")?;
    let iter = stmt
        .query_map([], |row| {
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;

            let photo_url: Option<String> = row.get(2)?;
            let photo_attribution: Option<String> = row.get(3)?;

            let contacts_address: Option<String> = row.get(4)?;
            let contacts_phone: Option<String> = row.get(5)?;
            let contacts_email: Option<String> = row.get(6)?;
            let contacts_website: Option<String> = row.get(7)?;
            let contacts_wikipedia: Option<String> = row.get(8)?;
            let contacts_x: Option<String> = row.get(9)?;
            let contacts_youtube: Option<String> = row.get(10)?;
            let contacts_facebook: Option<String> = row.get(11)?;
            let contacts_instagram: Option<String> = row.get(12)?;

            let updated: String = row.get(13)?;

            let photo = if let Some(url) = photo_url {
                Some(data::Photo {
                    url,
                    attribution: photo_attribution,
                })
            } else {
                None
            };

            let contacts = if contacts_address.is_some()
                || contacts_phone.is_some()
                || contacts_email.is_some()
                || contacts_website.is_some()
                || contacts_wikipedia.is_some()
                || contacts_x.is_some()
                || contacts_youtube.is_some()
                || contacts_facebook.is_some()
                || contacts_instagram.is_some()
            {
                Some(data::Contacts {
                    address: contacts_address,
                    phone: contacts_phone,
                    email: contacts_email,
                    website: contacts_website,
                    wikipedia: contacts_wikipedia,
                    x: contacts_x,
                    youtube: contacts_youtube,
                    facebook: contacts_facebook,
                    instagram: contacts_instagram,
                })
            } else {
                None
            };

            Ok(dto::Person {
                id,
                data: data::Person {
                    name,
                    photo,
                    contacts,
                    tenures: None, // Tenures are not queried here
                },
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

fn query_person_by_id(conn: &Connection, person_id: &str) -> Result<Option<data::Person>> {
    let mut stmt = conn.prepare("SELECT name, photo_url, photo_attribution, contacts_address, contacts_phone, contacts_email, contacts_website, contacts_wikipedia, contacts_x, contacts_youtube, contacts_facebook, contacts_instagram FROM person WHERE id = ?1")?;
    let mut rows = stmt.query([person_id])?;

    if let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let photo_url: Option<String> = row.get(1)?;
        let photo_attribution: Option<String> = row.get(2)?;
        let contacts_address: Option<String> = row.get(3)?;
        // ... and so on for all contact fields

        let photo = photo_url.map(|url| data::Photo { url, attribution: photo_attribution });
        let contacts = Some(data::Contacts {
            address: contacts_address,
            phone: row.get(4)?,
            email: row.get(5)?,
            website: row.get(6)?,
            wikipedia: row.get(7)?,
            x: row.get(8)?,
            youtube: row.get(9)?,
            facebook: row.get(10)?,
            instagram: row.get(11)?,
        }).filter(|c| c.address.is_some() || c.phone.is_some() || c.email.is_some() || c.website.is_some() || c.wikipedia.is_some() || c.x.is_some() || c.youtube.is_some() || c.facebook.is_some() || c.instagram.is_some());

        Ok(Some(data::Person {
            name,
            photo,
            contacts,
            tenures: None,
        }))
    } else {
        Ok(None)
    }
}

pub fn query_incumbent(conn: &Connection, office_id: &str) -> Result<dto::Officer> {
    let mut stmt = conn.prepare(
        "SELECT o.data, i.person_id
         FROM office AS o
         LEFT JOIN incumbent AS i ON o.id = i.office_id
         WHERE o.id = ?1",
    )?;
    let (office_data, person_id): (String, Option<String>) =
        stmt.query_row([office_id], |row| Ok((row.get(0)?, row.get(1)?)))?;

    let office: data::Office = serde_json::from_str(&office_data)?;

    let (person_id, person_name) = if let Some(pid) = person_id {
        let person_data = query_person_by_id(conn, &pid)?
            .with_context(|| format!("Incumbent person '{}' not found", pid))?;
        (Some(pid), Some(person_data.name))
    } else {
        (None, None)
    };

    Ok(dto::Officer {
        office_id: office_id.to_string(),
        office_name: office.name,
        person_id,
        person_name,
    })
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

pub fn query_subordinates(
    conn: &Connection,
    office_id: &str,
    relation: &str,
) -> Result<Vec<dto::Officer>> {
    let mut stmt = conn.prepare(
        "
        SELECT s.office_id, o.data, i.person_id, p.name
        FROM supervisor AS s
        INNER JOIN office AS o ON o.id = s.office_id
        LEFT JOIN incumbent AS i ON i.office_id = s.office_id
        LEFT JOIN person as p on p.id = i.person_id
        WHERE s.supervisor_office_id = ?1 AND s.relation = ?2
        ORDER BY s.office_id
    ",
    )?;
    let iter = stmt
        .query_map([office_id, relation], |row| {
            let subordinate_office_id: String = row.get(0)?;
            let office_data: String = row.get(1)?;
            let person_id: Option<String> = row.get(2)?;
            let person_name: Option<String> = row.get(3)?;

            let office: data::Office = serde_json::from_str(&office_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;

            Ok(dto::Officer {
                office_id: subordinate_office_id,
                office_name: office.name,
                person_id,
                person_name,
            })
        })
        .with_context(|| "could not query supervisor")?;

    let mut dtos = Vec::new();
    for result in iter {
        dtos.push(result?);
    }
    Ok(dtos)
}
