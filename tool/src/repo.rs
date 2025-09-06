use std::{collections::HashMap, path::Path};

use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use serde_variant::to_variant_name;

use crate::{context, data::{self}, dto};

pub struct Repository {
    conn: Connection,
    all_supervisor_variants: HashMap<String, data::SupervisingRelation>,
}

impl Repository {
    pub fn new(db_path: &Path) -> Result<Repository> {
        const ALL_VARIANTS: [data::SupervisingRelation; 5] = [
            data::SupervisingRelation::Adviser,
            data::SupervisingRelation::DuringThePleasureOf,
            data::SupervisingRelation::Head,
            data::SupervisingRelation::ResponsibleTo,
            data::SupervisingRelation::MemberOf,
        ];
        let mut map: HashMap<String, data::SupervisingRelation> = HashMap::new();
        for variant in ALL_VARIANTS {
            map.insert(to_variant_name(&variant)?.to_string(), variant);
        }

        let conn = Connection::open(db_path)?;
        
        Ok(Repository { conn, all_supervisor_variants: map, })
    }

    pub fn setup_database(&self) -> Result<()> {
        self.conn.execute(
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

        self.conn.execute(
            "CREATE TABLE office (
                id    TEXT PRIMARY KEY,
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
                contacts_instagram TEXT
            )",
            (),
        )
        .with_context(|| "could not create `office` table")?;

        self.conn.execute(
            "CREATE TABLE supervisor (
                office_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                supervisor_office_id TEXT NOT NULL
            )",
            (),
        )
        .with_context(|| "could not create `supervisor` table")?;

        self.conn.execute(
            "CREATE TABLE tenure (
                person_id TEXT NOT NULL,
                office_id TEXT NOT NULL,
                start TEXT,
                end TEXT
            )",
            (),
        )
        .with_context(|| "could not create `tenure` table")?;

        self.conn.execute(
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
        &self,
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

        self.conn.execute(
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
                id,
                &person.name,
                photo_url,
                photo_attribution,
                contacts_address,
                contacts_phone,
                contacts_email,
                contacts_website,
                contacts_wikipedia,
                contacts_x,
                contacts_youtube,
                contacts_facebook,
                contacts_instagram,
                updated,
            ),
        )
        .with_context(|| format!("could not insert person {} into DB", id))?;

        if let Some(tenures) = &person.tenures {
            for tenure in tenures {
                self.conn.execute(
                    "INSERT INTO tenure (person_id, office_id, start, end) VALUES (?1, ?2, ?3, ?4)",
                    (id, &tenure.office, &tenure.start, &tenure.end),
                )
                .with_context(|| format!("could not insert tenure into DB for {}", id))?;
            }
        }
        Ok(())
    }

    pub fn query_for_all_persons<F>(&self, mut process: F) -> Result<()>
    where
    F: FnMut(dto::PersonOffice) -> Result<()>,
    {
        let mut stmt = self.conn
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
                let photo = if let Some(url) = row.get(2)? {
                    Some(data::Photo {
                        url,
                        attribution: row.get(3)?,
                    })
                } else {
                    None
                };
                let contacts = Some(data::Contacts {
                    address: row.get(4)?,
                    phone: row.get(5)?,
                    email: row.get(6)?,
                    website: row.get(7)?,
                    wikipedia: row.get(8)?,
                    x: row.get(9)?,
                    youtube: row.get(10)?,
                    facebook: row.get(11)?,
                    instagram: row.get(12)?,
                })
                .filter(|c| {
                    c.address.is_some()
                        || c.phone.is_some()
                        || c.email.is_some()
                        || c.website.is_some()
                        || c.wikipedia.is_some()
                        || c.x.is_some()
                        || c.youtube.is_some()
                        || c.facebook.is_some()
                        || c.instagram.is_some()
                });

                Ok((
                    context::Person {
                        id: row.get(0)?,
                        name: row.get(1)?,
                    },
                    photo,
                    contacts,
                    row.get(13)?,
                ))
            })
            .with_context(|| "querying person table failed")?;

        for result in iter {
            let (person, photo, contacts, updated) = result?;
            let person_id = person.id.clone();
            let offices = self.query_offices_for_person(&person.id)
                .with_context(|| format!("could not query offices for {}", person.id))?;

            let person_office = dto::PersonOffice {
                person,
                offices: if offices.is_empty() {
                    None
                } else {
                    Some(offices)
                },
                updated,
                photo,
                contacts,
            };
            process(person_office)
                .with_context(|| format!("could not process person `{}`", person_id))?;
        }

        Ok(())
    }

    pub fn save_office(&self, id: &str, office: &data::Office) -> Result<()> {
        let (photo_url, photo_attribution) = if let Some(photo) = &office.photo {
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
        ) = if let Some(contacts) = &office.contacts {
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
            (None, None, None, None, None, None, None, None, None)
        };

        self.conn.execute(
            "
            INSERT INTO office (
                id, name,
                photo_url, photo_attribution,

                contacts_address,contacts_phone, contacts_email,
                contacts_website, contacts_wikipedia,
                contacts_x, contacts_youtube, contacts_facebook, contacts_instagram
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
        ",
            (
                id,
                &office.name,
                photo_url,
                photo_attribution,
                contacts_address,
                contacts_phone,
                contacts_email,
                contacts_website,
                contacts_wikipedia,
                contacts_x,
                contacts_youtube,
                contacts_facebook,
                contacts_instagram,
            ),
        )
        .with_context(|| format!("could not insert office {} into DB", id))?;

        if let Some(supervisors) = &office.supervisors {
            for (name, value) in supervisors.iter() {
                self.conn.execute(
                    "INSERT INTO supervisor (office_id, relation, supervisor_office_id) VALUES (?1, ?2, ?3)",
                    params![id, to_variant_name(name)?, value],
                ).with_context(|| "could not insert supervisor into DB")?;
            }
        }
        Ok(())
    }

    pub fn query_counts(&self) -> Result<dto::Counts> {
        Ok(dto::Counts {
            persons: self.conn.query_row("SELECT COUNT(*) FROM person", [], |row| row.get(0))?,
            offices: self.conn.query_row("SELECT COUNT(*) FROM office", [], |row| row.get(0))?,
        })
    }

    pub fn query_offices_for_person(&self, person_id: &str) -> Result<Vec<dto::Office>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT
                o.id,
                o.name,
                o.photo_url,
                o.photo_attribution,
                o.contacts_address,
                o.contacts_phone,
                o.contacts_email,
                o.contacts_website,
                o.contacts_wikipedia,
                o.contacts_x,
                o.contacts_youtube,
                o.contacts_facebook,
                o.contacts_instagram
            FROM incumbent AS i
            INNER JOIN office AS o ON o.id=i.office_id
            WHERE i.person_id=?1
            ORDER BY o.id
        ",
        )?;
        let iter = stmt
            .query_map([person_id], |row| {
                let photo = if let Some(url) = row.get(2)? {
                    Some(data::Photo { url, attribution: row.get(3)? })
                } else {
                    None
                };
                let contacts = Some(data::Contacts {
                    address: row.get(4)?,
                    phone: row.get(5)?,
                    email: row.get(6)?,
                    website: row.get(7)?,
                    wikipedia: row.get(8)?,
                    x: row.get(9)?,
                    youtube: row.get(10)?,
                    facebook: row.get(11)?,
                    instagram: row.get(12)?,
                })
                .filter(|c| c.address.is_some() || c.phone.is_some() || c.email.is_some() || c.website.is_some() || c.wikipedia.is_some() || c.x.is_some() || c.youtube.is_some() || c.facebook.is_some() || c.instagram.is_some());
                let office_id: String = row.get(0)?;

                Ok(dto::Office {
                    id: office_id,
                    name: row.get(1)?,
                    photo,
                    contacts,
                })
            })?;
        let mut offices = Vec::new();
        for result in iter {
            offices.push(result?);
        }

        Ok(offices)
    }

    pub fn query_subordinates_for_office(
        &self,
        office_id: &str,
    ) -> Result<HashMap<data::SupervisingRelation, Vec<context::Officer>>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT s.relation, s.office_id, o.name, i.person_id, p.name
            FROM supervisor AS s
            INNER JOIN office AS o ON o.id = s.office_id
            LEFT JOIN incumbent AS i ON i.office_id = s.office_id
            LEFT JOIN person as p on p.id = i.person_id
            WHERE s.supervisor_office_id = ?1
            ORDER BY s.office_id
        ",
        )?;
        let iter = stmt
            .query_map([office_id], |row| {
                let relation: String = row.get(0)?;
                let subordinate_office_id: String = row.get(1)?;
                let office_name: String = row.get(2)?;
                
                let person = if let (Some(id), Some(name)) = (row.get(3)?, row.get(4)?) {
                    Some(context::Person { id, name })
                } else {
                    None
                };

                Ok((relation, context::Officer {
                    office_id: subordinate_office_id,
                    office_name,
                    person,
                }))
            })?;

        let mut dtos = HashMap::new();
        for result in iter {
            let (relation, officer) = result?;
            let relation = self.all_supervisor_variants.get(&relation)
                .context(format!("could not understand supervisor relation `{}`", relation))?
                .clone();
            dtos.entry(relation)
                .or_insert_with(Vec::new)
                .push(officer);
        }
        Ok(dtos)
    }

    pub fn query_supervisors_for_office(
        &self,
        office_id: &str,
    ) -> Result<HashMap<data::SupervisingRelation, context::Officer>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT
                s.relation,
                s.supervisor_office_id,
                o.name,
                i.person_id,
                p.name
            FROM supervisor AS s
            INNER JOIN office AS o ON o.id = s.supervisor_office_id
            LEFT JOIN incumbent AS i ON i.office_id = s.supervisor_office_id
            LEFT JOIN person as p on p.id = i.person_id
            WHERE s.office_id = ?1
        ",
        )?;
        let iter = stmt.query_map([office_id], |row| {
            let relation_str: String = row.get(0)?;
            let person = if let (Some(id), Some(name)) = (row.get(3)?, row.get(4)?) {
                Some(context::Person { id, name })
            } else {
                None
            };
            Ok((
                relation_str,
                context::Officer {
                    office_id: row.get(1)?,
                    office_name: row.get(2)?,
                    person,
                },
            ))
        })?;

        let mut supervisors = std::collections::HashMap::new();
        for result in iter {
            let (relation, officer) = result?;
            let relation= self.all_supervisor_variants.get(&relation)
                .with_context(|| format!("unknown relation {}", relation))?
                .clone();
            supervisors.insert(relation, officer);
        }

        Ok(supervisors)
    }

}
