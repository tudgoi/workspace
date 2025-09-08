use std::{collections::{BTreeMap, HashMap}, path::Path};

use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use serde_variant::to_variant_name;

use crate::{
    context,
    data::{self},
    dto,
};

pub struct Repository {
    conn: Connection,
    all_supervising_relation_variants: HashMap<String, data::SupervisingRelation>,
    all_contact_type_variants: HashMap<String, data::ContactType>,
}

impl Repository {
    pub fn new(db_path: &Path) -> Result<Repository> {
        // get string for querying contact
        let conn = Connection::open(db_path)?;

        Ok(Repository {
            conn,
            all_supervising_relation_variants: Self::build_supervising_relation_variants()
                .with_context(|| format!("could not build SupervisingRelation variants"))?,
            all_contact_type_variants: Self::build_contact_type_variants()
                .with_context(|| format!("could not build ContactType variants"))?,
        })
    }

    fn build_supervising_relation_variants() -> Result<HashMap<String, data::SupervisingRelation>> {
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

        Ok(map)
    }

    fn build_contact_type_variants() -> Result<HashMap<String, data::ContactType>> {
        const ALL_VARIANTS: [data::ContactType; 10] = [
            data::ContactType::Address,
            data::ContactType::Phone,
            data::ContactType::Email,
            data::ContactType::Website,
            data::ContactType::Wikipedia,
            data::ContactType::X,
            data::ContactType::Youtube,
            data::ContactType::Facebook,
            data::ContactType::Instagram,
            data::ContactType::Wikidata,
        ];
        let mut map: HashMap<String, data::ContactType> = HashMap::new();
        for variant in ALL_VARIANTS {
            map.insert(to_variant_name(&variant)?.to_string(), variant);
        }

        Ok(map)
    }

    pub fn setup_database(&self) -> Result<()> {
        self.conn
            .execute(
                "CREATE TABLE person (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                photo_url TEXT,
                photo_attribution TEXT,

                updated TEXT NOT NULL
            )",
                (),
            )
            .with_context(|| "could not create `person` table")?;

        self.conn
            .execute(
                "CREATE TABLE person_contact (
                id TEXT NOT NULL,
                type TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (id, type)
            )",
                (),
            )
            .with_context(|| "could not create `person_contact` table")?;

        self.conn
            .execute(
                "CREATE TABLE office (
                id    TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                
                photo_url TEXT,
                photo_attribution TEXT
            )",
                (),
            )
            .with_context(|| "could not create `office` table")?;

        self.conn
            .execute(
                "CREATE TABLE office_contact (
                id TEXT NOT NULL,
                type TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (id, type)
            )",
                (),
            )
            .with_context(|| "could not create `office_contact` table")?;

        self.conn
            .execute(
                "CREATE TABLE supervisor (
                office_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                supervisor_office_id TEXT NOT NULL
            )",
                (),
            )
            .with_context(|| "could not create `supervisor` table")?;

        self.conn
            .execute(
                "CREATE TABLE tenure (
                person_id TEXT NOT NULL,
                office_id TEXT NOT NULL,
                start TEXT,
                end TEXT
            )",
                (),
            )
            .with_context(|| "could not create `tenure` table")?;

        self.conn
            .execute(
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

    pub fn save_person(&mut self, id: &str, person: &data::Person, updated: &str) -> Result<()> {
        let (photo_url, photo_attribution) = if let Some(photo) = &person.photo {
            (Some(photo.url.as_str()), photo.attribution.as_deref())
        } else {
            (None, None)
        };

        self.conn
            .execute(
                "
            INSERT INTO person (
                id, name,
                photo_url, photo_attribution,
                updated
            ) VALUES (?1, ?2, ?3, ?4, ?5)",
                (id, &person.name, photo_url, photo_attribution, updated),
            )
            .with_context(|| format!("could not insert person {} into DB", id))?;

        if let Some(contacts) = &person.contacts {
            self.save_person_contacts(id, contacts)?;
        }

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

    pub fn save_person_contacts(
        &mut self,
        id: &str,
        contacts: &BTreeMap<data::ContactType, String>,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO person_contact (id, type, value) VALUES (?1, ?2, ?3)",
            )?;
            for (contact_type, value) in contacts {
                stmt.execute(params![id, to_variant_name(&contact_type)?, value])
                    .with_context(|| format!("could not insert contact for person {}", id))?;
            }
        }
        tx.commit()
            .context(format!("failed to insert contacts for person"))
    }

    pub fn save_person_contact(
        &mut self,
        id: &str,
        contact_type: &data::ContactType,
        value: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO person_contact (id, type, value) VALUES (?1, ?2, ?3)",
            params![id, to_variant_name(contact_type)?, value],
        )
        .with_context(|| format!("could not insert contact for person {}", id))?;
        Ok(())
    }
    
    pub fn save_office(&mut self, id: &str, office: &data::Office) -> Result<()> {
        let (photo_url, photo_attribution) = if let Some(photo) = &office.photo {
            (Some(photo.url.as_str()), photo.attribution.as_deref())
        } else {
            (None, None)
        };

        self.conn
            .execute(
                "
            INSERT INTO office (
                id, name,
                photo_url, photo_attribution
            ) VALUES (?1, ?2, ?3, ?4)
        ",
                (id, &office.name, photo_url, photo_attribution),
            )
            .with_context(|| format!("could not insert office {} into DB", id))?;

        if let Some(contacts) = &office.contacts {
            self.save_office_contacts(id, contacts)
                .with_context(|| format!("could not save contacts for office {}", id))?;
        }

        if let Some(supervisors) = &office.supervisors {
            for (name, value) in supervisors {
                self.conn.execute(
                    "INSERT INTO supervisor (office_id, relation, supervisor_office_id) VALUES (?1, ?2, ?3)",
                    params![id, to_variant_name(name)?, value],
                ).with_context(|| "could not insert supervisor into DB")?;
            }
        }
        Ok(())
    }

    pub fn save_office_contacts(
        &mut self,
        id: &str,
        contacts: &BTreeMap<data::ContactType, String>,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO office_contact (id, type, value) VALUES (?1, ?2, ?3)",
            )?;
            for (contact_type, value) in contacts {
                stmt.execute(params![id, to_variant_name(&contact_type)?, value])
                    .with_context(|| format!("could not insert contact for office {}", id))?;
            }
        }
        tx.commit()
            .context(format!("failed to insert contacts for office"))
    }

    pub fn query_counts(&self) -> Result<dto::Counts> {
        Ok(dto::Counts {
            persons: self
                .conn
                .query_row("SELECT COUNT(*) FROM person", [], |row| row.get(0))?,
            offices: self
                .conn
                .query_row("SELECT COUNT(*) FROM office", [], |row| row.get(0))?,
        })
    }

    pub fn query_person_updated_date(&self, id: &str) -> Result<String> {
        self.conn.query_row(
            "SELECT updated FROM person WHERE id = ?1",
            [id],
            |row| row.get(0),
        ).with_context(|| format!("could not query updated date for person {}", id))
    }
    pub fn query_contacts_for_person(
        &self,
        id: &str,
    ) -> Result<BTreeMap<data::ContactType, String>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT type, value
            FROM person_contact
            WHERE id = ?1
        ",
        )?;
        let iter = stmt.query_map([id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut contacts = BTreeMap::new();
        for result in iter {
            let (contact_type, value): (String, String) = result?;
            let contact_type = self
                .all_contact_type_variants
                .get(&contact_type)
                .with_context(|| format!("could not get string for enum {:?}", contact_type))?
                .clone();
            contacts.insert(contact_type, value);
        }

        Ok(contacts)
    }

    pub fn query_contacts_for_office(
        &self,
        id: &str,
    ) -> Result<BTreeMap<data::ContactType, String>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT type, value
            FROM office_contact
            WHERE id = ?1
        ",
        )?;
        let iter = stmt.query_map([id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut contacts = BTreeMap::new();
        for result in iter {
            let (contact_type, value): (String, String) = result?;
            let contact_type = self
                .all_contact_type_variants
                .get(&contact_type)
                .with_context(|| format!("could not get string for enum {:?}", contact_type))?
                .clone();
            contacts.insert(contact_type, value);
        }

        Ok(contacts)
    }

    pub fn query_offices_for_person(&self, person_id: &str) -> Result<Vec<dto::Office>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT
                o.id,
                o.name,
                o.photo_url,
                o.photo_attribution
            FROM incumbent AS i
            INNER JOIN office AS o ON o.id=i.office_id
            WHERE i.person_id=?1
            ORDER BY o.id
        ",
        )?;
        let iter = stmt.query_map([person_id], |row| {
            let photo = if let Some(url) = row.get(2)? {
                Some(data::Photo {
                    url,
                    attribution: row.get(3)?,
                })
            } else {
                None
            };
            let office_id: String = row.get(0)?;

            Ok((
                office_id,
                row.get(1)?,
                photo,
            ))
        })?;
        let mut offices = Vec::new();
        for result in iter {
            let (id, name, photo) = result?;
            let contacts = self.query_contacts_for_office(&id)?;

            offices.push(dto::Office {
                id, name, photo,
                contacts: if contacts.is_empty() { None } else { Some(contacts) }
            });
        }

        Ok(offices)
    }

    pub fn query_tenures_for_person(&self, person_id: &str) -> Result<Vec<data::Tenure>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT office_id, start, end
            FROM tenure
            WHERE person_id = ?1
            ORDER BY start DESC
        ",
        )?;
        let iter = stmt.query_map([person_id], |row| {
            Ok(data::Tenure {
                office: row.get(0)?,
                start: row.get(1)?,
                end: row.get(2)?,
                additional_charge: None, // This info is not stored in the DB
            })
        })?;

        let mut tenures = Vec::new();
        for result in iter {
            tenures.push(result?);
        }
        Ok(tenures)
    }

    pub fn query_all_persons(&self) -> Result<HashMap<String, data::Person>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT id, name, photo_url, photo_attribution
            FROM person
            ORDER BY id
        ",
        )?;

        let iter = stmt.query_map([], |row| {
            let photo = if let Some(url) = row.get(2)? {
                Some(data::Photo {
                    url,
                    attribution: row.get(3)?,
                })
            } else {
                None
            };
            Ok((row.get(0)?, row.get(1)?, photo))
        })?;

        let mut persons = HashMap::new();
        for result in iter {
            let (id, name, photo): (String, String, Option<data::Photo>) = result?;
            let contacts = self.query_contacts_for_person(&id)?;
            let tenures = self.query_tenures_for_person(&id)?;

            let person_data = data::Person {
                name,
                photo,
                contacts: Some(contacts).filter(|c| !c.is_empty()),
                tenures: Some(tenures).filter(|t| !t.is_empty()),
            };

            persons.insert(id, person_data);
        }

        Ok(persons)
    }

    pub fn query_all_offices(&self) -> Result<HashMap<String, data::Office>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT id, name, photo_url, photo_attribution
            FROM office
            ORDER BY id
        ",
        )?;

        let iter = stmt.query_map([], |row| {
            let photo = if let Some(url) = row.get(2)? {
                Some(data::Photo {
                    url,
                    attribution: row.get(3)?,
                })
            } else {
                None
            };
            Ok((row.get(0)?, row.get(1)?, photo))
        })?;

        let mut offices = HashMap::new();
        for result in iter {
            let (id, name, photo): (String, String, Option<data::Photo>) = result?;
            let contacts = self.query_contacts_for_office(&id)?;
            let supervisors = self.query_supervisors_for_office_flat(&id)?;

            let office_data = data::Office {
                name,
                photo,
                contacts: Some(contacts).filter(|c| !c.is_empty()),
                supervisors: Some(supervisors).filter(|s| !s.is_empty()),
            };

            offices.insert(id, office_data);
        }

        Ok(offices)
    }

    pub fn query_subordinates_for_office(
        &self,
        office_id: &str,
    ) -> Result<BTreeMap<data::SupervisingRelation, Vec<context::Officer>>> {
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
        let iter = stmt.query_map([office_id], |row| {
            let relation: String = row.get(0)?;
            let subordinate_office_id: String = row.get(1)?;
            let office_name: String = row.get(2)?;

            let person = if let (Some(id), Some(name)) = (row.get(3)?, row.get(4)?) {
                Some(context::Person { id, name })
            } else {
                None
            };

            Ok((
                relation,
                context::Officer {
                    office_id: subordinate_office_id,
                    office_name,
                    person,
                },
            ))
        })?;

        let mut dtos = BTreeMap::new();
        for result in iter {
            let (relation, officer) = result?;
            let relation = self
                .all_supervising_relation_variants
                .get(&relation)
                .context(format!(
                    "could not understand supervisor relation `{}`",
                    relation
                ))?
                .clone();
            dtos.entry(relation).or_insert_with(Vec::new).push(officer);
        }
        Ok(dtos)
    }

    pub fn query_supervisors_for_office(
        &self,
        office_id: &str,
    ) -> Result<BTreeMap<data::SupervisingRelation, context::Officer>> {
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

        let mut supervisors = BTreeMap::new();
        for result in iter {
            let (relation, officer) = result?;
            let relation = self
                .all_supervising_relation_variants
                .get(&relation)
                .with_context(|| format!("unknown relation {}", relation))?
                .clone();
            supervisors.insert(relation, officer);
        }

        Ok(supervisors)
    }

    fn query_supervisors_for_office_flat(
        &self,
        office_id: &str,
    ) -> Result<BTreeMap<data::SupervisingRelation, String>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT relation, supervisor_office_id
            FROM supervisor
            WHERE office_id = ?1
        ",
        )?;
        let iter = stmt.query_map([office_id], |row| Ok((row.get(0)?, row.get(1)?)))?;

        let mut supervisors = BTreeMap::new();
        for result in iter {
            let (relation_str, supervisor_office_id): (String, String) = result?;
            let relation = self
                .all_supervising_relation_variants
                .get(&relation_str)
                .with_context(|| format!("unknown relation {}", relation_str))?
                .clone();
            supervisors.insert(relation, supervisor_office_id);
        }

        Ok(supervisors)
    }
}
