use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde_variant::to_variant_name;

use crate::{
    context,
    data::{self, SupervisingRelation},
    dto::{self, EntityType},
    graph,
};

const DB_SCHEMA_SQL: &str = include_str!("schema.sql");

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
            .execute_batch(DB_SCHEMA_SQL)
            .with_context(|| format!("could not create DB schema"))?;

        Ok(())
    }

    pub fn save_tenure_for_person(&mut self, id: &str, tenure: &data::Tenure) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO tenure (person_id, office_id, start, end) VALUES (?1, ?2, ?3, ?4)",
                (id, &tenure.office_id, &tenure.start, &tenure.end),
            )
            .with_context(|| format!("could not insert tenure into DB for {}", id))?;

        Ok(())
    }

    pub fn save_tenures_for_person(&mut self, id: &str, tenures: &Vec<data::Tenure>) -> Result<()> {
        for tenure in tenures {
            self.save_tenure_for_person(id, &tenure)?;
        }

        Ok(())
    }

    pub fn insert_entity(
        &mut self,
        entity_type: &dto::EntityType,
        id: &str,
        name: &str,
    ) -> Result<()> {
        let entity_type_str = to_variant_name(entity_type)
            .with_context(|| format!("could not convert {:?} to string", entity_type))?;
        self.conn.execute(
            "INSERT INTO entity (type, id, name) VALUES (?1, ?2, ?3)",
            params![entity_type_str, id, name],
        )?;
        Ok(())
    }

    pub fn insert_person_data(
        &mut self,
        id: &str,
        person: &data::Person,
        commit_date: Option<&str>,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;

        // Insert into the base 'entity' table
        tx.execute(
            "INSERT INTO entity (type, id, name) VALUES ('person', ?1, ?2)",
            params![id, &person.name],
        )?;

        // Insert photo if it exists
        if let Some(photo) = &person.photo {
            tx.execute(
                "INSERT INTO entity_photo (entity_type, entity_id, url, attribution) VALUES ('person', ?1, ?2, ?3)",
                params![id, &photo.url, &photo.attribution],
            )?;
        }

        // Insert contacts if they exist
        if let Some(contacts) = &person.contacts {
            for (contact_type, value) in contacts {
                tx.execute("INSERT INTO entity_contact (entity_type, entity_id, type, value) VALUES ('person', ?1, ?2, ?3)", params![id, to_variant_name(contact_type)?, value])?;
            }
        }

        // Insert tenures if they exist
        if let Some(tenures) = &person.tenures {
            for tenure in tenures {
                tx.execute("INSERT INTO person_office_tenure (person_id, office_id, start, end) VALUES (?1, ?2, ?3, ?4)", params![id, &tenure.office_id, &tenure.start, &tenure.end])?;
            }
        }

        if let Some(date) = commit_date {
            tx.execute("INSERT INTO entity_commit (entity_type, entity_id, date) VALUES ('person', ?1, ?2)", params![id, date])?;
        }

        tx.commit()?;

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
        self.conn
            .execute(
                "INSERT OR IGNORE INTO person_contact (id, type, value) VALUES (?1, ?2, ?3)",
                params![id, to_variant_name(contact_type)?, value],
            )
            .with_context(|| format!("could not insert contact for person {}", id))?;
        Ok(())
    }

    pub fn save_office_contact(
        &mut self,
        id: &str,
        contact_type: &data::ContactType,
        value: &str,
    ) -> Result<()> {
        self.insert_entity_contact(
            &EntityType::Office,
            id,
            contact_type,
            value,
        )
    }

    pub fn save_office(&mut self, id: &str, office: &data::Office) -> Result<()> {
        let tx = self.conn.transaction()?;

        // Insert into the base 'entity' table
        tx.execute(
            "INSERT INTO entity (type, id, name) VALUES ('office', ?1, ?2)",
            params![id, &office.name],
        )?;

        // Insert photo if it exists
        if let Some(photo) = &office.photo {
            tx.execute(
                "INSERT INTO entity_photo (entity_type, entity_id, url, attribution) VALUES ('office', ?1, ?2, ?3)",
                params![id, &photo.url, &photo.attribution],
            ).with_context(|| format!("could not insert photo for office"))?;
        }

        // Insert supervisors if they exist
        if let Some(supervisors) = &office.supervisors {
            for (relation, supervisor_office_id) in supervisors {
                tx.execute("INSERT INTO office_supervisor (office_id, relation, supervisor_office_id) VALUES (?1, ?2, ?3)", params![id, to_variant_name(relation)?, supervisor_office_id])?;
            }
        }

        tx.commit()?;

        if let Some(contacts) = &office.contacts {
        }

        Ok(())
    }

    pub fn save_supervisor(
        &mut self,
        office_id: &str,
        relation: &data::SupervisingRelation,
        supervisor_office_id: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO supervisor (office_id, relation, supervisor_office_id) VALUES (?1, ?2, ?3)",
            params![office_id, to_variant_name(relation)?, supervisor_office_id],
        ).with_context(|| format!("could not insert supervisor {} into DB", supervisor_office_id))?;

        Ok(())
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

    pub fn query_uncommitted_persons(&self) -> Result<Vec<context::Person>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name FROM person WHERE commit_date IS NULL ORDER BY id")?;

        let persons = stmt
            .query_map([], |row| {
                Ok(context::Person {
                    id: row.get(0)?,
                    name: row.get(1)?,
                })
            })?
            .collect::<Result<Vec<context::Person>, _>>()?;

        Ok(persons)
    }

    pub fn query_person_commit_date(&self, id: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT commit_date FROM person WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .with_context(|| format!("could not query commit date date for person {}", id))
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

            Ok((office_id, row.get(1)?, photo))
        })?;
        let mut offices = Vec::new();
        for result in iter {
            let (id, name, photo) = result?;
            let contacts = self.query_contacts_for_office(&id)?;

            offices.push(dto::Office {
                id,
                name,
                photo,
                contacts: if contacts.is_empty() {
                    None
                } else {
                    Some(contacts)
                },
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
                office_id: row.get(0)?,
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

    pub fn query_past_tenures(&self, id: &str) -> Result<Vec<context::TenureDetails>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT
                q.office_id,
                o.name,
                q.start,
                q.end
            FROM quondam AS q
            INNER JOIN office AS o ON o.id = q.office_id
            WHERE q.person_id = ?1
            ORDER BY q.end DESC
        ",
        )?;
        let iter = stmt.query_map([id], |row| {
            Ok(context::TenureDetails {
                office: context::Office {
                    id: row.get(0)?,
                    name: row.get(1)?,
                },
                start: row.get(2)?,
                end: row.get(3)?,
            })
        })?;
        Ok(iter.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn query_person(&self, id: &str) -> Result<Option<data::Person>> {
        let person_row = self
            .conn
            .query_row(
                "SELECT name, photo_url, photo_attribution FROM person WHERE id = ?1",
                [id],
                |row| {
                    let photo = if let Some(url) = row.get(1)? {
                        Some(data::Photo {
                            url,
                            attribution: row.get(2)?,
                        })
                    } else {
                        None
                    };
                    Ok((row.get(0)?, photo))
                },
            )
            .optional()?;

        if let Some((name, photo)) = person_row {
            let contacts = self.query_contacts_for_person(id)?;
            let tenures = self.query_tenures_for_person(id)?;

            let person = data::Person {
                name,
                photo,
                contacts: Some(contacts).filter(|c| !c.is_empty()),
                tenures: Some(tenures).filter(|t| !t.is_empty()),
            };
            Ok(Some(person))
        } else {
            Ok(None)
        }
    }

    pub fn query_all_persons(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT id FROM person ORDER BY id")?;

        let persons = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

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

    pub fn query_persons_without_contact(
        &self,
        contact_type: data::ContactType,
    ) -> Result<Vec<context::Person>> {
        let contact_type_str = to_variant_name(&contact_type)?;
        let mut stmt = self.conn.prepare(
            "
            SELECT p.id, p.name
            FROM person p
            WHERE NOT EXISTS (
                SELECT 1
                FROM person_contact pc
                WHERE pc.id = p.id AND pc.type = ?1
            )
            ",
        )?;

        let iter = stmt.query_map([contact_type_str], |row| {
            Ok(context::Person {
                id: row.get(0)?,
                name: row.get(1)?,
            })
        })?;

        let mut persons = Vec::new();
        for result in iter {
            let person = result?;
            persons.push(person);
        }

        Ok(persons)
    }

    pub fn query_external_persons_without_photo(
        &self,
        contact_type: data::ContactType,
    ) -> Result<HashMap<String, String>> {
        let contact_type = to_variant_name(&contact_type)?;
        let mut stmt = self.conn.prepare(
            "
            SELECT
                pc.value, -- wikidata_id
                p.id      -- person_id
            FROM
                person p
            JOIN
                person_contact pc ON p.id = pc.id
            WHERE
                p.photo_url IS NULL
                AND pc.type = ?1
            ",
        )?;

        let iter = stmt.query_map([contact_type], |row| {
            let wikidata_id: String = row.get(0)?;
            let person_id: String = row.get(1)?;
            Ok((wikidata_id, person_id))
        })?;

        let mut map = HashMap::new();
        for result in iter {
            let (wikidata_id, person_id) = result?;
            map.insert(wikidata_id, person_id);
        }

        Ok(map)
    }

    pub fn query_persons_with_contact_without_contact(
        &self,
        with_contact_type: data::ContactType,
        without_contact_type: data::ContactType,
    ) -> Result<HashMap<String, String>> {
        let with_contact_type_str = to_variant_name(&with_contact_type)?;
        let without_contact_type_str = to_variant_name(&without_contact_type)?;

        let mut stmt = self.conn.prepare(
            "
            SELECT with_pc.value, p.id
            FROM person p
            JOIN person_contact with_pc ON p.id = with_pc.id AND with_pc.type = ?1
            WHERE NOT EXISTS (
                SELECT 1
                FROM person_contact without_pc
                WHERE without_pc.id = p.id AND without_pc.type = ?2
            )
            ",
        )?;

        let iter = stmt.query_map([with_contact_type_str, without_contact_type_str], |row| {
            let with_contact_value: String = row.get(0)?;
            let person_id: String = row.get(1)?;
            Ok((with_contact_value, person_id))
        })?;

        let mut map = HashMap::new();
        for result in iter {
            let (with_contact_value, person_id) = result?;
            map.insert(with_contact_value, person_id);
        }

        Ok(map)
    }

    pub fn enable_commit_tracking(&mut self) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO commit_tracking (id, enabled) VALUES (1, 1)",
            params![],
        )?;
        Ok(())
    }

    pub fn entity_exists(&self, entity_type: &dto::EntityType, id: &str) -> Result<bool> {
        let entity_type = to_variant_name(entity_type)
            .with_context(|| format!("could not convert {:?} to string", entity_type))?;
        let mut stmt = self.conn.prepare(
            "SELECT EXISTS(
                 SELECT 1 FROM entity WHERE type = ?1 AND id = ?2
             )",
        )?;

        let exists: i32 = stmt.query_row((entity_type, id), |row| row.get(0))?;
        Ok(exists != 0)
    }

    pub fn entity_photo_exists(&self, entity_type: &dto::EntityType, id: &str) -> Result<bool> {
        let entity_type_str = to_variant_name(entity_type)
            .with_context(|| format!("could not convert {:?} to string", entity_type))?;
        let mut stmt = self.conn.prepare(
            "SELECT EXISTS(
                 SELECT 1 FROM entity_photo WHERE entity_type = ?1 AND entity_id = ?2
             )",
        )?;
        let exists: i32 = stmt.query_row((entity_type_str, id), |row| row.get(0))?;
        Ok(exists != 0)
    }

    pub fn insert_entity_photo(
        &mut self,
        entity_type: &dto::EntityType,
        id: &str,
        url: &str,
        attribution: Option<&str>,
    ) -> Result<()> {
        let entity_type_str = to_variant_name(entity_type)
            .with_context(|| format!("could not convert {:?} to string", entity_type))?;
        self.conn.execute(
            "INSERT INTO entity_photo (entity_type, entity_id, url, attribution)
             VALUES (?1, ?2, ?3, ?4)",
            params![entity_type_str, id, url, attribution],
        )?;
        Ok(())
    }

    /// Do an FTS query on entity_idx table and optionally restrict to the given entity_type
    pub fn search_entities(
        &self,
        query: &str,
        entity_type: Option<&EntityType>,
    ) -> Result<Vec<dto::Entity>> {
        let mut sql = "SELECT e.type, e.id, e.name FROM entity_idx AS fts
                       JOIN entity AS e ON fts.rowid = e.rowid
                       WHERE fts(?1)".to_string();
        if entity_type.is_some() {
            sql.push_str(" AND e.type = ?2");
        }
        sql.push_str(" ORDER BY rank");

        let mut stmt = self.conn.prepare(&sql)?;
        
        let entity_type_str = if let Some(et) = entity_type {
            Some(to_variant_name(et)?)
        } else {
            None
        };

        let entities = if let Some(ref et_str) = entity_type_str {
            stmt.query_map(params![query, et_str], |row| {
                let entity_type_str: String = row.get(0)?;
                let entity_type = if entity_type_str == "person" { EntityType::Person } else { EntityType::Office };
                Ok(dto::Entity { entity_type, id: row.get(1)?, name: row.get(2)? })
            })?.collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![query], |row| {
                let entity_type_str: String = row.get(0)?;
                let entity_type = if entity_type_str == "person" { EntityType::Person } else { EntityType::Office };
                Ok(dto::Entity { entity_type, id: row.get(1)?, name: row.get(2)? })
            })?.collect::<Result<Vec<_>, _>>()?
        };

        Ok(entities)
    }

    pub fn entity_contact_exists(
        &self,
        entity_type: &dto::EntityType,
        id: &str,
        contact_type: &data::ContactType,
    ) -> Result<bool> {
        let entity_type_str = to_variant_name(entity_type)?;
        let contact_type_str = to_variant_name(contact_type)?;
        let mut stmt = self.conn.prepare(
            "SELECT EXISTS(
                SELECT 1 FROM entity_contact
                WHERE entity_type = ?1 AND entity_id = ?2 AND type = ?3
            )"
        )?;
        let exists: i32 = stmt.query_row((entity_type_str, id, contact_type_str), |row| row.get(0))?;
        Ok(exists != 0)
    }

    pub fn insert_entity_contact(
        &mut self,
        entity_type: &dto::EntityType,
        id: &str,
        contact_type: &data::ContactType,
        value: &str,
    ) -> Result<()> {
        let entity_type_str = to_variant_name(entity_type)?;
        let contact_type_str = to_variant_name(contact_type)?;
        self.conn.execute(
            "INSERT INTO entity_contact (entity_type, entity_id, type, value) VALUES (?1, ?2, ?3, ?4)",
            params![entity_type_str, id, contact_type_str, value],
        )?;
        Ok(())
    }
    
    pub fn office_supervisor_exists(
        &self,
        id: &str,
        relation: &data::SupervisingRelation,
    ) -> Result<bool> {
        let relation_str = to_variant_name(relation)?;
        let mut stmt = self.conn.prepare(
            "SELECT EXISTS(
                SELECT 1 FROM office_supervisor WHERE office_id = ?1 AND relation = ?2
            )"
        )?;
        let exists: i32 = stmt.query_row((id, relation_str), |row| row.get(0))?;
        Ok(exists != 0)
    }
    
    pub fn insert_office_supervisor(
        &mut self,
        office_id: &str,
        relation: &data::SupervisingRelation,
        supervisor_office_id: &str,
    ) -> Result<()> {
        let relation_str = to_variant_name(relation)?;
        self.conn.execute(
            "INSERT INTO office_supervisor (office_id, relation, supervisor_office_id)
             VALUES (?1, ?2, ?3)",
            params![office_id, relation_str, supervisor_office_id],
        )?;
        Ok(())
    }

    pub fn insert_tenure(&mut self, person_id: &str, office_id: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO tenure (person_id, office_id) VALUES (?1, ?2)",
            params![person_id, office_id],
        )?;
        Ok(())
    }
}
