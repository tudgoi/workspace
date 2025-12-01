use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde_variant::to_variant_name;

use crate::{
    context::{self}, data::{self}, dto::{self, EntityType}, graph, ENTITY_SCHEMA_SQL, PROPERTY_SCHEMA_SQL
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
        const ALL_VARIANTS: [data::SupervisingRelation; 6] = [
            data::SupervisingRelation::Adviser,
            data::SupervisingRelation::DuringThePleasureOf,
            data::SupervisingRelation::Head,
            data::SupervisingRelation::ResponsibleTo,
            data::SupervisingRelation::MemberOf,
            data::SupervisingRelation::Minister,
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
            .execute_batch(ENTITY_SCHEMA_SQL)
            .with_context(|| format!("could not create entity schema"))?;

        self.conn
            .execute_batch(PROPERTY_SCHEMA_SQL)
            .with_context(|| format!("could not create property schema"))?;
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

    pub fn insert_office_data(&mut self, id: &str, office: &data::Office, commit_date: Option<&str>) -> Result<()> {
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

        // Insert contacts if they exist
        if let Some(contacts) = &office.contacts {
            for (contact_type, value) in contacts {
                tx.execute("INSERT INTO entity_contact (entity_type, entity_id, type, value) VALUES ('office', ?1, ?2, ?3)", params![id, to_variant_name(contact_type)?, value])?;
            }
        }

        if let Some(date) = commit_date {
            tx.execute("INSERT INTO entity_commit (entity_type, entity_id, date) VALUES ('office', ?1, ?2)", params![id, date])?;
        }

        tx.commit()?;

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

    pub fn list_person_office_tenure(&self, person_id: &str) -> Result<Vec<data::Tenure>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT office_id, start, end 
            FROM person_office_tenure
            WHERE person_id = ?1
            ",
        )?;

        let tenures = stmt
            .query_map([person_id], |row| {
                Ok(data::Tenure {
                    office_id: row.get(0)?,
                    start: row.get(1)?,
                    end: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(tenures)
    }

    pub fn get_person_past_tenures(&self, id: &str) -> Result<Vec<context::TenureDetails>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT
                q.office_id,
                o.name,
                q.start,
                q.end
            FROM person_office_quondam AS q
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

    pub fn list_all_person_ids(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT id FROM person ORDER BY id")?;

        let persons = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<String>, _>>()?;

        Ok(persons)
    }

    pub fn list_all_office_data(&self) -> Result<HashMap<String, data::Office>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT e.id, e.name, p.url, p.attribution
            FROM entity AS e
            LEFT JOIN entity_photo AS p ON e.id = p.entity_id AND p.entity_type = 'office'
            WHERE e.type = 'office'
            ORDER BY e.id
            ",
        )?;

        let office_iter = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let name: String = row.get(1)?;
            let photo = if let Some(url) = row.get(2)? {
                Some(data::Photo {
                    url,
                    attribution: row.get(3)?,
                })
            } else {
                None
            };
            Ok((id, name, photo))
        })?;

        let mut offices = HashMap::new();
        for result in office_iter {
            let (id, name, photo) = result?;
            let contacts = self.get_entity_contacts(&EntityType::Office, &id)?;
            let supervisors = self.query_supervisors_for_office_flat(&id)?;
            offices.insert(
                id,
                data::Office {
                    name,
                    photo,
                    contacts: if contacts.is_empty() {
                        None
                    } else {
                        Some(contacts)
                    },
                    supervisors: if supervisors.is_empty() {
                        None
                    } else {
                        Some(supervisors)
                    },
                },
            );
        }
        Ok(offices)
    }

    pub fn get_office_subordinates(
        &self,
        office_id: &str,
    ) -> Result<BTreeMap<data::SupervisingRelation, Vec<context::Officer>>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT s.relation, s.office_id, o.name, i.person_id, p.name
            FROM office_supervisor AS s
            INNER JOIN office AS o ON o.id = s.office_id
            LEFT JOIN person_office_incumbent AS i ON i.office_id = s.office_id
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

    pub fn get_office_supervisors(
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
            FROM office_supervisor AS s
            INNER JOIN office AS o ON o.id = s.supervisor_office_id
            LEFT JOIN person_office_incumbent AS i ON i.office_id = s.supervisor_office_id
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
            FROM office_supervisor
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

    fn escape_for_fts(input: &str) -> String {
        let mut s = String::from("\"");
        for c in input.chars() {
            if c == '"' {
                s.push_str("\"\""); // escape quotes by doubling
            } else {
                s.push(c);
            }
        }
        s.push('"');
        s
    }

    /// # entity

    /// Do an FTS query on entity_idx table and optionally restrict to the given entity_type
    pub fn exists_entity(&self, entity_type: &dto::EntityType, id: &str) -> Result<bool> {
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

    pub fn search_entity(
        &self,
        query: &str,
        entity_type: Option<&EntityType>,
    ) -> Result<Vec<dto::Entity>> {
        let query = Self::escape_for_fts(query);
        let mut sql = "SELECT e.type, e.id, e.name FROM entity_idx(?1) AS fts
                       JOIN entity AS e ON fts.rowid = e.rowid
                       WHERE "
            .to_string();
        if entity_type.is_some() {
            sql.push_str("e.type = ?2");
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
                let entity_type = if entity_type_str == "person" {
                    EntityType::Person
                } else {
                    EntityType::Office
                };
                Ok(dto::Entity {
                    entity_type,
                    id: row.get(1)?,
                    name: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![query], |row| {
                let entity_type_str: String = row.get(0)?;
                let entity_type = if entity_type_str == "person" {
                    EntityType::Person
                } else {
                    EntityType::Office
                };
                Ok(dto::Entity {
                    entity_type,
                    id: row.get(1)?,
                    name: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        Ok(entities)
    }

    /// # office
    pub fn list_all_office_id(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT id FROM office")?;

        let ids = stmt
            .query_map([], |row| Ok(row.get(0)?))?
            .collect::<Result<Vec<String>, _>>()?;

        Ok(ids)
    }

    pub fn get_office_name(&self, id: &str) -> Result<String> {
        let name = self
            .conn
            .query_row("SELECT name FROM office WHERE id=?1", [id], |row| {
                row.get(0)
            })
            .with_context(|| format!("could not get office name for {}", id))?;
        Ok(name)
    }

    /// # entity_photo
    
    pub fn get_entity_photo(&self, entity_type: graph::EntityType, id: &str) -> Result<Option<data::Photo>> {
        let entity_type_str: &str = to_variant_name(&entity_type)?;
        self.conn
            .query_row(
                "SELECT url, attribution FROM entity_photo WHERE entity_type = ?1 AND entity_id = ?2",
                params![entity_type_str, id],
                |row| Ok(data::Photo {
                    url: row.get(0)?,
                    attribution: row.get(1)?,
                }),
            )
            .optional()
            .with_context(|| format!("could not get photo for {} {}", entity_type_str, id))
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

    /// # entity_contact

    pub fn get_entity_contacts(
        &self,
        entity_type: &dto::EntityType,
        id: &str,
    ) -> Result<BTreeMap<data::ContactType, String>> {
        let entity_type_str = to_variant_name(entity_type)?;
        let mut stmt = self.conn.prepare(
            "
            SELECT type, value
            FROM entity_contact
            WHERE entity_type = ?1 AND entity_id = ?2
        ",
        )?;
        let iter = stmt.query_map(params![entity_type_str, id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
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

    pub fn exists_entity_contact(
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
            )",
        )?;
        let exists: i32 =
            stmt.query_row((entity_type_str, id, contact_type_str), |row| row.get(0))?;
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

    /// # entity_commit
    
    pub fn get_entity_commit_date(&self, entity_type: graph::EntityType, id: &str) -> Result<Option<String>> {
        let entity_type_str: &str = to_variant_name(&entity_type)?;
        self.conn
            .query_row(
                "SELECT date FROM entity_commit WHERE entity_type = ?1 AND entity_id = ?2",
                params![entity_type_str, id],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| format!("could not get commit date for {} {}", entity_type_str, id))
    }

    pub fn list_entity_uncommitted(&self) -> Result<Vec<dto::Entity>> {
        let mut stmt = self
            .conn
            .prepare("SELECT e.type, e.id, e.name FROM entity AS e LEFT JOIN entity_commit AS c ON e.id=c.entity_id WHERE e.type = 'person' AND c.date IS NULL ORDER BY e.id")?;

        let entities = stmt
            .query_map([], |row| {
                let entity_type_str: String = row.get(0)?;
                let entity_type = if entity_type_str == "person" {
                    EntityType::Person
                } else {
                    EntityType::Office
                };
                Ok(dto::Entity {
                    entity_type,
                    id: row.get(1)?,
                    name: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(entities)
    }

    /// # person
    pub fn get_person(&self, id: &str) -> Result<Option<dto::Person>> {
        self.conn
            .query_row(
                "
                SELECT
                    e.name,
                    p.thumbnail_url,
                    p.attribution,
                    c.date
                FROM entity AS e
                LEFT JOIN entity_photo AS p ON e.id = p.entity_id AND e.type = p.entity_type
                LEFT JOIN entity_commit AS c ON e.id = c.entity_id AND e.type = c.entity_type
                WHERE e.type = 'person' AND e.id = ?1
                ",
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
                    let contacts = self.get_entity_contacts(&EntityType::Person, id).ok();
                    Ok(dto::Person {
                        id: id.to_string(),
                        name: row.get(0)?,
                        photo,
                        contacts,
                        commit_date: row.get(3)?,
                    })
                },
            )
            .optional()
            .with_context(|| format!("could not get person {}", id))
    }

    /// # person_office_incumbent
    pub fn list_person_office_incumbent_office(&self, person_id: &str) -> Result<Vec<dto::Office>> {
        let mut stmt = self.conn.prepare(
            "
            SELECT i.office_id, e.name, p.url, p.attribution
            FROM person_office_incumbent AS i
            JOIN entity AS e ON i.office_id = e.id AND e.type = 'office'
            LEFT JOIN entity_photo AS p ON i.office_id = p.entity_id AND p.entity_type = 'office'
            WHERE i.person_id = ?1
            ",
        )?;
        stmt.query_map([person_id], |row| {
            let contacts = self
                .get_entity_contacts(&EntityType::Office, &row.get::<_, String>(0)?)
                .ok();
            Ok(dto::Office {
                id: row.get(0)?,
                name: row.get(1)?,
                photo: if let Some(url) = row.get(2)? {
                    Some(data::Photo {
                        url,
                        attribution: row.get(3)?,
                    })
                } else {
                    None
                },
                contacts,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("could not list incumbent offices for person {}", person_id))
    }
    
    pub fn get_person_office_incumbent_person(&self, office_id: &str) -> Result<Option<context::Person>> {
        self.conn
            .query_row(
                "SELECT p.id, p.name FROM person_office_incumbent AS i
                 JOIN person AS p ON i.person_id = p.id
                 WHERE i.office_id = ?1",
                [office_id],
                |row| Ok(context::Person {
                    id: row.get(0)?,
                    name: row.get(1)?,
                }),
            )
            .optional()
            .with_context(|| format!("could not get incumbent for office {}", office_id))
    } 
    
    /// # person_office_quondam
    pub fn list_person_office_quondam(&self, office_id: &str) -> Result<Vec<context::Quondam>> {
        let mut stmt = self.conn.prepare(
            "SELECT q.person_id, p.name, q.start, q.end FROM person_office_quondam AS q
             JOIN person AS p ON q.person_id = p.id
             WHERE q.office_id = ?1 ORDER BY q.end DESC",
        )?;
        stmt.query_map([office_id], |row| Ok(context::Quondam {
            person: context::Person{
                id: row.get(0)?,
                name: row.get(1)?,
            },
            start: row.get(2)?,
            end: row.get(3)?,
        }))?.collect::<Result<Vec<_>, _>>().with_context(|| format!("could not list quondams for office {}", office_id))
    }

    // [office_supervisor]

    pub fn exists_office_supervisor(
        &self,
        id: &str,
        relation: &data::SupervisingRelation,
    ) -> Result<bool> {
        let relation_str = to_variant_name(relation)?;
        let mut stmt = self.conn.prepare(
            "SELECT EXISTS(
                SELECT 1 FROM office_supervisor WHERE office_id = ?1 AND relation = ?2
            )",
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

    // [person_office_tenure]

    pub fn insert_person_office_tenure(&mut self, person_id: &str, office_id: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO person_office_tenure (person_id, office_id) VALUES (?1, ?2)",
            params![person_id, office_id],
        )?;
        Ok(())
    }
}
