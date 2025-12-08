use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use chrono::NaiveDate;
use rusqlite::{Connection, params};
use serde_variant::to_variant_name;

use crate::{
    LibrarySql,
    context::{self},
    data::{self, ContactType},
    dto::{self, EntityType},
};

pub struct Repository<'a> {
    pub conn: &'a mut Connection,
    all_supervising_relation_variants: HashMap<String, data::SupervisingRelation>,
}

impl<'a> Repository<'a> {
    pub fn new(conn: &'a mut Connection) -> Result<Repository<'a>> {
        Ok(Repository {
            conn,
            all_supervising_relation_variants: Self::build_supervising_relation_variants()
                .with_context(|| format!("could not build SupervisingRelation variants"))?,
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

    pub fn insert_person_data(
        &mut self,
        id: &str,
        person: &data::Person,
        commit_date: Option<&str>,
    ) -> Result<()> {
        self.conn
            .save_entity_name(&dto::EntityType::Person, id, &person.name)?;

        if let Some(data::Photo { url, attribution }) = &person.photo {
            self.conn.save_entity_photo(
                &dto::EntityType::Person,
                id,
                url,
                attribution.as_ref().map(String::as_str),
            )?;
        }
        let tx = self.conn.transaction()?;

        // Insert contacts if they exist
        if let Some(contacts) = &person.contacts {
            for (contact_type, value) in contacts {
                tx.execute("INSERT INTO entity_contact (entity_type, entity_id, type, value) VALUES ('person', ?1, ?2, ?3)", params![id, to_variant_name(contact_type)?, value])?;
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

    pub fn insert_office_data(
        &mut self,
        id: &str,
        office: &data::Office,
        commit_date: Option<&str>,
    ) -> Result<()> {
        self.conn
            .save_entity_name(&dto::EntityType::Office, id, &office.name)?;

        if let Some(data::Photo { url, attribution }) = &office.photo {
            self.conn.save_entity_photo(
                &dto::EntityType::Office,
                id,
                url,
                attribution.as_ref().map(String::as_str),
            )?;
        }

        let tx = self.conn.transaction()?;

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
            let mut contacts: BTreeMap<ContactType, String> = BTreeMap::new();
            self.conn
                .get_entity_contacts(&EntityType::Office, &id, |row| {
                    contacts.insert(row.get(0)?, row.get(1)?);

                    Ok(())
                })?;
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
}
