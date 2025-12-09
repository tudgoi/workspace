use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use chrono::NaiveDate;
use rusqlite::{Connection, params};
use serde_variant::to_variant_name;

use crate::{
    LibrarySql,
    context::{self},
    data::{self},
    dto::{self},
};

pub struct Repository<'a> {
    pub conn: &'a mut Connection,
}

impl<'a> Repository<'a> {
    pub fn new(conn: &'a mut Connection) -> Result<Repository<'a>> {
        Ok(Repository {
            conn,
        })
    }

    pub fn insert_person_data(
        &mut self,
        id: &str,
        person: &data::Person,
        commit_date: Option<&NaiveDate>,
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
            tx.save_entity_commit(&dto::EntityType::Person, id, date)?;
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
        commit_date: Option<&NaiveDate>,
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
            tx.save_entity_commit(&dto::EntityType::Office, id, date)?;
        }

        tx.commit()?;

        Ok(())
    }
}
