pub mod sqlitebe;

use chrono::NaiveDate;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use thiserror::Error;
use crate::WriteSql;

use crate::{
    data, dto,
    repo::{Repo, RepoError},
};
use sqlitebe::SqliteBackend;

#[derive(Error, Debug)]
pub enum RecordRepoError {
    #[error("record error: {0}")]
    Repo(#[from] RepoError),

    #[error("serde_json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

#[derive(Clone, Debug)]
pub struct Key<State, Schema> {
    pub entity_type: dto::EntityType,
    pub entity_id: String,
    pub path: String,
    pub state: State,
    _marker: PhantomData<Schema>,
}

#[derive(Clone, Copy)]
pub struct PersonPath;
#[derive(Clone, Copy)]
pub struct OfficePath;
#[derive(Clone, Copy)]
pub struct NamePath;
#[derive(Clone, Copy)]
pub struct PhotoPath;
#[derive(Clone)]
pub struct ContactPath {
    pub typ: data::ContactType,
}
#[derive(Clone)]
pub struct SupervisorPath {
    pub relation: data::SupervisingRelation,
}
#[derive(Clone)]
pub struct TenurePath {
    pub office_id: String,
    pub start: Option<NaiveDate>,
}

pub trait EntityPathTrait {}

impl Key<PersonPath, ()> {
    pub fn new(id: &str) -> Self {
        Self {
            entity_type: dto::EntityType::Person,
            entity_id: id.to_string(),
            path: format!("{}/{}", dto::EntityType::Person, id),
            state: PersonPath,
            _marker: PhantomData,
        }
    }

    pub fn tenure(
        &self,
        office_id: &str,
        start: Option<NaiveDate>,
    ) -> Key<TenurePath, Option<NaiveDate>> {
        Key {
            entity_type: self.entity_type,
            entity_id: self.entity_id.clone(),
            path: format!(
                "{}/tenure/{}/{}",
                self.path,
                office_id,
                start.map(|d| d.to_string()).unwrap_or_default(),
            ),
            state: TenurePath {
                office_id: office_id.to_string(),
                start,
            },
            _marker: PhantomData,
        }
    }
}

impl EntityPathTrait for PersonPath {}

impl Key<OfficePath, ()> {
    pub fn new(id: &str) -> Self {
        Self {
            entity_type: dto::EntityType::Office,
            entity_id: id.to_string(),
            path: format!("{}/{}", dto::EntityType::Office, id),
            state: OfficePath,
            _marker: PhantomData,
        }
    }

    pub fn supervisor(&self, relation: data::SupervisingRelation) -> Key<SupervisorPath, String> {
        Key {
            entity_type: self.entity_type,
            entity_id: self.entity_id.clone(),
            path: format!("{}/supervisor/{}", self.path, relation),
            state: SupervisorPath { relation },
            _marker: PhantomData,
        }
    }
}

impl EntityPathTrait for OfficePath {}

impl<P: EntityPathTrait, T> Key<P, T> {
    pub fn name(&self) -> Key<NamePath, String> {
        Key {
            entity_type: self.entity_type,
            entity_id: self.entity_id.clone(),
            path: format!("{}/name", self.path),
            state: NamePath,
            _marker: PhantomData,
        }
    }
    pub fn photo(&self) -> Key<PhotoPath, data::Photo> {
        Key {
            entity_type: self.entity_type,
            entity_id: self.entity_id.clone(),
            path: format!("{}/photo", self.path),
            state: PhotoPath,
            _marker: PhantomData,
        }
    }

    pub fn contact(&self, typ: data::ContactType) -> Key<ContactPath, String> {
        Key {
            entity_type: self.entity_type,
            entity_id: self.entity_id.clone(),
            path: format!("{}/contact/{}", self.path, typ),
            state: ContactPath { typ },
            _marker: PhantomData,
        }
    }
}

pub trait TableUpdater<T> {
    fn update_tables(&self, conn: &Connection, value: &T) -> Result<(), RecordRepoError>;
}

impl TableUpdater<String> for Key<NamePath, String> {
    fn update_tables(&self, conn: &Connection, value: &String) -> Result<(), RecordRepoError> {
        conn.save_entity_name(&self.entity_type, &self.entity_id, value)?;
        Ok(())
    }
}

impl TableUpdater<data::Photo> for Key<PhotoPath, data::Photo> {
    fn update_tables(&self, conn: &Connection, value: &data::Photo) -> Result<(), RecordRepoError> {
        conn.save_entity_photo(
            &self.entity_type,
            &self.entity_id,
            &value.url,
            value.attribution.as_deref(),
        )?;
        Ok(())
    }
}

impl TableUpdater<String> for Key<ContactPath, String> {
    fn update_tables(&self, conn: &Connection, value: &String) -> Result<(), RecordRepoError> {
        conn.save_entity_contact(&self.entity_type, &self.entity_id, &self.state.typ, value)?;
        Ok(())
    }
}

impl TableUpdater<String> for Key<SupervisorPath, String> {
    fn update_tables(&self, conn: &Connection, value: &String) -> Result<(), RecordRepoError> {
        conn.save_office_supervisor(&self.entity_id, &self.state.relation, value)?;
        Ok(())
    }
}

impl TableUpdater<Option<NaiveDate>> for Key<TenurePath, Option<NaiveDate>> {
    fn update_tables(
        &self,
        conn: &Connection,
        value: &Option<NaiveDate>,
    ) -> Result<(), RecordRepoError> {
        conn.save_tenure(
            &self.entity_id,
            &self.state.office_id,
            self.state.start.as_ref(),
            value.as_ref(),
        )?;
        Ok(())
    }
}

pub struct RecordRepo<'a> {
    repo: Repo<SqliteBackend<'a>>,
}

impl<'a> RecordRepo<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        RecordRepo {
            repo: Repo::new(SqliteBackend::new(conn)),
        }
    }

    pub fn save<P, T: Serialize>(
        &mut self,
        key: Key<P, T>,
        value: &T,
    ) -> Result<(), RecordRepoError>
    where
        Key<P, T>: TableUpdater<T>,
    {
        let bytes = serde_json::to_vec(value)?;
        self.repo.write(key.path.as_bytes().to_vec(), bytes)?;
        key.update_tables(self.repo.backend.conn, value)?;

        Ok(())
    }

    pub fn load<P, T: for<'de> Deserialize<'de>>(
        &self,
        key: Key<P, T>,
    ) -> Result<Option<T>, RecordRepoError> {
        let value = if let Some(bytes) = self.repo.read(&key.path.as_bytes())? {
            serde_json::from_slice(&bytes)?
        } else {
            None
        };

        Ok(value)
    }
}