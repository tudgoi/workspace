pub mod sqlitebe;

use chrono::NaiveDate;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use thiserror::Error;

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
}

#[derive(Clone, Debug)]
pub struct Key<State, Schema> {
    path: String,
    _marker: PhantomData<(State, Schema)>,
}

#[derive(Clone, Copy)]
pub struct PersonPath;
#[derive(Clone, Copy)]
pub struct OfficePath;
#[derive(Clone, Copy)]
pub struct NamePath;
#[derive(Clone, Copy)]
pub struct PhotoPath;
#[derive(Clone, Copy)]
pub struct ContactPath;
#[derive(Clone, Copy)]
pub struct SupervisorPath;
#[derive(Clone, Copy)]
pub struct TenurePath;

pub trait EntityPathTrait {}

impl Key<PersonPath, ()> {
    pub fn new(id: &str) -> Self {
        Self {
            path: format!("{}/{}", dto::EntityType::Person, id),
            _marker: PhantomData,
        }
    }

    pub fn tenure(
        &self,
        office_id: &str,
        start: Option<NaiveDate>,
    ) -> Key<TenurePath, Option<NaiveDate>> {
        Key {
            path: format!(
                "{}/tenure/{}/{}",
                self.path,
                office_id,
                start.map(|d| d.to_string()).unwrap_or_default(),
            ),
            _marker: PhantomData,
        }
    }
}

impl EntityPathTrait for PersonPath {}

impl Key<OfficePath, ()> {
    pub fn new(id: &str) -> Self {
        Self {
            path: format!("{}/{}", dto::EntityType::Office, id),
            _marker: PhantomData,
        }
    }

    pub fn supervisor(&self, relation: data::SupervisingRelation) -> Key<SupervisorPath, String> {
        Key {
            path: format!("{}/supervisor/{}", self.path, relation),
            _marker: PhantomData,
        }
    }
}

impl EntityPathTrait for OfficePath {}

impl<P: EntityPathTrait, T> Key<P, T> {
    pub fn name(&self) -> Key<NamePath, String> {
        Key {
            path: format!("{}/name", self.path),
            _marker: PhantomData,
        }
    }
    pub fn photo(&self) -> Key<PhotoPath, data::Photo> {
        Key {
            path: format!("{}/photo", self.path),
            _marker: PhantomData,
        }
    }

    pub fn contact(&self, typ: data::ContactType) -> Key<ContactPath, String> {
        Key {
            path: format!("{}/contact/{}", self.path, typ),
            _marker: PhantomData,
        }
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
    ) -> Result<(), RecordRepoError> {
        let bytes = serde_json::to_vec(value)?;
        self.repo.write(key.path.as_bytes().to_vec(), bytes)?;

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
