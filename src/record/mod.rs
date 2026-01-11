pub mod sqlitebe;

use chrono::NaiveDate;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::str::FromStr;
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

    #[error("postcard error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("unknown record type for path: {0}")]
    UnknownRecordType(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RecordValue {
    Name(String),
    Photo(data::Photo),
    Contact(String),
    Supervisor(String),
    Tenure(Option<NaiveDate>),
}

#[derive(Debug, Clone)]
pub enum RecordKey {
    Name(Key<NamePath, String>),
    Photo(Key<PhotoPath, data::Photo>),
    Contact(Key<ContactPath, String>),
    Supervisor(Key<SupervisorPath, String>),
    Tenure(Key<TenurePath, Option<NaiveDate>>),
}

#[derive(Clone, Debug)]
pub struct Key<State, Schema> {
    pub entity_type: dto::EntityType,
    pub entity_id: String,
    pub path: String,
    pub state: State,
    _marker: PhantomData<Schema>,
}

#[derive(Clone, Debug)]
pub struct PersonPath {
    pub id: Option<String>,
}
#[derive(Clone, Debug)]
pub struct OfficePath {
    pub id: Option<String>,
}
#[derive(Clone, Copy, Debug)]
pub struct NamePath;
#[derive(Clone, Copy, Debug)]
pub struct PhotoPath;
#[derive(Clone, Debug)]
pub struct ContactPath {
    pub typ: data::ContactType,
}
#[derive(Clone, Debug)]
pub struct SupervisorPath {
    pub relation: data::SupervisingRelation,
}
#[derive(Clone, Debug)]
pub struct TenurePath {
    pub office_id: String,
    pub start: Option<NaiveDate>,
}

pub trait ParseKeyState: Sized {
    fn parse(parts: &[&str]) -> Result<Self, RecordRepoError>;
}

impl ParseKeyState for PersonPath {
    fn parse(_parts: &[&str]) -> Result<Self, RecordRepoError> {
        Ok(PersonPath { id: None })
    }
}

impl ParseKeyState for OfficePath {
    fn parse(_parts: &[&str]) -> Result<Self, RecordRepoError> {
        Ok(OfficePath { id: None })
    }
}

impl ParseKeyState for NamePath {
    fn parse(_parts: &[&str]) -> Result<Self, RecordRepoError> {
        Ok(NamePath)
    }
}

impl ParseKeyState for PhotoPath {
    fn parse(_parts: &[&str]) -> Result<Self, RecordRepoError> {
        Ok(PhotoPath)
    }
}

impl ParseKeyState for ContactPath {
    fn parse(parts: &[&str]) -> Result<Self, RecordRepoError> {
        if parts.len() != 2 || parts[0] != "contact" {
            return Err(RecordRepoError::InvalidPath(format!("Invalid contact path: {:?}", parts)));
        }
        let typ = data::ContactType::from_str(parts[1])
            .map_err(|_| RecordRepoError::InvalidPath(format!("Invalid contact type: {}", parts[1])))?;
        Ok(ContactPath { typ })
    }
}

impl ParseKeyState for SupervisorPath {
    fn parse(parts: &[&str]) -> Result<Self, RecordRepoError> {
        if parts.len() != 2 || parts[0] != "supervisor" {
            return Err(RecordRepoError::InvalidPath(format!("Invalid supervisor path: {:?}", parts)));
        }
        let relation = data::SupervisingRelation::from_str(parts[1])
            .map_err(|_| RecordRepoError::InvalidPath(format!("Invalid supervisor relation: {}", parts[1])))?;
        Ok(SupervisorPath { relation })
    }
}

impl ParseKeyState for TenurePath {
    fn parse(parts: &[&str]) -> Result<Self, RecordRepoError> {
        if parts.len() != 3 || parts[0] != "tenure" {
            return Err(RecordRepoError::InvalidPath(format!("Invalid tenure path: {:?}", parts)));
        }
        let office_id = parts[1].to_string();
        let start = if parts[2].is_empty() {
            None
        } else {
            Some(NaiveDate::from_str(parts[2]).map_err(|_| {
                RecordRepoError::InvalidPath(format!("Invalid tenure start date: {}", parts[2]))
            })?)
        };
        Ok(TenurePath { office_id, start })
    }
}

pub trait EntityPathTrait: ParseKeyState {}

impl Key<PersonPath, ()> {
    pub fn new(id: &str) -> Self {
        Self {
            entity_type: dto::EntityType::Person,
            entity_id: id.to_string(),
            path: format!("{}/{}", dto::EntityType::Person, id),
            state: PersonPath {
                id: Some(id.to_string()),
            },
            _marker: PhantomData,
        }
    }

    pub fn all() -> Self {
        Self {
            entity_type: dto::EntityType::Person,
            entity_id: String::new(),
            path: format!("{}/", dto::EntityType::Person),
            state: PersonPath { id: None },
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
            state: OfficePath {
                id: Some(id.to_string()),
            },
            _marker: PhantomData,
        }
    }

    pub fn all() -> Self {
        Self {
            entity_type: dto::EntityType::Office,
            entity_id: String::new(),
            path: format!("{}/", dto::EntityType::Office),
            state: OfficePath { id: None },
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
        let bytes = postcard::to_stdvec(value)?;
        self.repo.write(key.path.as_bytes().to_vec(), bytes)?;
        key.update_tables(self.repo.backend.conn, value)?;

        Ok(())
    }

    pub fn load<P, T: for<'de> Deserialize<'de>>(
        &self,
        key: Key<P, T>,
    ) -> Result<Option<T>, RecordRepoError> {
        let value = if let Some(bytes) = self.repo.read(&key.path.as_bytes())? {
            postcard::from_bytes(&bytes)?
        } else {
            None
        };

        Ok(value)
    }

    fn parse_key<P: ParseKeyState, T>(path: &str) -> Result<Key<P, T>, RecordRepoError> {
        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() < 2 {
            return Err(RecordRepoError::InvalidPath(path.to_string()));
        }
        let entity_type = parts[0];
        let entity_id = parts[1];
        let suffix = &parts[2..];

        let state = P::parse(suffix)?;

        Ok(Key {
            entity_type: match entity_type {
                "person" => dto::EntityType::Person,
                "office" => dto::EntityType::Office,
                _ => return Err(RecordRepoError::InvalidPath(format!("Unknown entity type: {}", entity_type))),
            },
            entity_id: entity_id.to_string(),
            path: path.to_string(),
            state,
            _marker: PhantomData,
        })
    }

    pub fn scan<P, T>(
        &self,
        key: Key<P, T>,
    ) -> Result<
        impl Iterator<Item = Result<(RecordKey, RecordValue), RecordRepoError>> + '_,
        RecordRepoError,
    > {
        let prefix = key.path.into_bytes();
        let iter = self.repo.iter_prefix(&prefix)?;

        Ok(iter.map(|item| {
            let (k, v) = item?;
            let path = String::from_utf8(k).map_err(|_| {
                RecordRepoError::Repo(RepoError::Backend("Key is not valid UTF-8".to_string()))
            })?;

            if path.ends_with("/name") {
                let value: String = postcard::from_bytes(&v)?;
                let key = Self::parse_key::<NamePath, String>(&path)?;
                Ok((RecordKey::Name(key), RecordValue::Name(value)))
            } else if path.ends_with("/photo") {
                let value: data::Photo = postcard::from_bytes(&v)?;
                let key = Self::parse_key::<PhotoPath, data::Photo>(&path)?;
                Ok((RecordKey::Photo(key), RecordValue::Photo(value)))
            } else if path.contains("/contact/") {
                let value: String = postcard::from_bytes(&v)?;
                let key = Self::parse_key::<ContactPath, String>(&path)?;
                Ok((RecordKey::Contact(key), RecordValue::Contact(value)))
            } else if path.contains("/supervisor/") {
                let value: String = postcard::from_bytes(&v)?;
                let key = Self::parse_key::<SupervisorPath, String>(&path)?;
                Ok((RecordKey::Supervisor(key), RecordValue::Supervisor(value)))
            } else if path.contains("/tenure/") {
                let value: Option<NaiveDate> = postcard::from_bytes(&v)?;
                let key = Self::parse_key::<TenurePath, Option<NaiveDate>>(&path)?;
                Ok((RecordKey::Tenure(key), RecordValue::Tenure(value)))
            } else {
                Err(RecordRepoError::UnknownRecordType(path))
            }
        }))
    }
    
    pub fn commit_id(&self) -> Result<String, RecordRepoError> {
        Ok(self.repo.commit_id()?)
    }

    pub fn commit(&mut self) -> Result<(), RecordRepoError> {
        Ok(self.repo.commit()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_db(conn: &Connection) {
        conn.execute_batch(r#"
            CREATE TABLE repo (
              hash BLOB NOT NULL PRIMARY KEY,
              blob BLOB NOT NULL
            );
            CREATE TABLE refs (
              name TEXT NOT NULL PRIMARY KEY,
              hash BLOB NOT NULL
            );
            CREATE TABLE entity (
              type TEXT NOT NULL,
              id TEXT NOT NULL,
              name TEXT NOT NULL,
              PRIMARY KEY(type, id)
            );
            CREATE TABLE entity_photo (
              entity_type TEXT NOT NULL,
              entity_id TEXT NOT NULL,
              url TEXT NOT NULL,
              attribution TEXT,
              PRIMARY KEY(entity_type, entity_id)
            );
            CREATE TABLE person_office_tenure (
              person_id TEXT NOT NULL,
              office_id TEXT NOT NULL,
              start TEXT,
              end TEXT
            );
        "#).unwrap();
    }

    #[test]
    fn test_scan() {
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);

        let mut repo = RecordRepo::new(&conn);
        let p1 = Key::<PersonPath, ()>::new("p1");

        repo.save(p1.name(), &"Person One".to_string()).unwrap();

        let photo = data::Photo {
            url: "http://example.com/p1.jpg".to_string(),
            attribution: Some("Attr".to_string()),
        };
        repo.save(p1.photo(), &photo).unwrap();

        let t1 = p1.tenure("o1", None);
        repo.save(t1, &Some(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap())).unwrap();

        let items: Vec<_> = repo.scan(p1)
            .expect("Scan failed")
            .collect::<Result<Vec<_>, _>>()
            .expect("Iteration failed");

        assert_eq!(items.len(), 3);

        let mut found_name = false;
        let mut found_photo = false;
        let mut found_tenure = false;

        for (key, val) in items {
            match (key, val) {
                (RecordKey::Name(k), RecordValue::Name(n)) => {
                    assert_eq!(k.entity_id, "p1");
                    assert_eq!(n, "Person One");
                    found_name = true;
                }
                (RecordKey::Photo(k), RecordValue::Photo(p)) => {
                    assert_eq!(k.entity_id, "p1");
                    assert_eq!(p.url, "http://example.com/p1.jpg");
                    found_photo = true;
                }
                (RecordKey::Tenure(k), RecordValue::Tenure(t)) => {
                    assert_eq!(k.entity_id, "p1");
                    assert_eq!(k.state.office_id, "o1");
                    assert_eq!(t, Some(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()));
                    found_tenure = true;
                }
                _ => panic!("Unexpected type mismatch or unknown type"),
            }
        }

        assert!(found_name);
        assert!(found_photo);
        assert!(found_tenure);
    }
}