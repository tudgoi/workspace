pub mod sqlitebe;

use crate::{WriteSql, repo::RepoRef};
use chrono::NaiveDate;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::str::FromStr;
use thiserror::Error;

use crate::{
    data, dto,
    repo::{Hash, Repo, RepoError, RepoRefType},
};
use sqlitebe::{SqliteBackend, SqliteBackendError};

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

impl From<SqliteBackendError> for RecordRepoError {
    fn from(e: SqliteBackendError) -> Self {
        RecordRepoError::Repo(RepoError::backend(e))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum RecordValue {
    Name(String),
    Photo(data::Photo),
    Contact(String),
    Supervisor(String),
    Tenure(Option<NaiveDate>),
}

impl std::fmt::Display for RecordValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordValue::Name(v) => write!(f, "{}", v),
            RecordValue::Photo(v) => write!(f, "Photo({})", v.url),
            RecordValue::Contact(v) => write!(f, "{}", v),
            RecordValue::Supervisor(v) => write!(f, "{}", v),
            RecordValue::Tenure(v) => {
                if let Some(date) = v {
                    write!(f, "{}", date)
                } else {
                    write!(f, "Present")
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecordKey {
    Name(Key<NamePath, String>),
    Photo(Key<PhotoPath, data::Photo>),
    Contact(Key<ContactPath, String>),
    Supervisor(Key<SupervisorPath, String>),
    Tenure(Key<TenurePath, Option<NaiveDate>>),
}

impl RecordKey {
    pub fn path(&self) -> &str {
        match self {
            RecordKey::Name(k) => &k.path,
            RecordKey::Photo(k) => &k.path,
            RecordKey::Contact(k) => &k.path,
            RecordKey::Supervisor(k) => &k.path,
            RecordKey::Tenure(k) => &k.path,
        }
    }

    pub fn field(&self) -> String {
        let path = self.path();
        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() > 2 {
            parts[2..].join("/")
        } else {
            path.to_string()
        }
    }

    pub fn entity_info(&self) -> (dto::EntityType, String) {
        match self {
            RecordKey::Name(k) => (k.entity_type, k.entity_id.clone()),
            RecordKey::Photo(k) => (k.entity_type, k.entity_id.clone()),
            RecordKey::Contact(k) => (k.entity_type, k.entity_id.clone()),
            RecordKey::Supervisor(k) => (k.entity_type, k.entity_id.clone()),
            RecordKey::Tenure(k) => (k.entity_type, k.entity_id.clone()),
        }
    }

    pub fn update_index(
        &self,
        conn: &Connection,
        value: &RecordValue,
    ) -> Result<(), RecordRepoError> {
        match (self, value) {
            (RecordKey::Name(k), RecordValue::Name(v)) => k.update_index(conn, v),
            (RecordKey::Photo(k), RecordValue::Photo(v)) => k.update_index(conn, v),
            (RecordKey::Contact(k), RecordValue::Contact(v)) => k.update_index(conn, v),
            (RecordKey::Supervisor(k), RecordValue::Supervisor(v)) => k.update_index(conn, v),
            (RecordKey::Tenure(k), RecordValue::Tenure(v)) => k.update_index(conn, v),
            _ => Err(RecordRepoError::InvalidPath(
                "Key/Value type mismatch".to_string(),
            )),
        }
    }

    pub fn delete_index(&self, conn: &Connection) -> Result<(), RecordRepoError> {
        match self {
            RecordKey::Name(k) => k.delete_index(conn),
            RecordKey::Photo(k) => k.delete_index(conn),
            RecordKey::Contact(k) => k.delete_index(conn),
            RecordKey::Supervisor(k) => k.delete_index(conn),
            RecordKey::Tenure(k) => k.delete_index(conn),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecordDiff {
    Added(RecordKey, RecordValue),
    Changed(RecordKey, RecordValue, RecordValue), // key, old_value, new_value
    Removed(RecordKey, RecordValue),
}

impl RecordDiff {
    pub fn key(&self) -> &RecordKey {
        match self {
            RecordDiff::Added(k, _) => k,
            RecordDiff::Changed(k, _, _) => k,
            RecordDiff::Removed(k, _) => k,
        }
    }
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
            return Err(RecordRepoError::InvalidPath(format!(
                "Invalid contact path: {:?}",
                parts
            )));
        }
        let typ = data::ContactType::from_str(parts[1]).map_err(|_| {
            RecordRepoError::InvalidPath(format!("Invalid contact type: {}", parts[1]))
        })?;
        Ok(ContactPath { typ })
    }
}

impl ParseKeyState for SupervisorPath {
    fn parse(parts: &[&str]) -> Result<Self, RecordRepoError> {
        if parts.len() != 2 || parts[0] != "supervisor" {
            return Err(RecordRepoError::InvalidPath(format!(
                "Invalid supervisor path: {:?}",
                parts
            )));
        }
        let relation = data::SupervisingRelation::from_str(parts[1]).map_err(|_| {
            RecordRepoError::InvalidPath(format!("Invalid supervisor relation: {}", parts[1]))
        })?;
        Ok(SupervisorPath { relation })
    }
}

impl ParseKeyState for TenurePath {
    fn parse(parts: &[&str]) -> Result<Self, RecordRepoError> {
        if parts.len() != 3 || parts[0] != "tenure" {
            return Err(RecordRepoError::InvalidPath(format!(
                "Invalid tenure path: {:?}",
                parts
            )));
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

pub trait ValueIndexer<T> {
    fn update_index(&self, conn: &Connection, value: &T) -> Result<(), RecordRepoError>;
    fn delete_index(&self, _conn: &Connection) -> Result<(), RecordRepoError> {
        Ok(())
    }
}

impl ValueIndexer<String> for Key<NamePath, String> {
    fn update_index(&self, conn: &Connection, value: &String) -> Result<(), RecordRepoError> {
        conn.save_entity_name(&self.entity_type, &self.entity_id, value)?;
        Ok(())
    }
    fn delete_index(&self, conn: &Connection) -> Result<(), RecordRepoError> {
        conn.delete_entity(&self.entity_type, &self.entity_id)?;
        Ok(())
    }
}

impl ValueIndexer<data::Photo> for Key<PhotoPath, data::Photo> {
    fn update_index(&self, conn: &Connection, value: &data::Photo) -> Result<(), RecordRepoError> {
        conn.save_entity_photo(
            &self.entity_type,
            &self.entity_id,
            &value.url,
            value.attribution.as_deref(),
        )?;
        Ok(())
    }
    fn delete_index(&self, conn: &Connection) -> Result<(), RecordRepoError> {
        conn.delete_entity_photo(&self.entity_type, &self.entity_id)?;
        Ok(())
    }
}

impl ValueIndexer<String> for Key<ContactPath, String> {
    fn update_index(&self, conn: &Connection, value: &String) -> Result<(), RecordRepoError> {
        conn.save_entity_contact(&self.entity_type, &self.entity_id, &self.state.typ, value)?;
        Ok(())
    }
    fn delete_index(&self, conn: &Connection) -> Result<(), RecordRepoError> {
        conn.delete_entity_contact(&self.entity_type, &self.entity_id, &self.state.typ)?;
        Ok(())
    }
}

impl ValueIndexer<String> for Key<SupervisorPath, String> {
    fn update_index(&self, conn: &Connection, value: &String) -> Result<(), RecordRepoError> {
        conn.save_office_supervisor(&self.entity_id, &self.state.relation, value)?;
        Ok(())
    }
    fn delete_index(&self, conn: &Connection) -> Result<(), RecordRepoError> {
        conn.delete_office_supervisor(&self.entity_id, &self.state.relation)?;
        Ok(())
    }
}

impl ValueIndexer<Option<NaiveDate>> for Key<TenurePath, Option<NaiveDate>> {
    fn update_index(
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
    fn delete_index(&self, conn: &Connection) -> Result<(), RecordRepoError> {
        conn.delete_tenure(
            &self.entity_id,
            &self.state.office_id,
            self.state.start.as_ref(),
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

    pub fn working(&self) -> Result<RecordRepoRef<'_, 'a>, RecordRepoError> {
        Ok(RecordRepoRef {
            repo_ref: self.repo.get_ref(RepoRefType::Working)?,
        })
    }

    pub fn committed(&self) -> Result<RecordRepoRef<'_, 'a>, RecordRepoError> {
        Ok(RecordRepoRef {
            repo_ref: self.repo.get_ref(RepoRefType::Committed)?,
        })
    }

    pub fn commit(&mut self) -> Result<(), RecordRepoError> {
        Ok(self.repo.commit()?)
    }

    pub fn abandon(&mut self) -> Result<(), RecordRepoError> {
        Ok(self.repo.abandon()?)
    }

    pub fn init(&self) -> Result<(), RecordRepoError> {
        Ok(self.repo.init()?)
    }

    pub fn get_at(&self, hash: &Hash) -> Result<RecordRepoRef<'_, 'a>, RecordRepoError> {
        Ok(RecordRepoRef {
            repo_ref: RepoRef {
                repo: &self.repo,
                hash: hash.clone(),
                name: "detached".to_string(),
            },
        })
    }

    pub fn iterate_diff(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<RecordDiff, RecordRepoError>> + '_>, RecordRepoError>
    {
        let working = self.working()?;
        let committed = self.committed()?;

        let diffs: Vec<Result<RecordDiff, RecordRepoError>> =
            committed.iterate_diff(&working)?.collect();
        Ok(Box::new(diffs.into_iter()))
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
                _ => {
                    return Err(RecordRepoError::InvalidPath(format!(
                        "Unknown entity type: {}",
                        entity_type
                    )));
                }
            },
            entity_id: entity_id.to_string(),
            path: path.to_string(),
            state,
            _marker: PhantomData,
        })
    }
}

pub struct RecordRepoRef<'a, 'b> {
    repo_ref: crate::repo::RepoRef<'a, SqliteBackend<'b>>,
}

impl<'a, 'b> RecordRepoRef<'a, 'b> {
    pub fn save<P, T: Serialize>(
        &mut self,
        key: Key<P, T>,
        value: &T,
    ) -> Result<(), RecordRepoError>
    where
        Key<P, T>: ValueIndexer<T>,
    {
        let bytes = postcard::to_stdvec(value)?;
        self.repo_ref.write(key.path.as_bytes().to_vec(), bytes)?;
        key.update_index(self.repo_ref.repo.backend.conn, value)?;

        Ok(())
    }

    pub fn delete<P, T>(&mut self, key: Key<P, T>) -> Result<(), RecordRepoError>
    where
        Key<P, T>: ValueIndexer<T>,
    {
        self.repo_ref.remove(key.path.as_bytes())?;
        key.delete_index(self.repo_ref.repo.backend.conn)?;

        Ok(())
    }

    pub fn load<P, T: for<'de> Deserialize<'de>>(
        &self,
        key: Key<P, T>,
    ) -> Result<Option<T>, RecordRepoError> {
        let value = if let Some(bytes) = self.repo_ref.read(key.path.as_bytes())? {
            postcard::from_bytes(&bytes)?
        } else {
            None
        };

        Ok(value)
    }

    pub fn get(&self, path: &str) -> Result<Option<RecordValue>, RecordRepoError> {
        if let Some(bytes) = self.repo_ref.read(path.as_bytes())? {
            let (_, value) = self.parse_record(path, &bytes)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub fn save_from_json(&mut self, path: &str, json: &str) -> Result<(), RecordRepoError> {
        if path.ends_with("/name") {
            let value: String = serde_json::from_str(json)
                .map_err(|e| RecordRepoError::InvalidPath(e.to_string()))?;
            let key = RecordRepo::parse_key::<NamePath, String>(path)?;
            self.save(key, &value)
        } else if path.ends_with("/photo") {
            let value: data::Photo = serde_json::from_str(json)
                .map_err(|e| RecordRepoError::InvalidPath(e.to_string()))?;
            let key = RecordRepo::parse_key::<PhotoPath, data::Photo>(path)?;
            self.save(key, &value)
        } else if path.contains("/contact/") {
            let value: String = serde_json::from_str(json)
                .map_err(|e| RecordRepoError::InvalidPath(e.to_string()))?;
            let key = RecordRepo::parse_key::<ContactPath, String>(path)?;
            self.save(key, &value)
        } else if path.contains("/supervisor/") {
            let value: String = serde_json::from_str(json)
                .map_err(|e| RecordRepoError::InvalidPath(e.to_string()))?;
            let key = RecordRepo::parse_key::<SupervisorPath, String>(path)?;
            self.save(key, &value)
        } else if path.contains("/tenure/") {
            let value: Option<NaiveDate> = serde_json::from_str(json)
                .map_err(|e| RecordRepoError::InvalidPath(e.to_string()))?;
            let key = RecordRepo::parse_key::<TenurePath, Option<NaiveDate>>(path)?;
            self.save(key, &value)
        } else {
            Err(RecordRepoError::UnknownRecordType(path.to_string()))
        }
    }

    pub fn delete_path(&mut self, path: &str) -> Result<(), RecordRepoError> {
        if path.ends_with("/name") {
            let key = RecordRepo::parse_key::<NamePath, String>(path)?;
            self.delete(key)
        } else if path.ends_with("/photo") {
            let key = RecordRepo::parse_key::<PhotoPath, data::Photo>(path)?;
            self.delete(key)
        } else if path.contains("/contact/") {
            let key = RecordRepo::parse_key::<ContactPath, String>(path)?;
            self.delete(key)
        } else if path.contains("/supervisor/") {
            let key = RecordRepo::parse_key::<SupervisorPath, String>(path)?;
            self.delete(key)
        } else if path.contains("/tenure/") {
            let key = RecordRepo::parse_key::<TenurePath, Option<NaiveDate>>(path)?;
            self.delete(key)
        } else {
            Err(RecordRepoError::UnknownRecordType(path.to_string()))
        }
    }

    pub fn scan<P, T>(
        &self,
        key: Key<P, T>,
    ) -> Result<
        impl Iterator<Item = Result<(RecordKey, RecordValue), RecordRepoError>> + '_,
        RecordRepoError,
    > {
        let prefix_bytes = key.path.as_bytes().to_vec();
        let iter = self.repo_ref.iter_prefix(&prefix_bytes)?;

        Ok(iter.map(|item| {
            let (k, v) = item?;
            let path = String::from_utf8(k).map_err(|_| {
                RecordRepoError::Repo(RepoError::HashParse("Key is not valid UTF-8".to_string()))
            })?;

            self.parse_record(&path, &v)
        }))
    }

    pub fn list(
        &self,
        prefix: &str,
    ) -> Result<
        impl Iterator<Item = Result<(String, RecordValue), RecordRepoError>> + '_,
        RecordRepoError,
    > {
        let prefix_bytes = prefix.as_bytes().to_vec();
        let iter = self.repo_ref.iter_prefix(&prefix_bytes)?;

        Ok(iter.map(|item| {
            let (k, v) = item?;
            let path = String::from_utf8(k).map_err(|_| {
                RecordRepoError::Repo(RepoError::HashParse("Key is not valid UTF-8".to_string()))
            })?;

            let (_, value) = self.parse_record(&path, &v)?;
            Ok((path, value))
        }))
    }

    pub fn commit_id(&self) -> Result<Hash, RecordRepoError> {
        Ok(self.repo_ref.commit_id()?)
    }

    pub fn iterate_diff(
        &self,
        other: &RecordRepoRef<'a, 'b>,
    ) -> Result<impl Iterator<Item = Result<RecordDiff, RecordRepoError>> + '_, RecordRepoError>
    {
        use crate::repo::Diff;
        let iter = self.repo_ref.iterate_diff(&other.repo_ref)?;

        Ok(iter.map(|item| {
            let diff = item?;
            match diff {
                Diff::Added(k, v) => {
                    let path = String::from_utf8(k).map_err(|_| {
                        RecordRepoError::Repo(RepoError::HashParse(
                            "Key is not valid UTF-8".to_string(),
                        ))
                    })?;
                    let (rk, rv) = self.parse_record(&path, &v)?;
                    Ok(RecordDiff::Added(rk, rv))
                }
                Diff::Changed(k, old_v, new_v) => {
                    let path = String::from_utf8(k).map_err(|_| {
                        RecordRepoError::Repo(RepoError::HashParse(
                            "Key is not valid UTF-8".to_string(),
                        ))
                    })?;
                    let (rk, rv_old) = self.parse_record(&path, &old_v)?;
                    let (_, rv_new) = self.parse_record(&path, &new_v)?;
                    Ok(RecordDiff::Changed(rk, rv_old, rv_new))
                }
                Diff::Removed(k, v) => {
                    let path = String::from_utf8(k).map_err(|_| {
                        RecordRepoError::Repo(RepoError::HashParse(
                            "Key is not valid UTF-8".to_string(),
                        ))
                    })?;
                    let (rk, rv) = self.parse_record(&path, &v)?;
                    Ok(RecordDiff::Removed(rk, rv))
                }
            }
        }))
    }

    fn parse_record(
        &self,
        path: &str,
        v: &[u8],
    ) -> Result<(RecordKey, RecordValue), RecordRepoError> {
        if path.ends_with("/name") {
            let value: String = postcard::from_bytes(v)?;
            let key = RecordRepo::parse_key::<NamePath, String>(path)?;
            Ok((RecordKey::Name(key), RecordValue::Name(value)))
        } else if path.ends_with("/photo") {
            let value: data::Photo = postcard::from_bytes(v)?;
            let key = RecordRepo::parse_key::<PhotoPath, data::Photo>(path)?;
            Ok((RecordKey::Photo(key), RecordValue::Photo(value)))
        } else if path.contains("/contact/") {
            let value: String = postcard::from_bytes(v)?;
            let key = RecordRepo::parse_key::<ContactPath, String>(path)?;
            Ok((RecordKey::Contact(key), RecordValue::Contact(value)))
        } else if path.contains("/supervisor/") {
            let value: String = postcard::from_bytes(v)?;
            let key = RecordRepo::parse_key::<SupervisorPath, String>(path)?;
            Ok((RecordKey::Supervisor(key), RecordValue::Supervisor(value)))
        } else if path.contains("/tenure/") {
            let value: Option<NaiveDate> = postcard::from_bytes(v)?;
            let key = RecordRepo::parse_key::<TenurePath, Option<NaiveDate>>(path)?;
            Ok((RecordKey::Tenure(key), RecordValue::Tenure(value)))
        } else {
            Err(RecordRepoError::UnknownRecordType(path.to_string()))
        }
    }
}

pub fn abandon_changes(conn: &mut Connection) -> Result<(), RecordRepoError> {
    let mut repo = RecordRepo::new(conn);
    let old_hash = repo.working()?.commit_id()?;

    repo.abandon()?;

    let diffs = {
        let new_working = repo.working()?;
        let new_hash = new_working.commit_id()?;

        if old_hash != new_hash {
            let old_working = repo
                .get_at(&old_hash)
                .map_err(|e| RecordRepoError::Repo(RepoError::HashParse(e.to_string())))?;

            old_working
                .iterate_diff(&new_working)?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        }
    };

    if !diffs.is_empty() {
        let mut diffs = diffs;
        diffs.sort_by_key(|diff| {
            match diff {
                RecordDiff::Added(RecordKey::Name(_), _)
                | RecordDiff::Changed(RecordKey::Name(_), _, _) => 0,

                RecordDiff::Added(_, _) | RecordDiff::Changed(_, _, _) => 1,

                RecordDiff::Removed(RecordKey::Name(_), _) => 3,

                RecordDiff::Removed(_, _) => 2,
            }
        });

        let tx = conn.transaction()?;
        for diff in diffs {
            match diff {
                RecordDiff::Added(k, v) => k.update_index(&tx, &v)?,
                RecordDiff::Changed(k, _, v) => k.update_index(&tx, &v)?,
                RecordDiff::Removed(k, _) => k.delete_index(&tx)?,
            }
        }
        tx.commit()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_db(conn: &Connection) {
        conn.execute_batch(
            r#"
            CREATE TABLE repo (
              hash BLOB NOT NULL PRIMARY KEY,
              blob BLOB NOT NULL
            );
            CREATE TABLE refs (
              name TEXT NOT NULL PRIMARY KEY,
              hash BLOB NOT NULL
            );
            CREATE TABLE secrets (
              name TEXT NOT NULL PRIMARY KEY,
              value BLOB NOT NULL
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
        "#,
        )
        .unwrap();
    }

    #[test]
    fn test_scan() {
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);

        let repo = RecordRepo::new(&conn);
        repo.init().unwrap();
        let p1 = Key::<PersonPath, ()>::new("p1");

        repo.working()
            .unwrap()
            .save(p1.name(), &"Person One".to_string())
            .unwrap();

        let photo = data::Photo {
            url: "http://example.com/p1.jpg".to_string(),
            attribution: Some("Attr".to_string()),
        };
        repo.working().unwrap().save(p1.photo(), &photo).unwrap();

        let t1 = p1.tenure("o1", None);
        repo.working()
            .unwrap()
            .save(t1, &Some(NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()))
            .unwrap();

        let items: Vec<_> = repo
            .working()
            .unwrap()
            .scan(p1)
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

    #[test]
    fn test_list() {
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);

        let repo = RecordRepo::new(&conn);
        repo.init().unwrap();
        let p1 = Key::<PersonPath, ()>::new("p1");

        repo.working()
            .unwrap()
            .save(p1.name(), &"Person One".to_string())
            .unwrap();

        let items: Vec<_> = repo
            .working()
            .unwrap()
            .list("person/p1/")
            .expect("List failed")
            .collect::<Result<Vec<_>, _>>()
            .expect("Iteration failed");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, "person/p1/name");
        assert_eq!(items[0].1, RecordValue::Name("Person One".to_string()));
    }

    #[test]
    fn test_iterate_diff() {
        let conn = Connection::open_in_memory().unwrap();
        setup_db(&conn);

        let mut repo = RecordRepo::new(&conn);
        repo.init().unwrap();
        let p1 = Key::<PersonPath, ()>::new("p1");
        repo.working()
            .unwrap()
            .save(p1.name(), &"Person One".to_string())
            .unwrap();
        repo.commit().unwrap();

        repo.working()
            .unwrap()
            .save(p1.name(), &"Person One Updated".to_string())
            .unwrap();
        let p2 = Key::<PersonPath, ()>::new("p2");
        repo.working()
            .unwrap()
            .save(p2.name(), &"Person Two".to_string())
            .unwrap();

        let diffs: Vec<_> = repo
            .iterate_diff()
            .expect("Diff failed")
            .collect::<Result<Vec<_>, _>>()
            .expect("Iteration failed");
        assert_eq!(diffs.len(), 2);
        let mut found_added = false;
        let mut found_changed = false;

        for diff in diffs {
            match diff {
                RecordDiff::Added(RecordKey::Name(k), RecordValue::Name(v)) => {
                    assert_eq!(k.entity_id, "p2");
                    assert_eq!(v, "Person Two");
                    found_added = true;
                }
                RecordDiff::Changed(
                    RecordKey::Name(k),
                    RecordValue::Name(old_v),
                    RecordValue::Name(new_v),
                ) => {
                    assert_eq!(k.entity_id, "p1");
                    assert_eq!(old_v, "Person One");
                    assert_eq!(new_v, "Person One Updated");
                    found_changed = true;
                }
                _ => panic!("Unexpected diff type"),
            }
        }
        assert!(found_added);
        assert!(found_changed);
    }
}
