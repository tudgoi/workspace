use std::{collections::BTreeMap, str::FromStr, fmt};

use rusqlite::ToSql;
use serde::Serialize;

use crate::{data, graph};

#[derive(Debug)]
pub struct Person {
    pub id: String,
    pub name: String,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
    pub commit_date: Option<String>,
}

#[derive(Debug)]
pub struct Office {
    pub id: String,
    pub name: String,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
}

#[derive(Debug)]
pub struct Entity {
    pub entity_type: EntityType,
    pub id: String,
    pub name: String,
}

#[derive(Debug, Serialize, Eq, PartialEq, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Person,
    Office,
}

impl EntityType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EntityType::Person => "person",
            EntityType::Office => "office",
        }
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl ToSql for EntityType {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.as_str().into())
    }
}

impl FromStr for EntityType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "person" => Ok(EntityType::Person),
            "office" => Ok(EntityType::Office),
            _ => Err(format!("'{}' is not a valid EntityType", s)),
        }
    }
}

impl From<graph::EntityType> for EntityType {
    fn from(value: graph::EntityType) -> Self {
        match value {
            graph::EntityType::Person => EntityType::Person,
            graph::EntityType::Office => EntityType::Office,
        }
    }
}