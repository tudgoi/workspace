use std::collections::BTreeMap;

use serde::Serialize;

use crate::{data, graph};

#[derive(Debug)]
pub struct Office {
    pub id: String,
    pub name: String,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
}


#[derive(Debug)]
pub struct Counts {
    pub persons: u32,
    pub offices: u32,
}

#[derive(Debug)]
pub struct Entity {
    pub entity_type: EntityType,
    pub id: String,
    pub name: String,
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Person,
    Office,
}

impl From<graph::EntityType> for EntityType {
    fn from(value: graph::EntityType) -> Self {
        match value {
            graph::EntityType::Person => EntityType::Person,
            graph::EntityType::Office => EntityType::Office,
        }
    }
}