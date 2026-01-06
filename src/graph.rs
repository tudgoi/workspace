use std::collections::HashMap;

use serde::Serialize;

use crate::data;

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Person,
    Office,
}

#[derive(Debug, Clone)]
pub enum Property {
    Type(EntityType),
    Id(String),
    Name(String),
    Tenure(Vec<Property>),
    Photo {
        url: String,
        attribution: Option<String>,
    },
    Contact(data::ContactType, String),
    Supervisor(data::SupervisingRelation, Vec<Property>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Key {
    Type,
    Id,
    Name,
    Tenure,
    Photo,
    MemberOf,
    DuringThePleasureOf,
    Address,
    Phone,
    Email,
    Website,
    Wikipedia,
    X,
    Youtube,
    Facebook,
    Instagram,
    Wikidata,
    Head,
    Adviser,
    ResponsibleTo,
    Minister,
}

impl Property {
    /// Return the key (discriminant) for this property
    pub fn key(&self) -> Key {
        match self {
            Property::Type(_) => Key::Type,
            Property::Id(_) => Key::Id,
            Property::Name(_) => Key::Name,
            Property::Tenure(_) => Key::Tenure,
            Property::Photo { .. } => Key::Photo,
            Property::Contact(contact_type, _) => match contact_type {
                data::ContactType::Address => Key::Address,
                data::ContactType::Phone => Key::Phone,
                data::ContactType::Email => Key::Email,
                data::ContactType::Website => Key::Website,
                data::ContactType::Wikipedia => Key::Wikipedia,
                data::ContactType::X => Key::X,
                data::ContactType::Youtube => Key::Youtube,
                data::ContactType::Facebook => Key::Facebook,
                data::ContactType::Instagram => Key::Instagram,
                data::ContactType::Wikidata => Key::Wikidata,
            },
            Property::Supervisor(relation, _) => match relation {
                data::SupervisingRelation::MemberOf => Key::MemberOf,
                data::SupervisingRelation::Head => Key::Head,
                data::SupervisingRelation::Adviser => Key::Adviser,
                data::SupervisingRelation::DuringThePleasureOf => Key::DuringThePleasureOf,
                data::SupervisingRelation::ResponsibleTo => Key::ResponsibleTo,
                &data::SupervisingRelation::Minister => Key::Minister,
            },
        }
    }
}

#[derive(Debug)]
pub struct Entity(pub HashMap<Key, Property>);

impl From<Vec<Property>> for Entity {
    fn from(value: Vec<Property>) -> Self {
        let map = value
            .into_iter()
            .map(|property| (property.key(), property))
            .collect();
        Entity(map)
    }
}

impl Entity {
    pub fn get_type(&self) -> Option<&EntityType> {
        if let Some(Property::Type(entity_type)) = self.0.get(&Key::Type) {
            Some(entity_type)
        } else {
            None
        }
    }

    pub fn get_id(&self) -> Option<&str> {
        if let Some(Property::Id(id)) = self.0.get(&Key::Id) {
            Some(id)
        } else {
            None
        }
    }

    pub fn get_name(&self) -> Option<&str> {
        if let Some(Property::Name(name)) = self.0.get(&Key::Name) {
            Some(name)
        } else {
            None
        }
    }
}
