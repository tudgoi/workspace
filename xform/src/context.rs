use std::collections::BTreeMap;

use serde_derive::{Deserialize, Serialize};

use crate::data::{self, ContactType};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub title: String,
    pub base_url: String,
    pub source_url: String,
    pub icons: Icons,
    pub defaults: Defaults,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Defaults {
    pub photo: data::Photo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Icons {
    pub address: String,
    pub phone: String,
    pub email: String,
    pub website: String,
    pub wikipedia: String,
    pub x: String,
    pub youtube: String,
    pub facebook: String,
    pub instagram: String,
    pub wikidata: String,
}

impl Icons {
    pub fn for_contact_type(&self, typ: &data::ContactType) -> &str {
        match *typ {
            ContactType::Address => &self.address,
            ContactType::Phone => &self.phone,
            ContactType::Email => &self.email,
            ContactType::Website => &self.website,
            ContactType::Wikipedia => &self.wikipedia,
            ContactType::X => &self.x,
            ContactType::Youtube => &self.youtube,
            ContactType::Facebook => &self.facebook,
            ContactType::Instagram => &self.instagram,
            ContactType::Wikidata => &self.wikidata,
        }
    }
}

#[derive(Serialize, Debug)]
pub struct Quondam {
    pub person: Person,
    pub start: String,
    pub end: String,
}

#[derive(Serialize, Debug)]
pub struct TenureDetails {
    pub office: Office,
    pub start: String,
    pub end: String,
}

#[derive(Serialize, Debug)]
pub struct Person {
    pub id: String,
    pub name: String,
}

#[derive(Serialize, Debug)]
pub struct Office {
    pub id: String,
    pub name: String,
}

#[derive(Serialize, Debug)]
pub struct OfficeDetails {
    pub office: Office,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
    pub supervisors: Option<BTreeMap<data::SupervisingRelation, Officer>>,
    pub subordinates: Option<BTreeMap<data::SupervisingRelation, Vec<Officer>>>,
}

#[derive(Serialize, Debug)]
pub struct Officer {
    pub office_id: String,
    pub office_name: String,
    pub person: Option<Person>,
}

#[derive(Serialize, Debug)]
pub struct Page {
    pub base: String,
    pub dynamic: bool,
}

#[derive(Serialize, Debug)]
pub struct Metadata {
    pub commit_date: Option<String>,
    pub maintenance: Maintenance,
}

#[derive(Serialize, Debug)]
pub struct Maintenance {
    pub incomplete: bool,
}
