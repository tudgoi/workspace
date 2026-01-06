use std::collections::BTreeMap;

use serde_derive::Serialize;

use crate::data::{self};

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
