use std::collections::HashMap;

use serde_derive::{Serialize, Deserialize};
use serde_with::skip_serializing_none;

#[derive(Serialize, Deserialize, Debug)]
pub struct Person {
    pub name: String,
    pub photo: Option<Photo>,
    pub contacts: Option<Contacts>,
    pub tenures: Option<Vec<Tenure>>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Photo {
    pub url: String,
    pub attribution: Option<String>,
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize, Debug)]
pub struct Contacts {
    pub phone: Option<String>,
    pub email: Option<String>,
    pub website: Option<String>,
    pub wikipedia: Option<String>,
    pub x: Option<String>,
    pub facebook: Option<String>,
    pub instagram: Option<String>,
    pub youtube: Option<String>,
    pub address: Option<String>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Tenure {
    pub office: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub additional_charge: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Office {
    pub name: String,
    pub photo: Option<Photo>,
    pub supervisors: Option<HashMap<Supervisor, String>>,
    pub contacts: Option<Contacts>
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Supervisor {
    Adviser,
    DuringThePleasureOf,
    Head,
    ResponsibleTo,
    MemberOf,
}