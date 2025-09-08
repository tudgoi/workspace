use std::collections::{BTreeMap, HashMap};

use serde_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Person {
    pub name: String,
    pub photo: Option<Photo>,
    pub contacts: Option<BTreeMap<ContactType, String>>,
    pub tenures: Option<Vec<Tenure>>
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Photo {
    pub url: String,
    pub attribution: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ContactType {
    Address,
    Phone,
    Email,
    Website,
    Wikipedia,
    X,
    Youtube,
    Facebook,
    Instagram
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
    pub contacts: Option<BTreeMap<ContactType, String>>,
    pub supervisors: Option<HashMap<SupervisingRelation, String>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SupervisingRelation {
    Adviser,
    DuringThePleasureOf,
    Head,
    ResponsibleTo,
    MemberOf,
}
