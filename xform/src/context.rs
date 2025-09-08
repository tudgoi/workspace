use std::collections::BTreeMap;

use serde_derive::{Deserialize, Serialize};

use crate::data;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub title: String,
    pub base_url: String,
    pub icons: Icons,
    pub labels: Labels,
    pub defaults: Defaults,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Defaults {
    pub photo: data::Photo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Icons {
    pub phone: String,
    pub email: String,
    pub website: String,
    pub wikipedia: String,
    pub x: String,
    pub facebook: String,
    pub instagram: String,
    pub youtube: String,
    pub address: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Labels {
    pub supervisors: SupervisorsLabels,
    pub subordinates: SupervisorsLabels,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SupervisorsLabels {
    pub adviser: String,
    pub during_the_pleasure_of: String,
    pub head: String,
    pub member_of: String,
    pub responsible_to: String,
    pub elected_by: String,
}

#[derive(Serialize, Debug)]
pub struct IndexContext {
    pub persons: u32,
    pub offices: u32,
    pub config: Config,
    pub page: Page,   
}

#[derive(Serialize, Debug)]
pub struct PersonContext {
    pub person: Person,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
    pub offices: Option<Vec<Office>>,

    pub config: Config,
    pub page: Page,
    pub metadata: Metadata,
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
    pub path: String,
}

#[derive(Serialize, Debug)]
pub struct Metadata {
    pub updated: String,
    pub maintenance: Maintenance,
}

#[derive(Serialize, Debug)]
pub struct Maintenance {
    pub incomplete: bool,
}