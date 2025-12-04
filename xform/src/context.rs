use std::collections::BTreeMap;

use serde_derive::{Deserialize, Serialize};

use crate::data;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub title: String,
    pub base_url: String,
    pub source_url: String,
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
    pub minister: String,
}

#[derive(Serialize, Debug)]
pub struct ChangesContext {
    pub changes: Vec<Person>,
    pub config: Config,
    pub page: Page,   
}

#[derive(Serialize, Debug)]
pub struct PersonContext {
    pub person: Person,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
    pub offices: Option<Vec<OfficeDetails>>,
    pub past_tenures: Option<Vec<TenureDetails>>,

    pub config: Config,
    pub page: Page,
    pub metadata: Metadata,
}

#[derive(Serialize, Debug)]
pub struct OfficeContext {
    pub office: Office,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
    pub incumbent: Option<Person>,
    pub quondams: Option<Vec<Quondam>>,

    pub config: Config,
    pub page: Page,
    pub metadata: Metadata,
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