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

impl Icons {
    pub fn for_contact_type(&self, typ: &data::ContactType) -> &str {
        match typ {
            &ContactType::Address => &self.address,
            &ContactType::Phone => &self.phone,
            &ContactType::Email => &self.email,
            &ContactType::Website => &self.website,
            &ContactType::Wikipedia => &self.wikipedia,
            &ContactType::X => &self.x,
            &ContactType::Youtube => &self.youtube,
            &ContactType::Facebook => &self.facebook,
            &ContactType::Instagram => &self.instagram,
            &ContactType::Wikidata => &self.wikidata,
        }
    }
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

impl SupervisorsLabels {
    pub fn for_relation(&self, relation: &data::SupervisingRelation) -> &str {
        match relation {
            data::SupervisingRelation::Adviser => &self.adviser,
            data::SupervisingRelation::DuringThePleasureOf => &self.during_the_pleasure_of,
            data::SupervisingRelation::Head => &self.head,
            data::SupervisingRelation::MemberOf => &self.member_of,
            data::SupervisingRelation::ResponsibleTo => &self.responsible_to,
            data::SupervisingRelation::Minister => &self.minister,
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
