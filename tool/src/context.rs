use serde_derive::{Deserialize, Serialize};

use crate::dto;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub title: String,
    pub base_url: String,
    pub icons: Icons,
    pub labels: Labels,
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
pub struct PersonContext {
    pub person: Person,
    pub photo: Option<Photo>,
    pub office_photo: Option<Photo>,
    pub office: Option<Office>,
    pub contacts: Option<Contacts>,
    pub official_contacts: Option<Contacts>,
    pub supervisors: Option<Supervisors>,
    pub subordinates: Option<Subordinates>,

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
pub struct Photo {
    pub url: String,
    pub attribution: String
}

#[derive(Serialize, Debug)]
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

#[derive(Serialize, Debug)]
pub struct Office {
    pub id: String,
    pub name: String   
}

#[derive(Serialize, Debug)]
pub struct Supervisors {
    pub adviser: Option<Officer>,
    pub during_the_pleasure_of: Option<Officer>,
    pub head: Option<Officer>,
    pub responsible_to: Option<Officer>,
}

#[derive(Serialize, Debug)]
pub struct Subordinates {
    pub adviser: Vec<Officer>,
    pub during_the_pleasure_of: Vec<Officer>,
    pub head: Vec<Officer>,
    pub responsible_to: Vec<Officer>,
}

#[derive(Serialize, Debug)]
pub struct Officer {
    pub office: Office,
    pub person: Person,
}

impl From<dto::Officer> for Officer {
    fn from(value: dto::Officer) -> Self {
        Officer {
            office: Office {
                id: value.office.id,
                name: value.office.data.name,
            },
            person: Person {
                id: value.person.id,
                name: value.person.data.name,
            },
        }
    }
}

#[derive(Serialize, Debug)]
pub struct Page {
    pub path: String,
    pub updated: String
}

#[derive(Serialize, Debug)]
pub struct Metadata {
    pub incomplete: bool,
    pub updated: String
}