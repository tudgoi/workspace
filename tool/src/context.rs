use serde_derive::{Deserialize, Serialize};

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
    pub adviser: String,
    pub during_the_pleasure_of: String,
    pub head: String,
    pub member_of: String,
    pub responsible_to: String,
    pub elected_by: String
}

#[derive(Serialize, Debug)]
pub struct PersonContext {
    pub person: Person,
    pub photo: Option<Photo>,
    pub office: Option<Office>,
    pub supervisors: Option<Supervisors>,

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
pub struct Office {
    pub id: String,
    pub name: String   
}

#[derive(Serialize, Debug)]
pub struct Supervisors {
    pub adviser: Option<Officer>
}

#[derive(Serialize, Debug)]
pub struct Officer {
    pub office: Office,
    pub person: Option<Person>,
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