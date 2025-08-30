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
    pub elected_by: String,
    
    pub advises: String,
    pub during_their_pleasure: String,
    pub heads: String,
    pub members: String,
    pub under_their_responsibility: String,
    pub elected_by_them: String,
}

#[derive(Serialize, Debug)]
pub struct PersonContext {
    pub person: Person,
    pub photo: Option<Photo>,
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
    pub adviser: Option<Officer>
}

#[derive(Serialize, Debug)]
pub struct Subordinates {
    pub advises: Vec<Officer>
}

#[derive(Serialize, Debug)]
pub struct Officer {
    pub office: Office,
    pub person: Person,
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