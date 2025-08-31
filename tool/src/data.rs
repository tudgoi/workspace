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
    pub attribution: String
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
    pub start: String,
    pub end: Option<String>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Office {
    pub name: String,
    pub supervisors: Option<Supervisors>,
    pub contacts: Option<Contacts>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Supervisors {
    pub adviser: Option<String>,
    pub during_the_pleasure_of: Option<String>,
}