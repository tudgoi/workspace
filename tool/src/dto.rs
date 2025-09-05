use crate::data;

#[derive(Debug)]
pub struct Person {
    pub id: String,
    pub data: data::Person,
    pub updated: String,
}

#[derive(Debug)]
pub struct Office {
    pub id: String,
    pub data: data::Office
}

#[derive(Debug)]
pub struct PersonOffice {
    pub person: Person,
    pub offices: Option<Vec<Office>>,
}

#[derive(Debug)]
pub struct Officer {
    pub office_id: String,
    pub office_name: String,
    pub person_id: Option<String> ,
    pub person_name: Option<String>,
}

#[derive(Debug)]
pub struct Counts {
    pub persons: u32,
    pub offices: u32,
}