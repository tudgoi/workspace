use super::data;

#[derive(Debug)]
pub struct Person {
    pub id: String,
    pub data: data::Person,
}

#[derive(Debug)]
pub struct Office {
    pub id: String,
    pub data: data::Office
}

#[derive(Debug)]
pub struct PersonOffice {
    pub person: Person,
    pub office: Option<Office>
}

#[derive(Debug)]
pub struct Officer {
    pub office: Office,
    pub person: Person,
}
