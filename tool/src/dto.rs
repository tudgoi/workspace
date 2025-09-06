use std::collections::HashMap;

use crate::{context, data};

#[derive(Debug)]
pub struct Office {
    pub id: String,
    pub name: String,
    pub photo: Option<data::Photo>,
    pub contacts: Option<HashMap<data::ContactType, String>>,
}

#[derive(Debug)]
pub struct PersonOffice {
    pub person: context::Person,
    pub photo: Option<data::Photo>,
    pub contacts: Option<HashMap<data::ContactType, String>>,
    pub offices: Option<Vec<Office>>,
    pub updated: String,
}

#[derive(Debug)]
pub struct Counts {
    pub persons: u32,
    pub offices: u32,
}