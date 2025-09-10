use std::collections::BTreeMap;

use crate::data;

#[derive(Debug)]
pub struct Office {
    pub id: String,
    pub name: String,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
}

#[derive(Debug)]
pub struct Counts {
    pub persons: u32,
    pub offices: u32,
}