use std::collections::BTreeMap;

use rusqlite::ToSql;
use schemars::JsonSchema;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Person {
    pub name: String,
    pub photo: Option<Photo>,
    pub contacts: Option<BTreeMap<ContactType, String>>,
    pub tenures: Option<Vec<Tenure>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct Photo {
    pub url: String,
    pub attribution: Option<String>,
}

#[derive(
    Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ContactType {
    Address,
    Phone,
    Email,
    Website,
    Wikipedia,
    X,
    Youtube,
    Facebook,
    Instagram,
    Wikidata,
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Tenure {
    pub office_id: String,
    pub start: Option<String>,
    pub end: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Office {
    pub name: String,
    pub photo: Option<Photo>,
    pub contacts: Option<BTreeMap<ContactType, String>>,
    pub supervisors: Option<BTreeMap<SupervisingRelation, String>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SupervisingRelation {
    Head,
    Adviser,
    DuringThePleasureOf,
    ResponsibleTo,
    MemberOf,
    Minister,
}

impl SupervisingRelation {
    pub fn as_str(&self) -> &'static str {
        match self {
            SupervisingRelation::Head => "head",
            SupervisingRelation::Adviser => "adviser",
            SupervisingRelation::DuringThePleasureOf => "during_the_pleasure_of",
            SupervisingRelation::ResponsibleTo => "responsible_to",
            SupervisingRelation::MemberOf => "member_of",
            SupervisingRelation::Minister => "minister",
        }
    }
}

impl ToSql for SupervisingRelation {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.as_str().into())
    }
}