use std::collections::BTreeMap;

use rusqlite::{ToSql, types::FromSql};
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
impl ContactType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ContactType::Address => "address",
            ContactType::Phone => "phone",
            ContactType::Email => "email",
            ContactType::Website => "website",
            ContactType::Wikipedia => "wikipedia",
            ContactType::X => "x",
            ContactType::Youtube => "youtube",
            ContactType::Facebook => "facebook",
            ContactType::Instagram => "instagram",
            ContactType::Wikidata => "wikidata",
        }
    }
}

impl ToSql for ContactType {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.as_str().into())
    }
}

impl FromSql for ContactType {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            rusqlite::types::ValueRef::Text(s) => {
                match s {
                    b"address" => Ok(ContactType::Address),
                    b"phone" => Ok(ContactType::Phone),
                    b"email" => Ok(ContactType::Email),
                    b"website" => Ok(ContactType::Website),
                    b"wikipedia" => Ok(ContactType::Wikipedia),
                    b"x" => Ok(ContactType::X),
                    b"youtube" => Ok(ContactType::Youtube),
                    b"facebook" => Ok(ContactType::Facebook),
                    b"instagram" => Ok(ContactType::Instagram),
                    b"wikidata" => Ok(ContactType::Wikidata),
                    _ => Err(rusqlite::types::FromSqlError::Other(
                        format!("Unrecognized ContactType: {}", String::from_utf8_lossy(s))
                            .into(),
                    )),
                }
            }
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
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

impl FromSql for SupervisingRelation {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            rusqlite::types::ValueRef::Text(s) => {
                match s {
                    b"head" => Ok(SupervisingRelation::Head),
                    b"adviser" => Ok(SupervisingRelation::Adviser),
                    b"during_the_pleasure_of" => Ok(SupervisingRelation::DuringThePleasureOf),
                    b"responsible_to" => Ok(SupervisingRelation::ResponsibleTo),
                    b"member_of" => Ok(SupervisingRelation::MemberOf),
                    b"minister" => Ok(SupervisingRelation::Minister),
                    _ => Err(rusqlite::types::FromSqlError::Other(
                        format!(
                            "Unrecognized SupervisingRelation: {}",
                            String::from_utf8_lossy(s)
                        )
                        .into(),
                    )),
                }
            }
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}