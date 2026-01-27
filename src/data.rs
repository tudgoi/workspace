use std::{
    collections::BTreeMap,
    fmt::Display,
    fs,
    path::{Path, PathBuf},
};

use rusqlite::{ToSql, types::FromSql};
use schemars::JsonSchema;
use serde_derive::{Deserialize, Serialize};
use strum_macros::{EnumString, VariantArray};
use thiserror::Error;
use garde::Validate;

#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, Validate)]
pub struct Person {
    #[garde(ascii, length(max=32))]
    pub name: String,
    #[garde(dive)]
    pub photo: Option<Photo>,
    #[garde(skip)]
    pub contacts: Option<BTreeMap<ContactType, String>>,
    #[garde(dive)]
    pub tenures: Option<Vec<Tenure>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq, Validate)]
#[serde(deny_unknown_fields)]
pub struct Photo {
    #[garde(url)]
    pub url: String,
    #[garde(length(max=64))]
    pub attribution: Option<String>,
}

#[derive(
    Serialize,
    Deserialize,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Clone,
    JsonSchema,
    VariantArray,
    EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
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

    pub fn to_link(&self, s: &str) -> String {
        match *self {
            ContactType::Address => String::from(""),
            ContactType::Phone => format!("tel:{}", s),
            ContactType::Email => format!("mailto:{}", s),
            ContactType::Website => s.to_string(),
            ContactType::Wikipedia => format!("https://en.wikipedia.org/wiki/{}", s),
            ContactType::X => format!("https://x.com/{}", s),
            ContactType::Youtube => format!("https://www.youtube.com/{}", s),
            ContactType::Facebook => format!("https://www.facebook.com/{}", s),
            ContactType::Instagram => format!("https://www.instagram.com/{}", s),
            ContactType::Wikidata => format!("https://www.wikidata.org/wiki/{}", s),
        }
    }

    pub fn is_independent(&self) -> bool {
        match self {
            ContactType::Address => false,
            ContactType::Phone => false,
            ContactType::Email => false,
            ContactType::Website => false,
            ContactType::Wikipedia => true,
            ContactType::X => false,
            ContactType::Youtube => false,
            ContactType::Facebook => false,
            ContactType::Instagram => false,
            ContactType::Wikidata => true,
        }
    }
}

impl Display for ContactType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
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
            rusqlite::types::ValueRef::Text(s) => match s {
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
                    format!("Unrecognized ContactType: {}", String::from_utf8_lossy(s)).into(),
                )),
            },
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, PartialEq, Eq, Validate)]
#[serde(deny_unknown_fields)]
pub struct Tenure {
    #[garde(ascii, length(max=32))]
    pub office_id: String,
    #[garde(ascii, length(max=10))]
    pub start: Option<String>,
    #[garde(ascii, length(max=10))]
    pub end: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Office {
    pub name: String,
    pub photo: Option<Photo>,
    pub contacts: Option<BTreeMap<ContactType, String>>,
    pub supervisors: Option<BTreeMap<SupervisingRelation, String>>,
}

#[derive(
    Serialize,
    Deserialize,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Clone,
    VariantArray,
    EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum SupervisingRelation {
    Head,
    Adviser,
    DuringThePleasureOf,
    ResponsibleTo,
    MemberOf,
    Minister,
}

impl Display for SupervisingRelation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
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

    pub fn forward_label(&self) -> &'static str {
        match self {
            SupervisingRelation::Head => "Head",
            SupervisingRelation::Adviser => "Adviser",
            SupervisingRelation::DuringThePleasureOf => "During the pleasure of",
            SupervisingRelation::ResponsibleTo => "Responsible to",
            SupervisingRelation::MemberOf => "Member of",
            SupervisingRelation::Minister => "Minister",
        }
    }

    pub fn reverse_label(&self) -> &'static str {
        match self {
            SupervisingRelation::Head => "Heads",
            SupervisingRelation::Adviser => "Advises",
            SupervisingRelation::DuringThePleasureOf => "During their pleasure",
            SupervisingRelation::ResponsibleTo => "Under their responsibility",
            SupervisingRelation::MemberOf => "Members",
            SupervisingRelation::Minister => "Under their Ministry",
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
            rusqlite::types::ValueRef::Text(s) => match s {
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
            },
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

#[derive(Error, Debug)]
pub enum DataError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Error deserializing TOML: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Unexpected directory: {0:?}")]
    UnexpectedDir(PathBuf),

    #[error("Unexpected extension for: {0:?}. Should be `toml`")]
    FileExtension(PathBuf),

    #[error("Empty file stem part for: {0:?}. Should be of the format `<id>.toml`")]
    FileStem(PathBuf),
    
    #[error("Could not convert file name to string: {0:?}")]
    OsStr(PathBuf),
}

pub struct Data {
    person_dir: PathBuf,
    office_dir: PathBuf,
}

impl Data {
    pub fn open(base_dir: &Path) -> Result<Self, DataError> {
        Ok(Self {
            person_dir: base_dir.join("person"),
            office_dir: base_dir.join("office"),
        })
    }

    pub fn persons(&self) -> impl Iterator<Item = Result<(String, Person), DataError>> {
        toml_content_in_dir(&self.person_dir).map(|result| {
            let (id, content) = result?;
            let person = toml::from_str(&content)?;
            Ok((id, person))
        })
    }

    pub fn offices(&self) -> impl Iterator<Item = Result<(String, Office), DataError>> {
        toml_content_in_dir(&self.office_dir).map(|result| {
            let (id, content) = result?;
            let office = toml::from_str(&content)?;
            Ok((id, office))
        })
    }
}

fn toml_content_in_dir(
    dir: &Path,
) -> impl Iterator<Item = Result<(String, String), DataError>> + '_ {
    // TODO This doesn't return an error when dir doesn't exist. Why?
    fs::read_dir(dir).into_iter().flatten().map(|entry| {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            Err(DataError::UnexpectedDir(path))
        } else {
            let extension = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if extension != "toml" {
                Err(DataError::FileExtension(path))
            } else {
                let path = entry.path();
                let stem = path
                    .file_stem()
                    .ok_or(DataError::FileStem(path.clone()))?;
                let id = stem.to_str().ok_or(DataError::OsStr(path.clone()))?;
                let content = fs::read_to_string(&path)?;
                Ok((id.to_string(), content))
            }
        }
    })
}
