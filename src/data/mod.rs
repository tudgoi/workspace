use std::{
    collections::BTreeMap,
    fmt::Display,
    fs,
    path::{Path, PathBuf},
};

use garde::Validate;
use miette::{Diagnostic, LabeledSpan, NamedSource, SourceSpan};
use rusqlite::{ToSql, types::FromSql};
use schemars::JsonSchema;
use serde_derive::{Deserialize, Serialize};
use strum_macros::{EnumString, VariantArray};
use thiserror::Error;

pub mod indexer;
pub mod searcher;

#[derive(Serialize, Deserialize, Debug, JsonSchema, Clone, Validate)]
pub struct Person {
    #[garde(length(max = 64))]
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
    #[garde(length(max = 256))]
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
    #[garde(ascii, length(max = 64))]
    pub office_id: String,
    #[garde(ascii, length(max = 10))]
    pub start: Option<String>,
    #[garde(ascii, length(max = 10))]
    pub end: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Validate)]
pub struct Office {
    #[garde(length(max = 128))]
    pub name: String,
    #[garde(dive)]
    pub photo: Option<Photo>,
    #[garde(skip)]
    pub contacts: Option<BTreeMap<ContactType, String>>,
    #[garde(skip)]
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

#[derive(Debug, Error)]
#[error("Invalid person data for '{id}'")]
pub struct PersonValidationError {
    pub id: String,
    pub src: NamedSource<String>,
    pub labels: Vec<miette::LabeledSpan>,
    #[source]
    pub source: garde::Report,
}

impl Diagnostic for PersonValidationError {
    fn code<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        Some(Box::new("tudgoi::validation::person"))
    }

    fn help<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        Some(Box::new("The following validation errors occurred:"))
    }

    fn source_code(&self) -> Option<&dyn miette::SourceCode> {
        Some(&self.src)
    }

    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        Some(Box::new(self.labels.iter().cloned()))
    }
}

#[derive(Debug, Error)]
#[error("Invalid office data for '{id}'")]
pub struct OfficeValidationError {
    pub id: String,
    pub src: NamedSource<String>,
    pub labels: Vec<miette::LabeledSpan>,
    #[source]
    pub source: garde::Report,
}

impl Diagnostic for OfficeValidationError {
    fn code<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        Some(Box::new("tudgoi::validation::office"))
    }

    fn help<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        Some(Box::new("The following validation errors occurred:"))
    }

    fn source_code(&self) -> Option<&dyn miette::SourceCode> {
        Some(&self.src)
    }

    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        Some(Box::new(self.labels.iter().cloned()))
    }
}

#[derive(Error, Debug, Diagnostic)]
pub enum DataError {
    #[error("io error: {0}")]
    #[diagnostic(code(tudgoi::io))]
    Io(#[from] std::io::Error),

    #[error("Error deserializing TOML: {0}")]
    #[diagnostic(code(tudgoi::toml))]
    Toml(#[from] toml::de::Error),

    #[error("Unexpected directory: {0:?}")]
    #[diagnostic(code(tudgoi::fs::unexpected_dir))]
    UnexpectedDir(PathBuf),

    #[error("Unexpected extension for: {0:?}. Should be `toml`")]
    #[diagnostic(code(tudgoi::fs::extension))]
    FileExtension(PathBuf),

    #[error("Empty file stem part for: {0:?}. Should be of the format `<id>.toml`")]
    #[diagnostic(code(tudgoi::fs::stem))]
    FileStem(PathBuf),

    #[error("Could not convert file name to string: {0:?}")]
    #[diagnostic(code(tudgoi::fs::os_str))]
    OsStr(PathBuf),

    #[error("Could not load Jujutsu config: {0}")]
    #[diagnostic(code(tudgoi::jj::config))]
    Config(#[from] jj_lib::config::ConfigGetError),

    #[error("Could not load Jujutsu repository: {0}")]
    #[diagnostic(code(tudgoi::jj::repo))]
    RepoLoad(#[from] jj_lib::repo::RepoLoaderError),

    #[error("Could not load Jujutsu workspace: {0}")]
    #[diagnostic(code(tudgoi::jj::ws))]
    WorkspaceLoad(#[from] jj_lib::workspace::WorkspaceLoadError),

    #[error("Could not get commit id: {0}")]
    #[diagnostic(code(tudgoi::jj::commit_id))]
    CommitId(PathBuf),

    #[error(transparent)]
    #[diagnostic(transparent)]
    PersonValidation(#[from] PersonValidationError),

    #[error(transparent)]
    #[diagnostic(transparent)]
    OfficeValidation(#[from] OfficeValidationError),
}

pub struct Data {
    dir: PathBuf,
}

impl Data {
    pub fn open(base_dir: &Path) -> Result<Self, DataError> {
        Ok(Self {
            dir: base_dir.to_path_buf(),
        })
    }

    pub fn commit_id(&self) -> Result<String, DataError> {
        use jj_lib::local_working_copy::LocalWorkingCopyFactory;
        use jj_lib::object_id::ObjectId;
        use jj_lib::workspace::WorkingCopyFactories;

        let stacked = jj_lib::config::StackedConfig::with_defaults();
        let settings = jj_lib::settings::UserSettings::from_config(stacked)?;

        let store_factories = jj_lib::repo::StoreFactories::default();
        let mut wc_factories = WorkingCopyFactories::new();
        wc_factories.insert("local".to_owned(), Box::new(LocalWorkingCopyFactory {}));

        let workspace = jj_lib::workspace::Workspace::load(
            &settings,
            &self.dir,
            &store_factories,
            &wc_factories,
        )?;
        let repo = workspace.repo_loader().load_at_head()?;

        let wc_commit_id = repo
            .view()
            .get_wc_commit_id(workspace.workspace_name())
            .ok_or(DataError::CommitId(self.dir.clone()))?;

        Ok(wc_commit_id.hex())
    }

    pub fn persons(&self) -> impl Iterator<Item = Result<(String, Person), DataError>> {
        let person_dir = self.dir.join("person");
        toml_content_in_dir(person_dir).map(|result| {
            let (id, content) = result?;
            let person: Person = toml::from_str(&content)?;
            if let Err(e) = person.validate() {
                let labels = to_labels(&content, &e);
                return Err(DataError::PersonValidation(PersonValidationError {
                    id: id.clone(),
                    src: NamedSource::new(format!("{}.toml", id), content),
                    labels,
                    source: e,
                }));
            }
            Ok((id, person))
        })
    }

    pub fn offices(&self) -> impl Iterator<Item = Result<(String, Office), DataError>> {
        let office_dir = self.dir.join("office");
        toml_content_in_dir(office_dir).map(|result| {
            let (id, content) = result?;
            let office: Office = toml::from_str(&content)?;
            if let Err(e) = office.validate() {
                let labels = to_labels(&content, &e);
                return Err(DataError::OfficeValidation(OfficeValidationError {
                    id: id.clone(),
                    src: NamedSource::new(format!("{}.toml", id), content),
                    labels,
                    source: e,
                }));
            }
            Ok((id, office))
        })
    }
}

fn to_labels(content: &str, report: &garde::Report) -> Vec<miette::LabeledSpan> {
    report
        .iter()
        .map(|(path, error)| {
            let path_str = path.to_string();
            let span = find_span(content, &path_str).unwrap_or(SourceSpan::new(0.into(), 0));
            LabeledSpan::new_with_span(Some(error.to_string()), span)
        })
        .collect()
}

fn find_span(content: &str, path: &str) -> Option<SourceSpan> {
    let key = path
        .split(|c| c == '.' || c == '[' || c == ']')
        .filter(|s| !s.is_empty())
        .last()?;

    if let Some(pos) = content.find(key) {
        // Try to find the value after the key
        let rest = &content[pos + key.len()..];
        if let Some(eq_pos) = rest.find('=') {
            let after_eq = &rest[eq_pos + 1..];
            // Find the first non-whitespace character
            if let Some(val_start) = after_eq.find(|c: char| !c.is_whitespace()) {
                // Find the end of the line or the end of the value
                let val_content = &after_eq[val_start..];
                let val_len = val_content.find(['\n', '\r']).unwrap_or(val_content.len());
                return Some(SourceSpan::new(
                    (pos + key.len() + eq_pos + 1 + val_start).into(),
                    val_len,
                ));
            }
        }
        return Some(SourceSpan::new(pos.into(), key.len().into()));
    }
    None
}

fn toml_content_in_dir(
    dir: PathBuf,
) -> impl Iterator<Item = Result<(String, String), DataError>> {
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
                let stem = path.file_stem().ok_or(DataError::FileStem(path.clone()))?;
                let id = stem.to_str().ok_or(DataError::OsStr(path.clone()))?;
                let content = fs::read_to_string(&path)?;
                Ok((id.to_string(), content))
            }
        }
    })
}
