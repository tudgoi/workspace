use std::{
    fs::{self, ReadDir},
    path::Path,
};

use crate::{
    data,
    graph::{self},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Person {
    #[allow(dead_code)]
    updated: Option<String>,
    title: String,
    description: String,
    taxonomies: Taxonomies,
    extra: Extra,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Taxonomies {
    #[allow(dead_code)]
    maintenance: Option<Vec<String>>,
    member_of: Option<Vec<String>>,
    #[allow(dead_code)]
    elected_by: Option<Vec<String>>,
    during_the_pleasure_of: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Extra {
    photo: Option<String>,
    #[allow(dead_code)]
    sources: Option<Vec<String>>,
    contacts: Option<Contacts>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Contacts {
    address: Option<String>,
    #[allow(dead_code)]
    phone: Option<Sequence>,
    email: Option<Sequence>,
    website: Option<String>,
    wikipedia: Option<String>,
    x: Option<String>,
    youtube: Option<String>,
    facebook: Option<String>,
    instagram: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Sequence {
    One(String),
    Many(Vec<String>),
}

pub struct OldIngestor {
    dir_iter: ReadDir,
}

impl OldIngestor {
    pub fn new(dir_path: &Path) -> Result<Self> {
        let dir_iter = fs::read_dir(dir_path).with_context(|| "could not read directory")?;

        Ok(OldIngestor { dir_iter })
    }
}

impl Iterator for OldIngestor {
    type Item = Result<Vec<Vec<graph::Property>>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.dir_iter.next().map(|result| {
            let path = result.context("skipping - could not get next file")?.path();
            if !path.is_file() {
                bail!("skipping {:?} - not a file", path)
            }
            if path.extension().and_then(|s| s.to_str()) != Some("toml") {
                bail!("skipping {:?} - file does not have .toml extension", path)
            }
            let id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .with_context(|| format!("skipping {:?} - could not create id for", path))?
                .to_string();

            let content = fs::read_to_string(&path)
                .with_context(|| format!("skipping {:?} - could not read file", path))?;
            let old_person: Person = toml::from_str(&content)
                .with_context(|| format!("skipping {:?} - could not parse TOML from file", path))?;
            let (mut person, office): (Vec<graph::Property>, Vec<graph::Property>) =
                old_person.into();

            person.push(graph::Property::Id(id));

            Ok(vec![office, person])
        })
    }
}

impl From<Person> for (Vec<graph::Property>, Vec<graph::Property>) {
    fn from(value: Person) -> Self {
        let mut person = vec![graph::Property::Type(graph::EntityType::Person)];
        let mut office = vec![graph::Property::Type(graph::EntityType::Office)];

        // title
        person.push(graph::Property::Name(value.title));

        // description
        office.push(graph::Property::Name(value.description));

        person.push(graph::Property::Tenure(office.clone()));

        // taxonomies.member_of
        if let Some(member_of) = value.taxonomies.member_of
            && let Some(member_of) = member_of.first()
        {
            office.push(graph::Property::Supervisor(
                data::SupervisingRelation::MemberOf,
                vec![graph::Property::Name(member_of.clone())],
            ));
        }

        // taxonomies.during_the_pleasure_of
        if let Some(during_the_pleasure_of) = value.taxonomies.during_the_pleasure_of
            && let Some(during_the_pleasure_of) = during_the_pleasure_of.first()
        {
            office.push(graph::Property::Supervisor(
                data::SupervisingRelation::DuringThePleasureOf,
                vec![graph::Property::Name(during_the_pleasure_of.to_string())],
            ));
        }

        // extra.photo
        if let Some(photo) = value.extra.photo {
            person.push(graph::Property::Photo {
                url: photo,
                attribution: None,
            })
        }

        // contacts
        if let Some(contacts) = value.extra.contacts {
            // address
            if let Some(address) = contacts.address {
                person.push(graph::Property::Contact(
                    data::ContactType::Address,
                    address,
                ));
            }

            // email
            if let Some(email) = contacts.email {
                let emails = match email {
                    Sequence::One(email) => vec![email],
                    Sequence::Many(items) => items,
                };
                let mut office_emails = Vec::new();
                let mut personal_emails = Vec::new();
                for email in emails {
                    if email.contains(".gov.in") {
                        office_emails.push(email);
                    } else {
                        personal_emails.push(email);
                    }
                }
                if let Some(email) = office_emails.first() {
                    office.push(graph::Property::Contact(
                        data::ContactType::Email,
                        email.clone(),
                    ));
                }
                if let Some(email) = personal_emails.first() {
                    person.push(graph::Property::Contact(
                        data::ContactType::Email,
                        email.clone(),
                    ));
                }
            }

            // website
            if let Some(website) = contacts.website {
                if website.contains(".gov.in") {
                    office.push(graph::Property::Contact(
                        data::ContactType::Website,
                        website,
                    ));
                } else {
                    person.push(graph::Property::Contact(
                        data::ContactType::Website,
                        website,
                    ));
                }
            }

            // wikipedia
            if let Some(wikipedia) = contacts.wikipedia {
                person.push(graph::Property::Contact(
                    data::ContactType::Wikipedia,
                    wikipedia,
                ));
            }

            // x
            if let Some(x) = contacts.x {
                person.push(graph::Property::Contact(data::ContactType::X, x));
            }

            // youtube
            if let Some(youtube) = contacts.youtube {
                person.push(graph::Property::Contact(
                    data::ContactType::Youtube,
                    youtube,
                ));
            }

            // facebook
            if let Some(facebook) = contacts.facebook {
                person.push(graph::Property::Contact(
                    data::ContactType::Facebook,
                    facebook,
                ));
            }

            // instagram
            if let Some(instagram) = contacts.instagram {
                person.push(graph::Property::Contact(
                    data::ContactType::Instagram,
                    instagram,
                ));
            }
        }

        (person, office)
    }
}
