use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs;
use std::path::PathBuf;
use tera;
use tera::Tera;

use crate::data;

use super::from_toml_file;
use crate::context::{self};
use crate::dto::{self};

// TODO remove PersonOffice and pass them as separate params to the process function
fn query_for_all_persons<F>(conn: &Connection, process: F) -> Result<()>
where
    F: Fn(Result<dto::PersonOffice, rusqlite::Error>) -> Result<()>,
{
    let mut stmt = conn
        .prepare(
            "
        SELECT p.id, p.data, p.updated, o.id, o.data
        FROM person AS p
        LEFT JOIN incumbent AS i ON i.person_id=p.id
        INNER JOIN office AS o ON o.id=i.office_id
    ",
        )
        .with_context(|| format!("could not create statement for reading person table"))?;
    let iter = stmt
        .query_map([], |row| {
            let person_id: String = row.get(0)?;
            let person_data: String = row.get(1)?;
            let updated: String = row.get(2)?;
            let office_id: String = row.get(3)?;
            let office_data: String = row.get(4)?;

            let person: data::Person = serde_json::from_str(&person_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,                           // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e),                 // Box the original error
                )
            })?;
            let office: data::Office = serde_json::from_str(&office_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,                           // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e),                 // Box the original error
                )
            })?;
            Ok(dto::PersonOffice {
                person: dto::Person {
                    id: person_id,
                    data: person,
                },
                office: Some(dto::Office {
                    id: office_id,
                    data: office,
                }),
                updated,
            })
        })
        .with_context(|| format!("querying person table failed"))?;

    for result in iter {
        process(result)?;
    }

    Ok(())
}

fn query_incumbent(conn: &Connection, office_id: &str) -> Result<dto::Officer> {
    let mut stmt = conn
        .prepare(
            "
        SELECT o.id, o.data, p.id, p.data
        FROM incumbent AS i
        INNER JOIN office AS o ON o.id = i.office_id
        LEFT JOIN person AS p ON p.id = i.person_id
        WHERE i.office_id = ?1
        LIMIT 1
    ",
        )
        .with_context(|| format!("could not query incumbent for {:?}", office_id))?;
    let mut iter = stmt
        .query_map([office_id], |row| {
            // TODO do not query office_id which we already have
            let office_id: String = row.get(0)?;
            let office_data: String = row.get(1)?;
            let person_id: String = row.get(2)?;
            let person_data: String = row.get(3)?;

            let office: data::Office = serde_json::from_str(&office_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,                           // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e),                 // Box the original error
                )
            })?;
            let person: data::Person = serde_json::from_str(&person_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,                           // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e),                 // Box the original error
                )
            })?;

            Ok(dto::Officer {
                office: dto::Office {
                    id: office_id,
                    data: office,
                },
                person: dto::Person {
                    id: person_id,
                    data: person,
                },
            })
        })
        .with_context(|| format!("could not query incumbent"))?;

    let dto = iter
        .next()
        .context(format!("could not query person for office {:?}", office_id))?
        .with_context(|| format!("could not read incumbent from DB"))?;

    Ok(dto)
}

fn query_subordinates(
    conn: &Connection,
    office_id: &str,
    relation: &str,
) -> Result<Vec<dto::Officer>> {
    let mut stmt = conn
        .prepare(
            "
        SELECT o.data, p.id, p.data
        FROM supervisor AS s
        INNER JOIN incumbent AS i ON i.office_id = s.office_id
        INNER JOIN office AS o ON o.id = i.office_id
        INNER JOIN person AS p ON p.id = i.person_id
        WHERE supervisor_office_id = ?1 AND relation = ?2
    ",
        )
        .with_context(|| format!("could not query hierarchy for {:?}", office_id))?;
    let iter = stmt
        .query_map([office_id, relation], |row| {
            let office_data: String = row.get(0)?;
            let person_id = row.get(1)?;
            let person_data: String = row.get(2)?;

            let office: data::Office = serde_json::from_str(&office_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,                           // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e),                 // Box the original error
                )
            })?;
            let person: data::Person = serde_json::from_str(&person_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,                           // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e),                 // Box the original error
                )
            })?;

            Ok(dto::Officer {
                office: dto::Office {
                    id: office_id.to_string(),
                    data: office,
                },
                person: dto::Person {
                    id: person_id,
                    data: person,
                },
            })
        })
        .with_context(|| format!("could not query supervisor"))?;

    let mut dtos = Vec::new();
    for result in iter {
        dtos.push(result?);
    }
    Ok(dtos)
}

pub fn run(db: PathBuf, templates: PathBuf, output: PathBuf, output_json: bool) -> Result<()> {
    // read config
    let config: context::Config = from_toml_file(templates.join("config.toml"))
        .with_context(|| format!("could not parse config"))?;

    // open template
    let templates_glob = templates.join("**").join("*.html");
    let templates_glob_str = templates_glob
        .to_str()
        .context(format!("could not convert template path {:?}", templates))?;
    let tera =
        Tera::new(templates_glob_str).with_context(|| format!("could not create Tera instance"))?;

    // setup output
    fs::create_dir(output.as_path())
        .with_context(|| format! {"could not create output dir {:?}", output})?;

    // open DB
    let conn =
        Connection::open(db.as_path()).with_context(|| format!("could not open DB at {:?}", db))?;

    // person
    let person_path = output.join("person");
    fs::create_dir(person_path.as_path())
        .with_context(|| format!("could not create person dir {:?}", person_path))?;

    query_for_all_persons(&conn, |result| {
        let dto = result.with_context(|| format!("could not read person from DB"))?;


        // setup
        let output_path = person_path.join(format!("{}.html", dto.person.id));

        // person
        let person = context::Person {
            id: dto.person.id,
            name: dto.person.data.name,
        };

        // photo
        let photo = if let Some(photo) = dto.person.data.photo {
            Some(context::Photo {
                url: photo.url,
                attribution: photo.attribution,
            })
        } else {
            None
        };

        // contacts
        let contacts = if let Some(contacts) = dto.person.data.contacts {
            Some(context::Contacts {
                phone: contacts.phone,
                email: contacts.email,
                website: contacts.website,
                wikipedia: contacts.wikipedia,
                x: contacts.x,
                facebook: contacts.facebook,
                instagram: contacts.instagram,
                youtube: contacts.youtube,
                address: contacts.address,
            })
        } else {
            None
        };

        // office, official_contacts, supervisors, subordinates
        let (office, office_photo, official_contacts, supervisors, subordinates) = if let Some(dto) = dto.office {
            let supervisors = if let Some(supervisors) = dto.data.supervisors {
                let get_optional_supervisor = |id: Option<String>| -> Result<Option<context::Officer>> {
                    if let Some(id) = id {
                    let dto = query_incumbent(&conn, &id)
                        .with_context(|| format!("could not query office {}", id))?;

                    Ok(Some(dto.into()))
                    } else {
                        Ok(None)
                    }
                };
                let adviser = get_optional_supervisor(supervisors.adviser)?;
                let during_the_pleasure_of= get_optional_supervisor(supervisors.during_the_pleasure_of)?;
                let head = get_optional_supervisor(supervisors.head)?;
                let responsible_to = get_optional_supervisor(supervisors.responsible_to)?;
                Some(context::Supervisors { adviser, during_the_pleasure_of, head, responsible_to })
            } else {
                None
            };

            // subordinates
            let get_option_subordinates = |id: &str, relation: &str| -> Result<Vec<context::Officer>> {
                let mut subordinates = Vec::new();
                for dto in query_subordinates(&conn, id, relation)? {
                    subordinates.push(dto.into());
                }
                Ok(subordinates)
            };
            let adviser = get_option_subordinates(&dto.id, "adviser")?;
            let during_the_pleasure_of = get_option_subordinates(&dto.id, "during_the_pleasure_of")?;
            let head = get_option_subordinates(&dto.id, "head")?;
            let responsible_to = get_option_subordinates(&dto.id, "responsible_to")?;
            let subordinates = Some(context::Subordinates { adviser, during_the_pleasure_of, head, responsible_to });

            let office = Some(context::Office {
                id: dto.id,
                name: dto.data.name,
            });
            
            // office_photo
            let office_photo = if let Some(photo) = dto.data.photo {
                Some(context::Photo {
                    url: photo.url,
                    attribution: photo.attribution,
                })
            } else {
                None
            };

            let official_contacts = if let Some(contacts) = dto.data.contacts {
                Some(context::Contacts {
                    phone: contacts.phone,
                    email: contacts.email,
                    website: contacts.website,
                    wikipedia: contacts.wikipedia,
                    x: contacts.x,
                    facebook: contacts.facebook,
                    instagram: contacts.instagram,
                    youtube: contacts.youtube,
                    address: contacts.address,
                })
            } else {
                None
            };

            (office, office_photo, official_contacts, supervisors, subordinates)
        } else {
            (None, None, None, None, None)
        };

        // page
        let page = context::Page {
            path: "".to_string(),
            updated: dto.updated,
        };

        // metadata
        let metadata = context::Metadata {
            incomplete: true,
            updated: "2025-08-29".to_string(),
        };
        
        // construct context
        let person_context = context::PersonContext {
            person,
            photo,
            office_photo,
            contacts,
            office,
            official_contacts,
            supervisors,
            subordinates,
            config: config.clone(),
            page,
            metadata,
        };
        if output_json {
            let output_path = person_path.join(format!("{}.json", person_context.person.id));
            let context_json = serde_json::to_string(&person_context)?;
            fs::write(output_path.as_path(), context_json)
                .with_context(|| format!("could not write rendered file {:?}", output_path))?;
        }
        
        let context = tera::Context::from_serialize(person_context)
            .with_context(|| format!("could not create convert person to context"))?;

        // write output
        let str = tera
            .render("page.html", &context)
            .with_context(|| format!("could not render template"))?;

        fs::write(output_path.as_path(), str)
            .with_context(|| format!("could not write rendered file {:?}", output_path))?;

        Ok(())
    })?;

    Ok(())
}
