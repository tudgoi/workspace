use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_derive::Serialize;
use serde_variant::to_variant_name;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tera;
use tera::Tera;

use crate::data::Supervisor;
use crate::{OutputFormat, data};

use super::from_toml_file;
use crate::context::{self, Maintenance, Page};
use crate::dto::{self};

fn query_counts(conn: &Connection) -> Result<dto::Counts> {
    Ok(dto::Counts {
        persons: conn.query_one("SELECT COUNT(*) FROM person", [], |row| row.get(0))?,
        offices: conn.query_one("SELECT COUNT(*) FROM office", [], |row| row.get(0))?,
    })
}

fn query_offices(conn: &Connection, person_id: &str) -> Result<Vec<dto::Office>> {
    let mut stmt = conn.prepare(
        "
        SELECT o.id, o.data
        FROM incumbent AS i
        INNER JOIN office AS o ON o.id=i.office_id
        WHERE i.person_id=?1
        ORDER BY o.id
    ",
    )?;
    let iter = stmt.query_map([person_id], |row| {
        let id: String = row.get(0)?;
        let data: String = row.get(1)?;
        Ok((id, data))
    })?;
    let mut offices = Vec::new();
    for result in iter {
        let (id, data_str) = result?;
        let office: data::Office = serde_json::from_str(&data_str)?;
        offices.push(dto::Office { id, data: office });
    }

    Ok(offices)
}

fn query_for_all_persons<F>(conn: &Connection, mut process: F) -> Result<()>
where
    F: FnMut(dto::PersonOffice) -> Result<()>,
{
    let mut stmt = conn
        .prepare(
            "
        SELECT p.id, p.data, p.updated
        FROM person AS p
        ORDER BY p.id
    ",
        )
        .with_context(|| format!("could not create statement for reading person table"))?;
    let iter = stmt
        .query_map([], |row| {
            let person_id: String = row.get(0)?;
            let person_data: String = row.get(1)?;
            let updated: String = row.get(2)?;

            let person: data::Person = serde_json::from_str(&person_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,                           // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e),                 // Box the original error
                )
            })?;
            Ok(dto::Person {
                id: person_id,
                data: person,
                updated,
            })
        })
        .with_context(|| format!("querying person table failed"))?;

    for result in iter {
        let person = result?;
        let person_id = person.id.clone();
        let offices = query_offices(conn, &person.id)?;

        let person_office = dto::PersonOffice {
            person,
            offices: if offices.is_empty() {
                None
            } else {
                Some(offices)
            },
        };
        process(person_office)
            .with_context(|| format!("could not render person `{}`", person_id))?;
    }

    Ok(())
}

fn query_incumbent(conn: &Connection, office_id: &str) -> Result<dto::Officer> {
    let mut stmt = conn
        .prepare(
            "
        SELECT o.id, o.data, p.id, p.data, p.updated
        FROM office AS o
        LEFT JOIN incumbent AS i ON i.office_id = o.id
        LEFT JOIN person AS p ON p.id = i.person_id
        WHERE o.id = ?1
    ",
        )
        .with_context(|| format!("could not query incumbent for {:?}", office_id))?;
    let mut iter = stmt.query_map([office_id], |row| {
        // TODO do not query office_id which we already have
        let office_id: String = row.get(0)?;
        let office_data: String = row.get(1)?;
        let person_id: Option<String> = row.get(2)?;
        let person_data: Option<String> = row.get(3)?;
        let person_updated: Option<String> = row.get(4)?;

        let office: data::Office = serde_json::from_str(&office_data).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                1,                           // Column index
                rusqlite::types::Type::Text, // The SQL type
                Box::new(e),                 // Box the original error
            )
        })?;
        let person = if let (Some(person_id), Some(person_data), Some(person_updated)) =
            (person_id, person_data, person_updated)
        {
            let person = serde_json::from_str(&person_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,                           // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e),                 // Box the original error
                )
            })?;
            Some(dto::Person {
                id: person_id,
                data: person,
                updated: person_updated,
            })
        } else {
            None
        };

        Ok(dto::Officer {
            office: dto::Office {
                id: office_id,
                data: office,
            },
            person,
        })
    })?;

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
    let mut stmt = conn.prepare(
        "
        SELECT o.data, p.id, p.data, p.updated
        FROM supervisor AS s
        LEFT JOIN incumbent AS i ON i.office_id = s.office_id
        INNER JOIN office AS o ON o.id = s.office_id
        LEFT JOIN person AS p ON p.id = i.person_id
        WHERE s.supervisor_office_id = ?1 AND s.relation = ?2
        ORDER BY p.id
    ",
    )?;
    let iter = stmt
        .query_map([office_id, relation], |row| {
            let office_data: String = row.get(0)?;
            let person_id: Option<String> = row.get(1)?;
            let person_data: Option<String> = row.get(2)?;
            let person_updated: Option<String> = row.get(3)?;

            let office: data::Office = serde_json::from_str(&office_data).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,                           // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e),                 // Box the original error
                )
            })?;
            let person = if let (Some(person_id), Some(person_data), Some(person_updated)) =
                (person_id, person_data, person_updated)
            {
                let person = serde_json::from_str(&person_data).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        2,                           // Column index
                        rusqlite::types::Type::Text, // The SQL type
                        Box::new(e),                 // Box the original error
                    )
                })?;

                Some(dto::Person {
                    id: person_id,
                    data: person,
                    updated: person_updated,
                })
            } else {
                None
            };

            Ok(dto::Officer {
                office: dto::Office {
                    id: office_id.to_string(),
                    data: office,
                },
                person,
            })
        })
        .with_context(|| format!("could not query supervisor"))?;

    let mut dtos = Vec::new();
    for result in iter {
        dtos.push(result?);
    }
    Ok(dtos)
}

pub fn run(
    db: PathBuf,
    templates: PathBuf,
    output: PathBuf,
    output_format: OutputFormat,
) -> Result<()> {
    let render_dir = match output_format {
        OutputFormat::Json => {
            let dir = output.join("json");
            fs::create_dir(dir.as_path())
                .with_context(|| format!("could not create dir {:?}", dir))?;
            dir
        }
        OutputFormat::Html => {
            let dir = output.join("html");
            fs::create_dir(dir.as_path())
                .with_context(|| format!("could not create dir {:?}", dir))?;
            dir
        }
    };
    let mut search_index = Vec::new();

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

    // open DB
    let conn =
        Connection::open(db.as_path())
            .with_context(|| format!("could not open DB at {:?}", db))?;

    // persons
    render_persons(&conn, render_dir.as_path(), output_format, &tera, &config, &mut search_index)
        .with_context(|| format!("could not render persons"))?;

    // render index
    let counts = query_counts(&conn)?;
    let context = context::IndexContext {
        persons: counts.persons,
        offices: counts.offices,
        page: Page {
            path: "".to_string(),
        },
        config: config.clone(),
    };
    render_page(
        "index",
        &context,
        &render_dir.as_path(),
        output_format,
        &tera,
        "index.html",
    )
    .with_context(|| format!("could not render index"))?;

    if output_format == OutputFormat::Html {
        let search_index_str = serde_json::to_string(&search_index)?;
        let search_dir = output.join("search");
        fs::create_dir(search_dir.as_path())?;
        let file_path = search_dir.join("index.json");
        fs::write(file_path, search_index_str)?;
    }
    
    Ok(())
}

#[derive(Serialize, Debug)]
struct SearchIndexEntry {
    title: String,
    url: String,
}

fn render_persons(
    conn: &Connection,
    output: &Path,
    output_format: OutputFormat,
    tera: &Tera,
    config: &context::Config,
    search_index: &mut Vec<SearchIndexEntry>,
) -> Result<()> {
    // persons
    let person_path = output.join("person");
    fs::create_dir(person_path.as_path())
        .with_context(|| format!("could not create person dir {:?}", person_path))?;

    query_for_all_persons(&conn, |dto| {
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
        let mut offices = Vec::new();
        if let Some(dtos) = dto.offices {
            for dto in dtos {
                // supervisors
                let supervisors = if let Some(supervisors) = dto.data.supervisors {
                    let mut supervisors_context: HashMap<Supervisor, context::Officer> =
                        HashMap::new();
                    for (key, value) in supervisors.iter() {
                        let dto = query_incumbent(&conn, value).with_context(|| {
                            format!("could not query incumbent for office `{}`", value)
                        })?;

                        supervisors_context.insert(key.clone(), dto.into());
                    }

                    Some(supervisors_context)
                } else {
                    None
                };

                // subordinates
                const ALL_RELATIONS: [Supervisor; 5] = [
                    Supervisor::Adviser,
                    Supervisor::DuringThePleasureOf,
                    Supervisor::Head,
                    Supervisor::ResponsibleTo,
                    Supervisor::MemberOf,
                ];

                let mut map = HashMap::new();
                for relation in ALL_RELATIONS {
                    let mut officers = Vec::new();
                    let relation_str = to_variant_name(&relation)?;
                    let subordinates = query_subordinates(&conn, &dto.id, relation_str)
                        .with_context(|| {
                            format!(
                                "could not query subordinates for office `{}` as `{}`",
                                dto.id, relation_str
                            )
                        })?;
                    for dto in subordinates {
                        officers.push(dto.into());
                    }
                    if !officers.is_empty() {
                        map.insert(relation, officers);
                    }
                }
                let subordinates = if map.is_empty() { None } else { Some(map) };

                // office_photo
                let office_photo = if let Some(photo) = dto.data.photo {
                    Some(context::Photo {
                        url: photo.url,
                        attribution: photo.attribution,
                    })
                } else {
                    None
                };

                // official_contacts
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
                offices.push(context::Office {
                    id: dto.id,
                    name: dto.data.name,
                    photo: office_photo,
                    contacts: official_contacts,
                    supervisors,
                    subordinates,
                });
            }
        }

        // page
        let page = context::Page {
            path: "".to_string(),
        };

        // metadata
        let metadata = context::Metadata {
            maintenance: Maintenance { incomplete: true },
            updated: dto.person.updated,
        };

        // construct context
        let person_context = context::PersonContext {
            person,
            photo,
            contacts,
            offices: if offices.is_empty() {
                None
            } else {
                Some(offices)
            },
            config: config.clone(),
            page,
            metadata,
        };

        match output_format {
            OutputFormat::Json => {
                let output_path = person_path.join(format!("{}.json", person_context.person.id));
                let context_json = serde_json::to_string(&person_context)?;
                fs::write(output_path.as_path(), context_json)
                    .with_context(|| format!("could not write rendered file {:?}", output_path))?;
            }
            OutputFormat::Html => {
                let title = if let Some(ref offices) = person_context.offices {
                    if let Some(ref office) = offices.first() {
                        format!("{}, {}", person_context.person.name, office.name)
                    } else {
                        person_context.person.name.clone()
                    }
                } else {
                        person_context.person.name.clone()
                };
                search_index.push(SearchIndexEntry {
                    title,
                    url: format!("./person/{}.html", person_context.person.id),
                });

                let output_path = person_path.join(format!("{}.html", person_context.person.id));
                let context = tera::Context::from_serialize(person_context)
                    .with_context(|| format!("could not create convert person to context"))?;

                // write output
                let str = tera.render("page.html", &context)
                    .with_context(|| format!("could not render template page.html"))?;

                fs::write(output_path.as_path(), str)
                    .with_context(|| format!("could not write rendered file {:?}", output_path))?;
            }
        }

        Ok(())
    }).with_context(|| format!("could not process persons"))?;


    Ok(())
}

fn render_page<T: serde::Serialize>(
    name: &str,
    context: &T,
    output_path: &Path,
    output_format: OutputFormat,
    tera: &Tera,
    template_name: &str,
) -> Result<()> {
    match output_format {
        OutputFormat::Json => {
            let output_path = output_path.join(format!("{}.json", name));
            let context_json = serde_json::to_string(context)?;
            fs::write(output_path.as_path(), context_json)
                .with_context(|| format!("could not write rendered file {:?}", output_path))?;
        }
        OutputFormat::Html => {
            let output_path = output_path.join(format!("{}.html", name));
            let context = tera::Context::from_serialize(context)
                .with_context(|| format!("could not create context"))?;

            // write output
            let str = tera.render(template_name, &context)
                .with_context(|| format!("could not render template `{}`", template_name))?;

            fs::write(output_path.as_path(), str)
                .with_context(|| format!("could not write rendered file {:?}", output_path))?;
        }
    }
    Ok(())
}
