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
use crate::OutputFormat;

use crate::context::{self, Maintenance, Page};
use super::{from_toml_file, repository};

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
    render_persons(&conn, &render_dir, output_format, &tera, &config, &mut search_index)
        .with_context(|| format!("could not render persons"))?;

    // render index
    let counts = repository::query_counts(&conn)?;
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
        &render_dir,
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

    repository::query_for_all_persons(&conn, |dto| {
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
                        let dto = repository::query_incumbent(&conn, value).with_context(|| {
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
                    let subordinates = repository::query_subordinates(&conn, &dto.id, relation_str)
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
                let office_name = if let Some(ref offices) = person_context.offices {
                    if let Some(ref office) = offices.first() {
                        Some(&office.name)
                    } else {
                        None
                    }
                } else {
                    None
                };
                let title = if let Some(office_name) = office_name {
                    format!("{} ({}), {}", person_context.person.name, person_context.person.id, office_name)
                } else {
                    format!("{} ({})", person_context.person.name, person_context.person.id)
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
