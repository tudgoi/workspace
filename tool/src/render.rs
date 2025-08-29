use std::fs;
use std::path::PathBuf;
use anyhow::{Context, Result};
use tera::{Tera};
use tera;
use rusqlite::Connection;

use crate::data;

use crate::dto::{self};
use super::from_toml_file;
use crate::context;

fn query_for_all_persons<F>(conn: &Connection, process: F) -> Result<()>
where F: Fn(Result<dto::PersonOffice, rusqlite::Error>) -> Result<()>
{
    let mut stmt = conn.prepare("
        SELECT p.id, p.data, o.id, o.data
        FROM person AS p
        LEFT JOIN incumbent AS i ON i.person_id=p.id
        INNER JOIN office AS o ON o.id=i.office_id
    ").with_context(|| format!("could not create statement for reading person table"))?;
    let iter = stmt.query_map([], |row| {
        let person_id: String = row.get(0)?;
        let person_data: String = row.get(1)?;
        let office_id: String = row.get(2)?;
        let office_data: String = row.get(3)?;

        let person: data::Person = serde_json::from_str(&person_data)
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(
                1, // Column index
                rusqlite::types::Type::Text, // The SQL type
                Box::new(e), // Box the original error
            ))?;
        let office: data::Office = serde_json::from_str(&office_data)
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(
                3, // Column index
                rusqlite::types::Type::Text, // The SQL type
                Box::new(e), // Box the original error
            ))?;
        Ok(dto::PersonOffice {
            person: dto::Person {
                id: person_id,
                data: person
            },
            office: Some(dto::Office {
                id: office_id,
                data: office
            })
        })
    }).with_context(|| format!("querying person table failed"))?;

    for result in iter {
        process(result)?;
    }

    Ok(())
}

fn query_incumbent(conn: &Connection, office_id: &str) -> Result<dto::OfficePerson> {
    let mut stmt = conn.prepare("
        SELECT o.id, o.data, p.id, p.data
        FROM incumbent AS i
        INNER JOIN office AS o ON o.id = i.office_id
        LEFT JOIN person AS p ON p.id = i.person_id
        WHERE i.office_id = ?1
        LIMIT 1
    ").with_context(|| format!("could not query incumbent for {:?}", office_id))?;
    let mut iter = stmt.query_map([office_id], |row| {
        let office_id: String = row.get(0)?;
        let office_data: String = row.get(1)?;
        let person_id: Option<String> = row.get(2)?;
        let person_data: Option<String> = row.get(3)?;

        let office: data::Office = serde_json::from_str(&office_data)
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(
                1, // Column index
                rusqlite::types::Type::Text, // The SQL type
                Box::new(e), // Box the original error
            ))?;
        let person: Option<data::Person> = if let Some(person_data) = person_data {
            Some(serde_json::from_str(&person_data)
                .map_err(|e| rusqlite::Error::FromSqlConversionFailure(
                    3, // Column index
                    rusqlite::types::Type::Text, // The SQL type
                    Box::new(e), // Box the original error
                ))?
            )
        } else {
            None
        };
        
        let person_dto = if let (Some(person_id), Some(person)) = (person_id, person) {
            Some(dto::Person {
                id: person_id,
                data: person
            })
        } else {
            None
        };

        Ok(dto::OfficePerson {
            office: dto::Office {
                id: office_id,
                data: office
            },
            person: person_dto
        })
    }).with_context(|| format!("could not query incumbent"))?;

    let dto = iter.next()
        .context(format!("could not query person for office {:?}", office_id))?
        .with_context(|| format!("could not read incumbent from DB"))?;
        
    Ok(dto)
}

pub fn run(db: PathBuf, templates: PathBuf, output: PathBuf) -> Result<()> {
    // read config
    let config: context::Config = from_toml_file(templates.join("config.toml"))
        .with_context(|| format!("could not parse config"))?;
    
    // open template
    let templates_glob = templates
        .join("**")
        .join("*.html");
    let templates_glob_str = templates_glob
        .to_str()
        .context(format!("could not convert template path {:?}", templates))?;
    let tera = Tera::new(templates_glob_str)
        .with_context(|| format!("could not create Tera instance"))?;
    
    // setup output
    fs::create_dir(output.as_path())
        .with_context(|| format!{"could not create output dir {:?}", output})?;

    // open DB
    let conn = Connection::open(db.as_path())
        .with_context(|| format!("could not open DB at {:?}", db))?;
    
    // person
    let person_path = output.join("person");
    fs::create_dir(person_path.as_path())
        .with_context(|| format!("could not create person dir {:?}", person_path))?;

    query_for_all_persons(&conn, |result| {
        let dto = result
            .with_context(|| format!("could not read person from DB"))?;

        // setup
        let output_path = person_path.join(format!("{}.html", dto.person.id));

        // person
        let person = context::Person {
            id: dto.person.id,
            name: dto.person.data.name
        };
        
        // office and supervisors
        let (office, supervisors) = if let Some(office) = dto.office {
            let supervisors = if let Some(supervisors) = office.data.supervisor {
                let adviser = if let Some(id) = supervisors.adviser {
                    let dto = query_incumbent(&conn, &id)
                        .with_context(|| format!("could not query office {}", id))?;

                    let person = if let Some(person) = dto.person {
                        Some(context::Person {
                            id: person.id,
                            name: person.data.name,
                        })
                    } else {
                        None
                    };
                    Some(context::Officer {
                        office: context::Office {
                            id: dto.office.id,
                            name: dto.office.data.name
                        },
                        person,
                    })
                } else {
                    None
                };
                
                Some(context::Supervisors {
                    adviser
                })
            } else {
                None
            };

            let office = Some(context::Office {
                id: office.id,
                name: office.data.name
            });
            
            (office, supervisors)
        } else {
            (None, None)
        };
        
        // photo
        let photo = if let Some(photo) = dto.person.data.photo {
            Some(context::Photo {
                url: photo.url,
                attribution: photo.attribution
            })
        } else {
            None
        };
        
        // page
        let page = context::Page {
          path: "".to_string(),
          updated: "2025-08-29".to_string()
        };
        
        // metadata
        let metadata = context::Metadata {
            incomplete: true,
            updated: "2025-08-29".to_string()
        };

        // construct context
        let  context = tera::Context::from_serialize(context::PersonContext {
            person,
            photo,
            office,
            supervisors,
            config: config.clone(),
            page,
            metadata,
        })
            .with_context(|| format!("could not create convert person to context"))?;

        // write output
        let str = tera.render("page.html", &context)
            .with_context(|| format!("could not render template"))?;
    
        fs::write(output_path.as_path(), str)
            .with_context(|| format!("could not write rendered file {:?}", output_path))?;

        Ok(())
    })?;
    
    Ok(())
}