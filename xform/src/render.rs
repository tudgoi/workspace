use anyhow::{Context, Result};
use askama::Template;
use axum::extract::State;
use rusqlite::{Connection, OptionalExtension};
use std::path::Path;
use std::{fs, sync::Arc};
use tera;
use tera::Tera;

use crate::dto::EntityType;
use crate::{LibrarySql, SchemaSql, data};
use crate::{
    context::{OfficeContext, PersonContext},
    dto,
    serve::{self, AppState},
};

use super::repo;
use crate::context::{self, Maintenance, Person, Quondam};

#[tokio::main]
pub async fn run(db: &Path, templates: &Path, output: &Path) -> Result<()> {
    let state = AppState::new(db.to_path_buf(), templates.to_path_buf(), false)?;
    let mut pooled_conn = state.db_pool.get()?;
    let context_fetcher = ContextFetcher::new(&mut pooled_conn, state.config.as_ref().clone())
        .with_context(|| format!("could not create context fetcher"))?;

    fs::create_dir(output).with_context(|| format!("could not create output dir {:?}", output))?;

    let renderer = Renderer::new(templates)?;

    // persons
    render_persons(&context_fetcher, &renderer, output)
        .with_context(|| format!("could not render persons"))?;

    // offices
    render_offices(&context_fetcher, &renderer, output)
        .with_context(|| format!("could not render offices"))?;

    // render index
    let template = serve::handler::index(State(Arc::new(state))).await?;
    let str = template.render()?;
    let output_path = output.join("index.html");
    fs::write(output_path.as_path(), str)
        .with_context(|| format!("could not write rendered file {:?}", output_path))?;

    let search_db_path = output.join("search.db");
    create_search_database(&search_db_path, db)?;

    Ok(())
}

pub struct ContextFetcher<'a> {
    config: context::Config,
    repo: repo::Repository<'a>,
}

impl<'a> ContextFetcher<'a> {
    pub fn new(conn: &'a mut Connection, config: context::Config) -> Result<Self> {
        // read config
        let repo = repo::Repository::new(conn)?;

        Ok(ContextFetcher { config, repo })
    }

    pub fn fetch_person(&self, dynamic: bool, id: &str) -> Result<context::PersonContext> {
        let name = self
            .repo
            .conn
            .get_entity_name(&dto::EntityType::Person, id, |row| row.get(0))?;
        let photo = self
            .repo
            .conn
            .get_entity_photo(&dto::EntityType::Person, id, |row| {
                Ok(data::Photo {
                    url: row.get(0)?,
                    attribution: row.get(1)?,
                })
            })
            .optional()?;
        let contacts = self.repo.get_entity_contacts(&EntityType::Person, id).ok();
        let commit_date = self
            .repo
            .conn
            .get_entity_commit_date(&dto::EntityType::Person, id, |row| row.get(0))
            .optional()?;
        let person = dto::Person {
            id: id.to_string(),
            name,
            photo,
            contacts,
            commit_date,
        };
        let mut offices_for_person = Vec::new();
        self.repo
            .conn
            .get_person_incumbent_office_details(id, |row| {
                let contacts = self
                    .repo
                    .get_entity_contacts(&EntityType::Office, &row.get::<_, String>(0)?)
                    .ok();
                offices_for_person.push(dto::Office {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    photo: if let Some(url) = row.get(2)? {
                        Some(data::Photo {
                            url,
                            attribution: row.get(3)?,
                        })
                    } else {
                        None
                    },
                    contacts,
                });

                Ok(())
            })?;

        // office, official_contacts, supervisors, subordinates
        let mut offices = Vec::new();
        for office_dto in offices_for_person {
            // supervisors
            let supervisors = self
                .repo
                .get_office_supervisors(&office_dto.id)
                .with_context(|| {
                    format!("could not query supervisors for office {}", office_dto.id)
                })?;

            // subordinates
            let subordinates = self
                .repo
                .get_office_subordinates(&office_dto.id)
                .with_context(|| {
                    format!("could not query subordinates for office {}", office_dto.id)
                })?;

            offices.push(context::OfficeDetails {
                office: context::Office {
                    id: office_dto.id,
                    name: office_dto.name,
                },
                photo: office_dto.photo,
                contacts: office_dto.contacts,
                supervisors: if supervisors.is_empty() {
                    None
                } else {
                    Some(supervisors)
                },
                subordinates: if subordinates.is_empty() {
                    None
                } else {
                    Some(subordinates)
                },
            });
        }

        let mut past_tenures = Vec::new();
        self.repo.conn.get_past_tenures(&id, |row| {
            past_tenures.push(context::TenureDetails {
                office: context::Office {
                    id: row.get(0)?,
                    name: row.get(1)?,
                },
                start: row.get(2)?,
                end: row.get(3)?,
            });

            Ok(())
        })?;

        // page
        let page = context::Page {
            base: "../".to_string(),
            dynamic,
        };

        // metadata
        let metadata = context::Metadata {
            maintenance: Maintenance { incomplete: true },
            commit_date: person.commit_date,
        };

        Ok(context::PersonContext {
            person: context::Person {
                id: person.id,
                name: person.name,
            },
            photo: person.photo,
            contacts: person.contacts,
            offices: Some(offices).filter(|v| !v.is_empty()),
            past_tenures: Some(past_tenures).filter(|v| !v.is_empty()),
            config: self.config.clone(),
            page,
            metadata,
        })
    }

    pub fn fetch_office(&self, dynamic: bool, id: &str) -> Result<context::OfficeContext> {
        let name = self.repo.get_office_name(id)?;
        let photo = self
            .repo
            .conn
            .get_entity_photo(&dto::EntityType::Office, id, |row| {
                Ok(data::Photo {
                    url: row.get(0)?,
                    attribution: row.get(1)?,
                })
            })
            .optional()?;
        let contacts = self
            .repo
            .get_entity_contacts(&dto::EntityType::Office, id)?;
        let incumbent = self
            .repo
            .conn
            .get_office_incumbent(id, |row| {
                Ok(context::Person {
                    id: row.get(0)?,
                    name: row.get(1)?,
                })
            })
            .optional()?;
        let mut quondams = Vec::new();
        self.repo.conn.get_office_quondams(id, |row| {
            quondams.push(context::Quondam {
                person: context::Person {
                    id: row.get(0)?,
                    name: row.get(1)?,
                },
                start: row.get(2)?,
                end: row.get(3)?,
            });

            Ok(())
        })?;
        let mut quondams = Vec::new();
        self.repo.conn.get_office_quondams(id, |row| {
            quondams.push(Quondam {
                person: Person {
                    id: row.get(0)?,
                    name: row.get(1)?,
                },
                start: row.get(2)?,
                end: row.get(3)?,
            });
            Ok(())
        })?;
        let commit_date = self
            .repo
            .conn
            .get_entity_commit_date(&dto::EntityType::Office, id, |row| {
                Ok(row.get::<_, chrono::NaiveDate>(0)?)
            })
            .optional()?
            .map(|d| d.to_string());

        // page
        let page = context::Page {
            base: "../".to_string(),
            dynamic,
        };

        // metadata
        let metadata = context::Metadata {
            maintenance: Maintenance { incomplete: true },
            commit_date,
        };

        Ok(context::OfficeContext {
            office: context::Office {
                id: id.to_string(),
                name,
            },
            photo,
            contacts: Some(contacts).filter(|v| !v.is_empty()),
            incumbent,
            quondams: Some(quondams).filter(|v| !v.is_empty()),
            config: self.config.clone(),
            page,
            metadata,
        })
    }
}

pub struct Renderer {
    tera: Tera,
}

impl Renderer {
    pub fn new(templates: &Path) -> Result<Self> {
        let templates_glob = templates.join("**").join("*.html");
        let templates_glob_str = templates_glob
            .to_str()
            .with_context(|| format!("could not convert template path {:?}", templates))?;
        let tera = Tera::new(templates_glob_str)
            .with_context(|| format!("could not create Tera instance"))?;

        Ok(Renderer { tera })
    }

    pub fn render_person(&self, context: &PersonContext) -> Result<String> {
        self.render(&context, "person.html")
    }

    pub fn render_office(&self, context: &OfficeContext) -> Result<String> {
        self.render(&context, "office.html")
    }

    fn render<T: serde::Serialize>(&self, context: &T, template_name: &str) -> Result<String> {
        let context = tera::Context::from_serialize(context)
            .with_context(|| format!("could not create convert person to context"))?;
        self.tera
            .render(template_name, &context)
            .with_context(|| format!("could not render template {} with context", template_name))
    }
}

fn render_persons(
    context_fetcher: &ContextFetcher,
    renderer: &Renderer,
    output: &Path,
) -> Result<()> {
    // persons
    let person_path = output.join("person");
    fs::create_dir(person_path.as_path())
        .with_context(|| format!("could not create person dir {:?}", person_path))?;

    let person_ids = context_fetcher
        .repo
        .list_all_person_ids()
        .with_context(|| "could not query all persons")?;

    for id in person_ids {
        let person_context = context_fetcher
            .fetch_person(false, &id)
            .with_context(|| format!("could not fetch context for person {}", id))?;
        let str = renderer.render_person(&person_context)?;

        let output_path = person_path.join(format!("{}.html", person_context.person.id));

        fs::write(output_path.as_path(), str)
            .with_context(|| format!("could not write rendered file {:?}", output_path))?;
    }

    Ok(())
}

fn render_offices(
    context_fetcher: &ContextFetcher,
    renderer: &Renderer,
    output: &Path,
) -> Result<()> {
    // persons
    let office_path = output.join("office");
    fs::create_dir(office_path.as_path())
        .with_context(|| format!("could not create office dir {:?}", office_path))?;

    let ids = context_fetcher
        .repo
        .list_all_office_id()
        .with_context(|| "could not query all offices")?;

    for id in ids {
        let office_context = context_fetcher
            .fetch_office(false, &id)
            .with_context(|| format!("could not fetch context for office {}", id))?;
        let str = renderer.render_office(&office_context)?;

        let output_path = office_path.join(format!("{}.html", office_context.office.id));

        fs::write(output_path.as_path(), str)
            .with_context(|| format!("could not write rendered file {:?}", output_path))?;
    }

    Ok(())
}

pub fn create_search_database(search_db_path: &Path, db_path: &Path) -> Result<()> {
    let conn = Connection::open(search_db_path)
        .with_context(|| format!("could not create search database"))?;
    conn.create_entity_tables()?;
    let db_path_str = db_path
        .to_str()
        .with_context(|| format!("could not convert path {:?}", db_path))?;
    conn.attach_db(db_path_str)?;
    conn.copy_entity_from_db()?;
    conn.detach_db()?;

    // The error from `close` is `(Connection, Error)`, so we map it to just the error.
    conn.close()
        .map_err(|(_, err)| err)
        .with_context(|| format!("could not close search database"))?;

    Ok(())
}
