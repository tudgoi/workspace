use anyhow::{Context, Result};
use askama::Template;
use axum::extract::{self, State};
use rusqlite::{Connection, OptionalExtension};
use std::collections::BTreeMap;
use std::path::Path;
use std::{fs, sync::Arc};
use tera;
use tera::Tera;

use crate::data::ContactType;
use crate::{LibrarySql, SchemaSql, data};
use crate::{
    context::OfficeContext,
    dto,
    serve::{self, AppState},
};

use crate::context::{self, Maintenance, Person, Quondam};

#[tokio::main]
pub async fn run(db: &Path, templates: &Path, output: &Path) -> Result<()> {
    let state = Arc::new(AppState::new(db.to_path_buf(), templates.to_path_buf(), false)?);
    let mut conn = state.db_pool.get()?;
    let context_fetcher = ContextFetcher::new(&mut conn, state.config.as_ref().clone())
        .with_context(|| format!("could not create context fetcher"))?;

    fs::create_dir(output).with_context(|| format!("could not create output dir {:?}", output))?;

    let renderer = Renderer::new(templates)?;

    // persons
    render_persons(context_fetcher.conn, State(state.clone()), output)
        .await
        .with_context(|| format!("could not render persons"))?;

    // offices
    render_offices(&context_fetcher, &renderer, output)
        .with_context(|| format!("could not render offices"))?;

    // render index
    let template = serve::handler::index(State(state.clone())).await?;
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
    conn: &'a mut Connection,
}

impl<'a> ContextFetcher<'a> {
    pub fn new(conn: &'a mut Connection, config: context::Config) -> Result<Self> {
        // read config

        Ok(ContextFetcher { config, conn })
    }

    pub fn fetch_office(&self, dynamic: bool, id: &str) -> Result<context::OfficeContext> {
        let name = self
            .conn
            .get_entity_name(&dto::EntityType::Office, id, |row| row.get(0))?;
        let photo = self
            .conn
            .get_entity_photo(&dto::EntityType::Office, id, |row| {
                Ok(data::Photo {
                    url: row.get(0)?,
                    attribution: row.get(1)?,
                })
            })
            .optional()?;
        let mut contacts: BTreeMap<ContactType, String> = BTreeMap::new();
        self.conn
            .get_entity_contacts(&dto::EntityType::Office, id, |row| {
                contacts.insert(row.get(0)?, row.get(1)?);

                Ok(())
            })?;
        let incumbent = self
            .conn
            .get_office_incumbent(id, |row| {
                Ok(context::Person {
                    id: row.get(0)?,
                    name: row.get(1)?,
                })
            })
            .optional()?;
        let mut quondams = Vec::new();
        self.conn.get_office_quondams(id, |row| {
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
        self.conn.get_office_quondams(id, |row| {
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

async fn render_persons(conn: &Connection, state: State<Arc<AppState>>, output: &Path) -> Result<()> {
    // persons
    let person_path = output.join("person");
    fs::create_dir(person_path.as_path())
        .with_context(|| format!("could not create person dir {:?}", person_path))?;

    let mut ids: Vec<String> = Vec::new();
        conn
        .get_entity_ids(&dto::EntityType::Person, |row| {
            ids.push(row.get(0)?);
            Ok(())
        })?;

    for id in ids {
        let template =
            serve::handler::person_page(state.clone(), extract::Path(format!("{}.html", id)))
                .await?;
        let str = template.render()?;
        let output_path = output.join("index.html");
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

    let mut ids: Vec<String> = Vec::new();
    context_fetcher
        .conn
        .get_entity_ids(&dto::EntityType::Office, |row| {
            ids.push(row.get(0)?);
            Ok(())
        })?;
    for id in ids {
        let office_context = context_fetcher.fetch_office(false, &id)?;
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
