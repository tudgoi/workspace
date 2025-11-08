use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs;
use std::path::Path;
use tera;
use tera::Tera;

use crate::{
    ENTITY_SCHEMA_SQL, OutputFormat, context::{OfficeContext, PersonContext, PersonEditContext}, data, dto, graph
};

use super::{from_toml_file, repo};
use crate::context::{self, Maintenance, Page, Person};

pub fn run(db: &Path, templates: &Path, output: &Path, output_format: OutputFormat) -> Result<()> {
    let context_fetcher = ContextFetcher::new(db, templates)
        .with_context(|| format!("could not create context fetcher"))?;

    fs::create_dir(output).with_context(|| format!("could not create output dir {:?}", output))?;

    let renderer = Renderer::new(templates, output_format)?;

    // persons
    render_persons(&context_fetcher, &renderer, output, output_format)
        .with_context(|| format!("could not render persons"))?;

    // offices
    render_offices(&context_fetcher, &renderer, output, output_format)
        .with_context(|| format!("could not render offices"))?;

    // render index
    let context = context_fetcher
        .fetch_index()
        .with_context(|| format!("could not fetch context for index"))?;
    let str = renderer
        .render_index(&context)
        .with_context(|| format!("could not render index"))?;
    let extension = match output_format {
        OutputFormat::Html => ".html",
        OutputFormat::Json => ".json",
    };
    let output_path = output.join(format!("index{}", extension));
    fs::write(output_path.as_path(), str)
        .with_context(|| format!("could not write rendered file {:?}", output_path))?;

    if output_format == OutputFormat::Html {
        let search_db_path = output.join("search.db");
        create_search_database(&search_db_path, db)?;
    }

    Ok(())
}

pub struct ContextFetcher {
    config: context::Config,
    repo: repo::Repository,
}

impl ContextFetcher {
    pub fn new(db: &Path, templates: &Path) -> Result<Self> {
        // read config
        let config: context::Config = from_toml_file(templates.join("config.toml"))
            .with_context(|| format!("could not parse config"))?;
        let repo = repo::Repository::new(db)
            .with_context(|| format!("could not open repository at {:?}", db))?;

        Ok(ContextFetcher { config, repo })
    }

    pub fn fetch_person_edit(&self, dynamic: bool, id: &str) -> Result<PersonEditContext> {
        let person_dto = self.repo
            .get_person(&id)?
            .with_context(|| format!("person {} not found", id))?;

        let tenures = self.repo.list_person_office_tenure(&id)?;

        let person_data = data::Person {
            name: person_dto.name,
            photo: person_dto.photo,
            contacts: person_dto.contacts.filter(|c| !c.is_empty()),
            tenures: if tenures.is_empty() {
                None
            } else {
                Some(tenures)
            },
        };

        Ok(PersonEditContext {
            person: person_data,
            config: self.config.clone(),
            page: context::Page {
                base: "../".to_string(),
                dynamic,
            },
            metadata: context::Metadata {
                maintenance: Maintenance { incomplete: false },
                commit_date: Some(String::from("")),
            },
        })
    }

    pub fn fetch_person(&self, dynamic: bool, id: &str) -> Result<context::PersonContext> {
        let person = self
            .repo
            .get_person(id)
            .with_context(|| format!("could not fetch person"))?
            .with_context(|| format!("no person found"))?;

        let offices_for_person = self
            .repo
            .list_person_office_incumbent_office(&id)
            .with_context(|| format!("could not query offices"))?;

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

        let past_tenures = self.repo.get_person_past_tenures(&id)?;

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

    pub fn fetch_office(&self, id: &str) -> Result<context::OfficeContext> {
        let name = self.repo.get_office_name(id)?;
        let photo = self.repo.get_entity_photo(graph::EntityType::Office, id)?;
        let contacts = self
            .repo
            .get_entity_contacts(&dto::EntityType::Office, id)?;
        let incumbent = self.repo.get_person_office_incumbent_person(id)?;
        let quondams = self.repo.list_person_office_quondam(id)?;
        let commit_date = self
            .repo
            .get_entity_commit_date(graph::EntityType::Office, id)?;

        // page
        let page = context::Page {
            base: "../".to_string(),
            dynamic: false,
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

    pub fn fetch_index(&self) -> Result<context::IndexContext> {
        let counts = self.repo.query_counts()?;

        Ok(context::IndexContext {
            persons: counts.persons,
            offices: counts.offices,
            page: Page {
                base: "./".to_string(),
                dynamic: false,
            },
            config: self.config.clone(),
        })
    }

    pub fn fetch_changes(&self) -> Result<context::ChangesContext> {
        let persons = self
            .repo
            .list_entity_uncommitted()?
            .into_iter()
            .map(|v| Person {
                id: v.id,
                name: v.name,
            })
            .collect();

        Ok(context::ChangesContext {
            changes: persons,
            page: Page {
                base: "./".to_string(),
                dynamic: false,
            },
            config: self.config.clone(),
        })
    }
}

pub struct Renderer {
    tera: Tera,
    output_format: OutputFormat,
}

impl Renderer {
    pub fn new(templates: &Path, output_format: OutputFormat) -> Result<Self> {
        let templates_glob = templates.join("**").join("*.html");
        let templates_glob_str = templates_glob
            .to_str()
            .with_context(|| format!("could not convert template path {:?}", templates))?;
        let tera = Tera::new(templates_glob_str)
            .with_context(|| format!("could not create Tera instance"))?;

        Ok(Renderer {
            tera,
            output_format,
        })
    }

    pub fn render_index(&self, context: &context::IndexContext) -> Result<String> {
        self.render(context, "index.html")
    }

    pub fn render_changes(&self, context: &context::ChangesContext) -> Result<String> {
        self.render(context, "changes.html")
    }

    pub fn render_person(&self, context: &PersonContext) -> Result<String> {
        self.render(context, "person.html")
    }
    
    pub fn render_person_edit(&self, context: &PersonEditContext) -> Result<String> {
        self.render(context, "person_edit.html")
    }

    pub fn render_office(&self, context: &OfficeContext) -> Result<String> {
        self.render(context, "office.html")
    }

    fn render<T: serde::Serialize>(&self, context: &T, template_name: &str) -> Result<String> {
        match self.output_format {
            OutputFormat::Json => {
                let str = serde_json::to_string(&context)?;

                Ok(str)
            }
            OutputFormat::Html => {
                let context = tera::Context::from_serialize(context)
                    .with_context(|| format!("could not create convert person to context"))?;
                self.tera.render(template_name, &context).with_context(|| {
                    format!("could not render template {} with context", template_name)
                })
            }
        }
    }
}

fn render_persons(
    context_fetcher: &ContextFetcher,
    renderer: &Renderer,
    output: &Path,
    output_format: OutputFormat,
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

        match output_format {
            OutputFormat::Json => {
                let output_path = person_path.join(format!("{}.json", person_context.person.id));
                fs::write(output_path.as_path(), str)
                    .with_context(|| format!("could not write rendered file {:?}", output_path))?;
            }
            OutputFormat::Html => {
                let output_path = person_path.join(format!("{}.html", person_context.person.id));

                fs::write(output_path.as_path(), str)
                    .with_context(|| format!("could not write rendered file {:?}", output_path))?;
            }
        }
    }

    Ok(())
}

fn render_offices(
    context_fetcher: &ContextFetcher,
    renderer: &Renderer,
    output: &Path,
    output_format: OutputFormat,
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
            .fetch_office(&id)
            .with_context(|| format!("could not fetch context for office {}", id))?;
        let str = renderer.render_office(&office_context)?;

        match output_format {
            OutputFormat::Json => {
                let output_path = office_path.join(format!("{}.json", office_context.office.id));
                fs::write(output_path.as_path(), str)
                    .with_context(|| format!("could not write rendered file {:?}", output_path))?;
            }
            OutputFormat::Html => {
                let output_path = office_path.join(format!("{}.html", office_context.office.id));

                fs::write(output_path.as_path(), str)
                    .with_context(|| format!("could not write rendered file {:?}", output_path))?;
            }
        }
    }

    Ok(())
}

pub fn create_search_database(search_db_path: &Path, db_path: &Path) -> Result<()> {
    let conn = Connection::open(search_db_path)
        .with_context(|| format!("could not create search database"))?;
    let conn = populate_search_database(conn, db_path)
        .with_context(|| format!("could not populate search database"))?;
    //
    // The error from `close` is `(Connection, Error)`, so we map it to just the error.
    conn.close()
        .map_err(|(_, err)| err)
        .with_context(|| format!("could not close search database"))?;

    Ok(())
}

pub fn create_search_database_in_memory(db_path: &Path) -> Result<Vec<u8>> {
    let conn = Connection::open_in_memory()
        .with_context(|| format!("could not create search database"))?;
    let conn = populate_search_database(conn, db_path)
        .with_context(|| format!("could not populate search database"))?;
    let db_bytes = conn
        .serialize("main")
        .with_context(|| format!("could not serialize search database"))?;

    Ok(db_bytes.to_vec())
}

fn populate_search_database(conn: Connection, db_path: &Path) -> Result<Connection> {
    let db_path_str = db_path
        .to_str()
        .with_context(|| format!("could not convert {:?} to str", db_path))?;
    conn.execute_batch(ENTITY_SCHEMA_SQL)
        .with_context(|| format!("could not setup search database"))?;
    conn.execute("ATTACH DATABASE ?1 AS db", [db_path_str])
        .with_context(|| format!("could not attach search database"))?;
    conn.execute_batch("INSERT INTO entity SELECT * FROM db.entity")
        .with_context(|| format!("could not copy data to search DB"))?;

    conn.execute_batch("DETACH DATABASE db")
        .with_context(|| format!("could not detach search database"))?;

    Ok(conn)
}
