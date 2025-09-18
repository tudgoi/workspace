use anyhow::{Context, Result};
use serde_derive::Serialize;
use std::fs;
use std::path::Path;
use tera;
use tera::Tera;

use crate::dto;
use crate::repo::Repository;
use crate::{OutputFormat, context::PersonContext};

use super::{from_toml_file, repo};
use crate::context::{self, Maintenance, Page, Person};

pub fn run(db: &Path, templates: &Path, output: &Path, output_format: OutputFormat) -> Result<()> {
    let context_fetcher = ContextFetcher::new(db, templates)
        .with_context(|| format!("could not create context fetcher"))?;

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

    let renderer = Renderer::new(templates, output_format)?;

    let mut search_index = Vec::new();

    // persons
    render_persons(
        &context_fetcher,
        &renderer,
        output,
        output_format,
        &mut search_index,
    )
    .with_context(|| format!("could not render persons"))?;

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
    let output_path = render_dir.join(format!("index{}", extension));
    fs::write(output_path.as_path(), str)
        .with_context(|| format!("could not write rendered file {:?}", output_path))?;

    // write the search index file
    if output_format == OutputFormat::Html {
        let search_index_str = serde_json::to_string(&search_index)?;
        let search_dir = output.join("search");
        fs::create_dir(search_dir.as_path())?;
        let file_path = search_dir.join("index.json");
        fs::write(file_path, search_index_str)?;
    }

    Ok(())
}

pub struct ContextFetcher {
    config: context::Config,
    repo: Repository,
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

    pub fn fetch_person(&self, id: &str) -> Result<context::PersonContext> {
        let person = self.repo.get_person(id)
            .with_context(|| format!("could not fetch person"))?
            .with_context(|| format!("no person found"))?;

        let offices_for_person = self
            .repo
            .list_person_office_incumbent(&id)
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
        };

        // metadata
        let metadata = context::Metadata {
            maintenance: Maintenance { incomplete: true },
            commit_date: person.commit_date,
        };

        Ok(context::PersonContext {
            person: context::Person {
                id: id.to_string(),
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

    pub fn fetch_index(&self) -> Result<context::IndexContext> {
        let counts = self.repo.query_counts()?;

        Ok(context::IndexContext {
            persons: counts.persons,
            offices: counts.offices,
            page: Page {
                base: "./".to_string(),
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
            }).collect();

        Ok(context::ChangesContext {
            changes: persons,
            page: Page {
                base: "./".to_string(),
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
        self.render(context, "page.html")
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
                self.tera
                    .render(template_name, &context)
                    .with_context(|| format!("could not render template page.html with context"))
            }
        }
    }
}

#[derive(Serialize, Debug)]
struct SearchIndexEntry {
    title: String,
    url: String,
}

fn render_persons(
    context_fetcher: &ContextFetcher,
    renderer: &Renderer,
    output: &Path,
    output_format: OutputFormat,
    search_index: &mut Vec<SearchIndexEntry>,
) -> Result<()> {
    let output = output.join(match output_format {
        OutputFormat::Html => "html",
        OutputFormat::Json => "json",
    });
    // persons
    let person_path = output.join("person");
    fs::create_dir(person_path.as_path())
        .with_context(|| format!("could not create person dir {:?}", person_path))?;

    let person_ids = context_fetcher
        .repo
        .query_all_persons()
        .with_context(|| "could not query all persons")?;

    for id in person_ids {
        let person_context = context_fetcher
            .fetch_person(&id)
            .with_context(|| format!("could not fetch context for person {}", id))?;
        let str = renderer.render_person(&person_context)?;

        match output_format {
            OutputFormat::Json => {
                let output_path = person_path.join(format!("{}.json", person_context.person.id));
                fs::write(output_path.as_path(), str)
                    .with_context(|| format!("could not write rendered file {:?}", output_path))?;
            }
            OutputFormat::Html => {
                // add to search index
                let office_name = if let Some(ref offices) = person_context.offices {
                    if let Some(ref office) = offices.first() {
                        Some(&office.office.name)
                    } else {
                        None
                    }
                } else {
                    None
                };
                let title = if let Some(office_name) = office_name {
                    format!(
                        "{} ({}), {}",
                        person_context.person.name, person_context.person.id, office_name
                    )
                } else {
                    format!(
                        "{} ({})",
                        person_context.person.name, person_context.person.id
                    )
                };
                search_index.push(SearchIndexEntry {
                    title,
                    url: format!("./person/{}.html", person_context.person.id),
                });

                let output_path = person_path.join(format!("{}.html", person_context.person.id));

                fs::write(output_path.as_path(), str)
                    .with_context(|| format!("could not write rendered file {:?}", output_path))?;
            }
        }
    }

    Ok(())
}
