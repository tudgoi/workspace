use std::{path::PathBuf};
use clap::{Parser, Subcommand};
use anyhow::{ensure, Context, Result};
use rusqlite::Connection;
use std::fs;
use serde_derive::{Serialize, Deserialize};
use tera::{Tera};
use tera;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Index {
        source: PathBuf,
        output: PathBuf,
    },
    
    Render {
        db: PathBuf,
        templates: PathBuf,
        output: PathBuf,
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct PersonRecord {
    id: String,
    person: Person,
    config: Config
}

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    title: String,
    base_url: String,
    contact: Contact,
    labels: Labels,
}

#[derive(Serialize, Deserialize, Debug)]
struct Contact {
    icons: Icons
}

#[derive(Serialize, Deserialize, Debug)]
struct Icons {
    phone: String,
    email: String,
    website: String,
    wikipedia: String,
    x: String,
    facebook: String,
    instagram: String,
    youtube: String,
    address: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Labels {
    adviser: String,
    during_the_pleasure_of: String,
    head: String,
    member_of: String,
    responsible_to: String,
    elected_by: String
}

#[derive(Serialize, Deserialize, Debug)]
struct Person {
    name: String,
    photo: Option<Photo>,
    link: Option<Link>,
    tenure: Option<Vec<Tenure>>
}

#[derive(Serialize, Deserialize, Debug)]
struct Photo {
    url: String,
    attribution: String
}

#[derive(Serialize, Deserialize, Debug)]
struct Link {
    wikipedia: Option<String>
}

#[derive(Serialize, Deserialize, Debug)]
struct Tenure {
    office: String,
    start: String,
    end: Option<String>
}

fn main() -> Result<()> {
    let args = Cli::parse();
    
    match args.command {
        Commands::Index { source, output} => run_index(source, output)
            .with_context(|| format!("error running `index` command"))?,
        Commands::Render { db, templates, output } => run_render(db, templates, output)
            .with_context(|| format!("error running `render` command"))?
    }

    Ok(())
}

fn run_index(source: PathBuf, output: PathBuf) -> Result<()> {
    ensure!(!output.exists(), "output DB already exists at {:?}", output);
    
    // setup sqlite DB
    let conn = Connection::open(output.as_path())
        .with_context(|| format!("could not create sqlite DB at {:?}", output))?;

    conn.execute(
        "CREATE TABLE person (
            id    TEXT PRIMARY KEY,
            name  TEXT NOT NULL
        )",
        (),
    ).with_context(|| format!("could not create `person` table"))?;

    conn.execute(
        "CREATE TABLE office (
            id    TEXT PRIMARY KEY,
            data  TEXT NOT NULL
        )",
        (),
    ).with_context(|| format!("could not create `office` table"))?;
    
    // process person
    let data_dir = source.join("person");
    let paths = data_dir.read_dir()
        .with_context(|| format!("could not open person directory {:?}",
            data_dir
        ))?;

    for path in paths {
        let file_entry = path.with_context(|| format!("could not read person data directory {:?}", data_dir))?;
        let file_path = file_entry.path();
        let file_stem = file_path.file_stem()
            .with_context(|| format!("invalid file name {:?} in person directory", file_path))?;
        let id = file_stem.to_str()
            .context(format!("could not convert filename {:?} to string", file_stem))?;
        let data = fs::read_to_string(file_entry.path())
            .with_context(|| format!("could not read person data file {:?}", file_entry.path()))?;
        let value: Person = toml::from_str(&data)
            .with_context(|| format!("Could not parse person from {:?}", file_entry.path()))?;
        conn.execute(
            "INSERT INTO person (id, name) VALUES (?1, ?2)",
        (id, value.name),
        )?;
    }
    
    // process office
    let data_dir = source.join("office");
    let paths = data_dir.read_dir()
        .with_context(|| format!("could not open office directory {:?}",
            data_dir
        ))?;

    for path in paths {
        let file_entry = path
            .with_context(|| format!("could not read office data directory {:?}", data_dir))?;
        let file_path = file_entry.path();
        let file_stem = file_path.file_stem()
            .with_context(|| format!("invalid file name {:?} in office directory", file_path))?;
        let id = file_stem.to_str()
            .context(format!("could not convert filename {:?} to string", file_stem))?;
        let data = fs::read_to_string(file_entry.path())
            .with_context(|| format!("could not read office data file {:?}", file_entry.path()))?;
        let value: Person = toml::from_str(&data)
            .with_context(|| format!("Could not parse office from {:?}", file_entry.path()))?;
        conn.execute(
            "INSERT INTO office (id, data) VALUES (?1, ?2)",
        (id, data),
        )?;
    }
    
    Ok(())
}

fn run_render(db: PathBuf, templates: PathBuf, output: PathBuf) -> Result<()> {
    // open template
    let templates_glob = templates
        .join("**")
        .join("*.html");
    let templates_glob_str = templates_glob
        .to_str()
        .context(format!("could not convert template path {:?}", templates))?;
    let tera = Tera::new(templates_glob_str)
        .with_context(|| format!("could not create Tera instance"))?;
    
    fs::create_dir(output.as_path())
        .with_context(|| format!{"could not create output dir {:?}", output})?;

    // open DB
    let conn = Connection::open(db.as_path())
        .with_context(|| format!("could not open DB at {:?}", db))?;
    
    // person
    let person_path = output.join("person");
    fs::create_dir(person_path.as_path())
        .with_context(|| format!("could not create person dir {:?}", person_path))?;

    let mut stmt = conn.prepare("SELECT id, name FROM person")
        .with_context(|| format!("could not create statement for reading person table"))?;
    let iter = stmt.query_map([], |row| {
        
        Ok(PersonRecord {
            id: row.get(0)?,
            person: Person {
                name: row.get(1)?,
                photo: None,
                link: None,
                tenure: None
            },
            config: Config {
                title: "The Title".to_string(),
                base_url: "http://arunkd13.org".to_string(),
                contact: Contact {
                    icons: Icons {
                        phone: String::new(),
                        email: String::new(),
                        website: String::new(),
                        wikipedia: String::new(),
                        x: String::new(),
                        facebook: String::new(),
                        instagram: String::new(),
                        youtube: String::new(),
                        address: String::new(),
                        
                    }
                },
                labels: Labels {
                    adviser: String::new(),
                    during_the_pleasure_of: String::new(),
                    head: String::new(),
                    member_of: String::new(),
                    responsible_to: String::new(),
                    elected_by: String::new()
                }
            }
        })
    }).with_context(|| format!("querying person table failed"))?;

    for result in iter {
        let record = result.with_context(|| format!("could not read person from DB"))?;
        let mut context = tera::Context::from_serialize(&record.person)
            .with_context(|| format!("could not create convert person to context"))?;
        context.insert("id", &record.id);
        context.insert("config", &record.config);
        context.insert("incomplete", &true);
        context.insert("path", "");
        let output_path = person_path.join(format!("{}.html", record.id));
        let str = tera.render("page.html", &context)
            .with_context(|| format!("could not render template"))?;
    
        fs::write(output_path.as_path(), str)
            .with_context(|| format!("could not write rendered file {:?}", output_path))?;
    }
    
    Ok(())
}