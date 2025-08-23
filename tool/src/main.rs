use std::{path::PathBuf};
use clap::{Parser, Subcommand};
use anyhow::{ensure, Context, Result};
use rusqlite::Connection;
use std::fs;
use serde_derive::{Deserialize};
use tera::{Tera};

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

#[derive(Deserialize, Debug)]
struct Person {
    name: String,
    photo: Option<Photo>,
    link: Option<Link>,
    tenure: Option<Vec<Tenure>>
}

#[derive(Deserialize, Debug)]
struct Photo {
    url: String,
    attribution: String
}

#[derive(Deserialize, Debug)]
struct Link {
    wikipedia: Option<String>
}

#[derive(Deserialize, Debug)]
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
            data  TEXT NOT NULL
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
            "INSERT INTO person (id, data) VALUES (?1, ?2)",
        (id, data),
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
    let templates_glob = templates
        .join("**")
        .join("*.html");
    let templates_glob_str = templates_glob
        .to_str()
        .context(format!("could not convert template path {:?}", templates))?;
    let tera = Tera::new(templates_glob_str)
        .with_context(|| format!("could not create Tera instance"))?;
    
    // person
    let mut context = tera::Context::new();
    context.insert("name", "Droupadi Murmu");
    let str = tera.render("person.html", &context)
        .with_context(|| format!("could not render template"))?;
    
    println!("{}", str);

    fs::create_dir(output.as_path())
        .with_context(|| format!("could not create output directory {:?}", output))?;

    Ok(())
}