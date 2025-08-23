use std::path::PathBuf;
use clap::{Parser};
use anyhow::{ensure, anyhow, Context, Result};
use rusqlite::Connection;
use std::fs;
use serde_derive::Deserialize;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    source: PathBuf,
    output: PathBuf,
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
    
    ensure!(!args.output.exists(), "output DB already exists at {:?}", args.output);
    
    // setup sqlite DB
    let conn = Connection::open(args.output.as_path())
        .with_context(|| format!("could not create sqlite DB at {:?}", args.output))?;

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
    let data_dir = args.source.join("person");
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
    let data_dir = args.source.join("office");
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
