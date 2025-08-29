use std::path::PathBuf;
use anyhow::{ensure, Result, Context};
use rusqlite::Connection;

use super::data;
use super::from_toml_file;

pub fn run(source: PathBuf, output: PathBuf) -> Result<()> {
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
    
    conn.execute(
        "CREATE TABLE tenure (
            person_id TEXT NOT NULL,
            office_id TEXT NOT NULL,
            start TEXT NOT NULL,
            end TEXT
        )",
        ()
    ).with_context(|| format!("could not create `tenure` table"))?;
    
    conn.execute("
        CREATE VIEW incumbent (
            office_id,
            person_id
        ) AS SELECT office_id, person_id
        FROM tenure
        WHERE start IS NOT NULL AND end IS NULL", ()
    ).with_context(|| format!("could not create view `incumbent`"))?;
    
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

        let value: data::Person = from_toml_file(file_entry.path())
            .with_context(|| format!("could not load person"))?;
        let json = serde_json::to_string(&value)
            .with_context(|| format!("could not convert person to JSON"))?;
        conn.execute(
            "INSERT INTO person (id, data) VALUES (?1, ?2)",
        (id, json),
        ).with_context(|| format!("could not insert person into DB"))?;
        
        if let Some(tenures) = value.tenures {
            for tenure in tenures {
                conn.execute(
                    "INSERT INTO tenure (person_id, office_id, start, end) VALUES (?1, ?2, ?3, ?4)",
                    (id, tenure.office, tenure.start, tenure.end)
                ).with_context(|| format!("could not insert tenure into DB for {}", id))?;
            }
        }
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
        
        let value: data::Office = from_toml_file(file_entry.path())
            .with_context(|| format!("failed to parse template"))?;
        let json = serde_json::to_string(&value)
            .with_context(|| format!("could not convert office to JSON"))?;
        conn.execute(
            "INSERT INTO office (id, data) VALUES (?1, ?2)",
        (id, json),
        )?;
    }
    
    Ok(())
}
