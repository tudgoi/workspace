use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde::de::DeserializeOwned;
use std::fs;
use std::path::PathBuf;
use include_sqlite_sql::{include_sql, impl_sql};

mod augment;
mod context;
mod data;
mod dto;
mod export;
mod graph;
mod import;
mod ingest;
mod render;
mod repo;
mod serve;

include_sql!("sql/library.sql");
const ENTITY_SCHEMA_SQL: &str = include_str!("../schema/entity.sql");
const PROPERTY_SCHEMA_SQL: &str = include_str!("../schema/property.sql");

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Import {
        source: PathBuf,
        output: PathBuf,
    },

    Export {
        db: PathBuf,
        output: PathBuf,
    },

    Render {
        db: PathBuf,
        templates: PathBuf,
        output: PathBuf,
    },

    Augment {
        db: PathBuf,

        #[arg(short = 's', long, value_enum)]
        source: Source,

        #[arg(short = 'f', long, value_enum)]
        fields: Vec<Field>,
    },

    Ingest {
        db: PathBuf,

        #[arg(short = 's', long, value_enum)]
        source: Source,

        #[arg(short = 'd', long)]
        directory: Option<PathBuf>,
    },

    Serve {
        db: PathBuf,
        templates: PathBuf,
        static_files: PathBuf,

        #[arg(short = 'p', long)]
        port: Option<String>,
    },
}

#[derive(Clone, ValueEnum)]
enum Field {
    Wikidata,
    Photo,
    Wikipedia,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Source {
    Wikidata,
    Gemini,
    Json,
    Old,
}


fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Import { source, output } => import::run(source.as_path(), output.as_path())
            .with_context(|| "could not run `import`"),

        Commands::Export { db, output } => {
            export::run(db.as_path(), output.as_path()).with_context(|| "could not run `export`")
        }

        Commands::Render {
            db,
            templates,
            output,
        } => render::run(
            db.as_path(),
            templates.as_path(),
            output.as_path(),
        )
        .with_context(|| "could not run `render`"),
        Commands::Augment {
            db,
            source: source_name,
            fields,
        } => augment::run(db.as_path(), source_name, fields)
            .with_context(|| "could not run `augment`"),

        Commands::Ingest {
            db,
            source,
            directory,
        } => ingest::run(db.as_path(), source, directory.as_deref())
            .with_context(|| "could not run `ingest`"),

        Commands::Serve {
            db,
            templates,
            static_files,
            port,
        } => serve::run(db, templates, static_files, port.as_deref())
            .with_context(|| "failed to run `serve`"),
    }
}

fn from_toml_file<T>(path: PathBuf) -> Result<T>
where
    T: DeserializeOwned,
{
    let str = fs::read_to_string(path.as_path())
        .with_context(|| format!("could not read toml file {:?}", path))?;
    let value =
        toml::from_str(&str).with_context(|| format!("failed to parse toml file {:?}", path))?;

    Ok(value)
}
