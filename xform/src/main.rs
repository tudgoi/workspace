use std::path::PathBuf;
use clap::{Parser, Subcommand, ValueEnum};
use anyhow::{Context, Result};
use serde::{de::DeserializeOwned};
use std::{fs};

mod data;
mod context;
mod repo;
mod dto;
mod export;
mod render;
mod import;
mod ingest;

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

        #[arg(short='o', long, value_enum, default_value_t = OutputFormat::Html)]
        output_format: OutputFormat,
    },
    
    Ingest {
        db: PathBuf,
        #[arg(short='s', long, value_enum)]
        source: IngestionSource,
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum OutputFormat {
    Json,
    Html,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum IngestionSource {
    Wikidata,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    
    match args.command {
        Commands::Import { source, output} => import::run(source.as_path(), output.as_path())
            .with_context(|| "could not run `import`"),
        Commands::Export { db, output } => export::run(db.as_path(), output.as_path())
            .with_context(|| "could not run `export`"),
        Commands::Render {
            db,
            templates,
            output,
            output_format
        } => render::run(db.as_path(), templates.as_path(), output.as_path(), output_format)
            .with_context(|| "could not run `render`"),
        Commands::Ingest { db, source: source_name } => ingest::run(db.as_path(), source_name),
    }
}

fn from_toml_file<T>(path: PathBuf) -> Result<T> where T: DeserializeOwned {
    let str = fs::read_to_string(path.as_path())
        .with_context(|| format!("could not read toml file {:?}", path))?;
    let value = toml::from_str(&str)
        .with_context(|| format!("failed to parse toml file {:?}", path))?;

    Ok(value)
}
