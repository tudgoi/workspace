use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde::de::DeserializeOwned;
use std::fs;
use std::path::PathBuf;

mod context;
mod data;
mod dto;
mod export;
mod import;
mod augment;
mod render;
mod repo;

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

    Augment {
        db: PathBuf,

        #[arg(short = 's', long, value_enum)]
        source: Source,

        #[arg(short = 'f', long, value_enum)]
        fields: Vec<Field>,
    },
}

#[derive(Clone, ValueEnum)]
enum Field {
    Wikidata,
    Photo,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum OutputFormat {
    Json,
    Html,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Source {
    Wikidata,
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
            output_format,
        } => render::run(
            db.as_path(),
            templates.as_path(),
            output.as_path(),
            output_format,
        )
        .with_context(|| "could not run `render`"),
        Commands::Augment {
            db,
            source: source_name,
            fields,
        } => augment::run(db.as_path(), source_name, fields),
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
