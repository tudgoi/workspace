use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use include_sqlite_sql::{impl_sql, include_sql};
use serde::de::DeserializeOwned;
use static_toml::static_toml;
use std::fs;
use std::path::PathBuf;

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
include_sql!("sql/schema.sql");

static_toml! {
    pub static CONFIG = include_toml!("config.toml");
}

impl data::ContactType {
    pub fn icon(&self) -> &'static str {
        let icons = &CONFIG.icons;
        match self {
            Self::Address => icons.address,
            Self::Phone => icons.phone,
            Self::Email => icons.email,
            Self::Website => icons.website,
            Self::Wikipedia => icons.wikipedia,
            Self::X => icons.x,
            Self::Youtube => icons.youtube,
            Self::Facebook => icons.facebook,
            Self::Instagram => icons.instagram,
            Self::Wikidata => icons.wikidata,
        }
    }
}

impl Default for data::Photo {
    fn default() -> Self {
        Self {
            url: CONFIG.defaults.photo.url.to_string(),
            attribution: None,
        }
    }
}

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

        Commands::Render { db, output } => {
            render::run(db.as_path(), output.as_path()).with_context(|| "could not run `render`")
        }
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
            port,
        } => serve::run(db, port.as_deref()).with_context(|| "failed to run `serve`"),
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
