use std::path::PathBuf;
use clap::{Parser, Subcommand, ValueEnum};
use anyhow::{Context, Result};
use serde::{de::DeserializeOwned};
use std::{fs};

mod data;
mod context;
mod dto;
mod render;
mod index;

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

        #[arg(short='o', long, value_enum, default_value_t = OutputFormat::Html)]
        output_format: OutputFormat,
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum OutputFormat {
    Json,
    Html
}


fn main() -> Result<()> {
    let args = Cli::parse();
    
    match args.command {
        Commands::Index { source, output} => index::run(source, output)
            .with_context(|| format!("error running `index` command"))?,
        Commands::Render {
            db,
            templates,
            output,
            output_format
        } => render::run(db, templates, output, output_format)
            .with_context(|| format!("error running `render` command"))?
    }

    Ok(())
}

fn from_toml_file<T>(path: PathBuf) -> Result<T> where T: DeserializeOwned {
    let str = fs::read_to_string(path.as_path())
        .with_context(|| format!("could not read toml file {:?}", path))?;
    let value = toml::from_str(&str)
        .with_context(|| format!("failed to parse toml file {:?}", path))?;

    Ok(value)
}
