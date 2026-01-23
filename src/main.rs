use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use include_sqlite_sql::{impl_sql, include_sql};
use serde::de::DeserializeOwned;
use static_toml::static_toml;
use std::fs;
use std::path::PathBuf;

use crate::record::RecordRepo;
use crate::record::sqlitebe::SqliteBackend;

mod augment;
mod context;
mod data;
mod dto;
mod export;
mod graph;
mod import;
mod ingest;
mod record;
mod render;
mod repo;
mod serve;

include_sql!("sql/schema.sql");
include_sql!("sql/library.sql");
include_sql!("sql/write.sql");

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
/// Maintain a database of government officers and generate a static website.
#[derive(Parser)]
#[command(version = env!("LONG_VERSION"), about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize the database
    Init {
        /// Path to the database file
        db: PathBuf,
    },

    /// Import data from the source directory into the database
    Import {
        /// Path to the database file
        db: PathBuf,
        source: PathBuf,
    },

    /// Export the database into the given directory.
    /// 
    /// The same can be imported into another database using the import command.
    Export {
        /// Path to the database file
        db: PathBuf,
        output: PathBuf,
    },

    /// Render the static website
    Render {
        /// Path to the database file
        db: PathBuf,
        output: PathBuf,
    },

    /// Serve the Web UI for viewing and mantaining the database
    Serve {
        /// Path to the database file
        db: PathBuf,
        #[arg(short = 'p', long)]
        port: Option<String>,
    },

    /// Pull the data from a remote and replace the working copy with it
    Pull {
        /// Path to the database file
        db: PathBuf,
        #[arg(short = 'p', long)]
        peer: String,
    },

    /// Show statistics for the database
    Stats {
        /// Path to the database file
        db: PathBuf,
    },

    /// Compact the database by removing data that is no longer referenced
    Gc {
        /// Path to the database file
        db: PathBuf,
    },

    Augment {
        /// Path to the database file
        db: PathBuf,
        #[arg(short = 's', long, value_enum)]
        source: Source,

        #[arg(short = 'f', long, value_enum)]
        fields: Vec<Field>,
    },

    Ingest {
        /// Path to the database file
        db: PathBuf,
        #[arg(short = 's', long, value_enum)]
        source: Source,

        #[arg(short = 'd', long)]
        directory: Option<PathBuf>,
    },

    /// Get a value from the database
    Get {
        /// Path to the database file
        db: PathBuf,
        /// The path to the value
        path: String,
    },

    /// List keys and values with the given path prefix in JSONL format
    List {
        /// Path to the database file
        db: PathBuf,
        /// The path prefix to list
        prefix: String,
    },

    /// Set a value in the database
    Set {
        /// Path to the database file
        db: PathBuf,
        /// The path to the value
        path: String,
        /// The value in JSON format
        value: String,
    },

    /// Delete a value from the database
    Delete {
        /// Path to the database file
        db: PathBuf,
        /// The path to the value
        path: String,
    },

    /// Show the database schema documentation
    Schema,
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Init { db } => {
            import::init(db.as_path()).with_context(|| "could not run `init`")
        }

        Commands::Import { db, source } => import::run(source.as_path(), db.as_path())
            .with_context(|| "could not run `import`"),

        Commands::Export { db, output } => export::run(db.as_path(), output.as_path())
            .with_context(|| "could not run `export`"),

        Commands::Render { db, output } => render::run(db.as_path(), output.as_path())
            .await
            .with_context(|| "could not run `render`"),
        Commands::Augment {
            db,
            source: source_name,
            fields,
        } => augment::run(db.as_path(), source_name, fields)
            .with_context(|| "could not run `augment`"),

        Commands::Ingest { db, source, directory } => {
            ingest::run(db.as_path(), source, directory.as_deref())
                .with_context(|| "could not run `ingest`")
        }

        Commands::Get { db, path } => {
            let conn = rusqlite::Connection::open(db)?;
            let repo = RecordRepo::new(&conn);
            let value = repo.working()?.get(&path)?;

            match value {
                Some(v) => println!("{}", serde_json::to_string_pretty(&v)?),
                None => println!("null"),
            }
            Ok(())
        }

        Commands::List { db, prefix } => {
            let conn = rusqlite::Connection::open(db)?;
            let repo = RecordRepo::new(&conn);
            let working = repo.working()?;

            for item in working.list(&prefix)? {
                let (path, value) = item?;
                let output = serde_json::json!({
                    "path": path,
                    "value": value,
                });
                println!("{}", serde_json::to_string(&output)?);
            }
            Ok(())
        }

        Commands::Set { db, path, value } => {
            let conn = rusqlite::Connection::open(db)?;
            let mut repo = RecordRepo::new(&conn);

            repo.working()?.save_from_json(&path, &value)?;
            repo.commit()?;
            Ok(())
        }

        Commands::Delete { db, path } => {
            let conn = rusqlite::Connection::open(db)?;
            let mut repo = RecordRepo::new(&conn);

            repo.working()?.delete_path(&path)?;
            repo.commit()?;
            Ok(())
        }

        Commands::Schema => {
            println!("{}", include_str!("../SCHEMA.md"));
            Ok(())
        }

        Commands::Serve { db, port } => serve::run(db, port.as_deref())
            .await
            .with_context(|| "failed to run `serve`"),

        Commands::Stats { db } => {
            let conn = rusqlite::Connection::open(db)?;
            let backend = SqliteBackend::new(&conn);
            let repo = repo::Repo::new(backend);
            let stats = repo.stats()?;

            println!("Repository Statistics:");
            println!("----------------------");
            println!("Total Key-Value pairs: {}", stats.key_value_count);
            println!("Total value size: {}", stats.total_value_size);
            println!("Value size distribution:");
            print_binned_distribution(stats.value_size_distribution);
            println!("");
            println!("Total nodes in DB: {}", stats.node_count);
            println!("Total nodes size: {}", stats.total_node_size);
            println!("Node size distribution:");
            print_binned_distribution(stats.node_size_distribution);

            Ok(())
        }

        Commands::Gc { db } => {
            let conn = rusqlite::Connection::open(db)?;
            let backend = SqliteBackend::new(&conn);
            let repo = repo::Repo::new(backend);
            let deleted = repo.gc()?;

            println!("Garbage collection finished. Deleted {} nodes.", deleted);

            Ok(())
        }

        Commands::Pull { db, peer } => {
            let peer_id = peer
                .parse::<iroh::EndpointId>()
                .map_err(|e| anyhow::anyhow!("failed to parse peer ID: {}", e))?;

            // 1. Capture old state
            let mut conn = rusqlite::Connection::open(&db)?;
            let repo = RecordRepo::new(&conn);
            let old_hash = repo.working()?.commit_id()?;

            // 2. Pull
            let manager = r2d2_sqlite::SqliteConnectionManager::file(&db);
            let pool = r2d2::Pool::new(manager)?;
            let backend = crate::record::sqlitebe::SqlitePoolBackend::new(pool);
            let client = repo::sync::client::RepoClient::new(backend);

            println!("Pulling from {}...", peer_id);
            client
                .pull(peer_id)
                .await
                .map_err(|e| anyhow::anyhow!("pull failed: {}", e))?;

            // 3. Re-index
            let diffs = {
                let new_working = repo.working()?;
                let new_hash = new_working.commit_id()?;

                if old_hash != new_hash {
                    println!("Updating indexes...");
                    let old_working = repo
                        .get_at(&old_hash)
                        .map_err(|e| anyhow::anyhow!("failed to get old ref: {}", e))?;

                    old_working
                        .iterate_diff(&new_working)
                        .map_err(|e| anyhow::anyhow!("diff failed: {}", e))?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| anyhow::anyhow!("diff iteration failed: {}", e))?
                } else {
                    Vec::new()
                }
            };

            if !diffs.is_empty() {
                let mut diffs = diffs;
                diffs.sort_by_key(|diff| {
                    use crate::record::{RecordDiff, RecordKey};
                    match diff {
                        RecordDiff::Added(RecordKey::Name(_), _)
                        | RecordDiff::Changed(RecordKey::Name(_), _, _) => 0,

                        RecordDiff::Added(_, _) | RecordDiff::Changed(_, _, _) => 1,

                        RecordDiff::Removed(RecordKey::Name(_), _) => 3,

                        RecordDiff::Removed(_, _) => 2,
                    }
                });

                let tx = conn.transaction()?;
                for diff in diffs {
                    match diff {
                        crate::record::RecordDiff::Added(k, v) => k.update_index(&tx, &v)?,
                        crate::record::RecordDiff::Changed(k, _, v) => k.update_index(&tx, &v)?,
                        crate::record::RecordDiff::Removed(k, _) => k.delete_index(&tx)?,
                    }
                }
                tx.commit()?;
                println!("Indexes updated.");
            }

            println!("Pull complete.");

            Ok(())
        }
    }
}

fn print_binned_distribution(dist: std::collections::BTreeMap<usize, usize>) {
    if dist.is_empty() {
        return;
    }

    let mut binned = std::collections::BTreeMap::new();

    for (size, count) in dist {
        let n = ((size as f64 / 4.0) + 1.0).log2().floor() as i32;
        let n = n.max(0) as u32;
        let left = 4 * ((1 << n) - 1);
        let right = 4 * ((1 << (n + 1)) - 1);
        *binned.entry((left, right)).or_insert(0) += count;
    }

    for ((left, right), count) in binned {
        println!("  {:>6} - {:>6} bytes: {:>5}", left, right - 1, count);
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