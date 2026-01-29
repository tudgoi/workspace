use std::path::Path;

use anyhow::Result;

use crate::{build, data::searcher::Searcher};

pub fn run(data_dir: &Path, query: &str) -> Result<()> {
    build::run(data_dir)?;

    let output_dir = data_dir.join("output");
    let searcher = Searcher::open(&output_dir)?;
    let results = searcher.search(&query)?;

    for result in results {
        println!("{}/{}", result.type_str, result.id);
    }

    Ok(())
}
