use std::path::Path;

use anyhow::Result;

use crate::data::{Data, indexer::Indexer};

pub fn run(data_dir: &Path) -> Result<()> {
    let output_dir = data_dir.join("output");

    let data = Data::open(&data_dir)?;
    let data_commit_id = data.commit_id()?;

    let mut indexer = Indexer::open(&output_dir)?;
    if let Some(indexer_commit_id) = indexer.commit_id()?
        && indexer_commit_id == data_commit_id
    {
        return Ok(());
    }

    for result in data.offices() {
        match result {
            Ok((id, office)) => {
                indexer.add_office(&id, office)?;
            }
            Err(crate::data::DataError::OfficeValidation(e)) => {
                eprintln!("{:?}", miette::Report::new(e));
            }
            Err(e) => return Err(e.into()),
        }
    }

    for result in data.persons() {
        match result {
            Ok((id, person)) => {
                indexer.add_person(&id, person)?;
            }
            Err(crate::data::DataError::PersonValidation(e)) => {
                eprintln!("{:?}", miette::Report::new(e));
            }
            Err(e) => return Err(e.into()),
        }
    }

    indexer.commit(&data_commit_id)?;

    Ok(())
}
