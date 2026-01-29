use std::path::Path;

use anyhow::Result;

use crate::data::{Data, indexer::Indexer};

pub async fn run(data_dir: &Path) -> Result<()> {
    let output_dir = data_dir.join("output");

    let data = Data::open(&data_dir)?;
    let data_commit_id = data.commit_id()?;

    let mut indexer = Indexer::open(&output_dir)?;
    let indexer_commit_id = indexer.commit_id()?;

    if let Some(old_id) = indexer_commit_id {
        if old_id == data_commit_id {
            return Ok(());
        }

        let diffs = data.diff(&old_id).await?;
        if !diffs.is_empty() {
            for diff in diffs {
                match diff {
                    crate::data::DataDiff::Added(id, item)
                    | crate::data::DataDiff::Modified(id, item) => match item {
                        crate::data::DataItem::Person(p) => indexer.add_person(&id, p)?,
                        crate::data::DataItem::Office(o) => indexer.add_office(&id, o)?,
                    },
                    crate::data::DataDiff::Deleted(id, _) => {
                        indexer.delete(&id)?;
                    }
                }
            }
        }
    } else {
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
    }

    indexer.commit(&data_commit_id)?;

    Ok(())
}
