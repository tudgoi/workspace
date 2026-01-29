use std::{
    fs,
    path::{Path, PathBuf},
};

use miette::Diagnostic;
use tantivy::{
    Index, IndexWriter, doc,
    schema::{Field, STORED, STRING, Schema, TEXT},
};
use thiserror::Error;

use crate::data::{Office, Person};

const COMMIT_ID_FILE: &str = "commit_id";

pub struct Indexer {
    path: PathBuf,
    type_field: Field,
    id_field: Field,
    name_field: Field,
    writer: IndexWriter,
}

#[derive(Error, Debug, Diagnostic)]
pub enum IndexerError {
    #[error("io error: {0}")]
    #[diagnostic(code(tudgoi::io))]
    Io(#[from] std::io::Error),

    #[error("tantivy error: {0}")]
    #[diagnostic(code(tudgoi::tantivy))]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("tantivy could not open directory: {0}")]
    #[diagnostic(code(tudgoi::tantivy::od))]
    OpenDirectory(#[from] tantivy::directory::error::OpenDirectoryError),
}

impl Indexer {
    pub fn open(output_dir: &Path) -> Result<Self, IndexerError> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", STRING | STORED);
        let name_field = schema_builder.add_text_field("name", TEXT);
        let type_field = schema_builder.add_text_field("type", STRING | STORED);
        let schema = schema_builder.build();

        let path = output_dir.join("index");
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        let directory = tantivy::directory::MmapDirectory::open(&path)?;
        let index = Index::open_or_create(directory, schema)?;
        let writer: IndexWriter = index.writer(50_000_000)?; // 50MB heap

        Ok(Indexer {
            path,
            type_field,
            id_field,
            name_field,
            writer,
        })
    }

    pub fn add_person(&mut self, id: &str, person: Person) -> Result<(), IndexerError> {
        self.writer.add_document(doc!(
            self.id_field => id,
            self.name_field => person.name,
            self.type_field => "person",
        ))?;

        Ok(())
    }

    pub fn add_office(&mut self, id: &str, office: Office) -> Result<(), IndexerError> {
        self.writer.add_document(doc!(
            self.id_field => id,
            self.name_field => office.name,
            self.type_field => "office",
        ))?;

        Ok(())
    }

    pub fn commit(&mut self, id: &str) -> Result<(), IndexerError> {
        self.writer.commit()?;
        fs::write(self.path.join(COMMIT_ID_FILE), id)?;

        Ok(())
    }

    pub fn commit_id(&self) -> Result<Option<String>, IndexerError> {
        let commit_id_file = self.path.join(COMMIT_ID_FILE);
        if commit_id_file.try_exists()? {
            let id = fs::read_to_string(commit_id_file)?;

            Ok(Some(id))
        } else {
            Ok(None)
        }
    }
}
