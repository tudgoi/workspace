use std::{fs, path::Path};

use miette::Diagnostic;
use tantivy::{Index, IndexWriter, doc, schema::{Field, STORED, STRING, Schema, TEXT}};
use thiserror::Error;

use crate::data::{Office, Person};

pub struct Indexer {
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
}

impl Indexer {
    pub fn new(output_dir: &Path) -> Result<Self, IndexerError> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", STRING | STORED);
        let name_field = schema_builder.add_text_field("name", TEXT);
        let type_field = schema_builder.add_text_field("type", STRING | STORED);
        let schema = schema_builder.build();

        let path = output_dir.join("index");
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }

        let index = Index::create_in_dir(path, schema)?;
        let writer: IndexWriter = index.writer(50_000_000)?; // 50MB heap
                                                                       
        Ok(Indexer {
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
    
    pub fn commit(&mut self) -> Result<(), IndexerError> {
        self.writer.commit()?;

        Ok(())
    }
}