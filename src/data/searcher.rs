use miette::Diagnostic;
use std::path::Path;
use tantivy::{Index, IndexReader, collector::TopDocs, query::QueryParser, schema::Value};
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
pub enum SearcherError {
    #[error("tantivy error: {0}")]
    #[diagnostic(code(tudgoi::tantivy))]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("query parser error: {0}")]
    #[diagnostic(code(tudgoi::tantivy::query_parser))]
    QueryParser(#[from] tantivy::query::QueryParserError),
}

pub struct Searcher {
    index: Index,
    reader: IndexReader,
}

impl Searcher {
    pub fn open(output_dir: &Path) -> Result<Self, SearcherError> {
        let path = output_dir.join("index");
        let index = Index::open_in_dir(path)?;
        let reader = index.reader()?;
        Ok(Searcher { index, reader })
    }

    pub fn search(&self, query_str: &str) -> Result<Vec<SearchResult>, SearcherError> {
        let searcher = self.reader.searcher();
        let schema = self.index.schema();
        let id_field = schema.get_field("id").expect("id field should exist");
        let name_field = schema.get_field("name").expect("name field should exist");
        let type_field = schema.get_field("type").expect("type field should exist");

        let query_parser = QueryParser::for_index(&self.index, vec![id_field, name_field]);
        let query = query_parser.parse_query(query_str)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

        let mut results = Vec::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
            let id = retrieved_doc
                .get_first(id_field)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let type_str = retrieved_doc
                .get_first(type_field)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            results.push(SearchResult { id, type_str });
        }

        Ok(results)
    }
}

pub struct SearchResult {
    pub id: String,
    pub type_str: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{Person, Office};
    use crate::data::indexer::Indexer;
    use tempfile::tempdir;

    #[test]
    fn test_search_by_id() {
        let tmp_dir = tempdir().unwrap();
        let mut indexer = Indexer::open(tmp_dir.path()).unwrap();
        
        indexer.add_person("p1", Person {
            name: "Person One".to_string(),
            photo: None,
            contacts: None,
            tenures: None,
        }).unwrap();
        
        indexer.add_office("o1", Office {
            name: "Office One".to_string(),
            photo: None,
            contacts: None,
            supervisors: None,
        }).unwrap();
        
        indexer.commit("test").unwrap();
        
        let searcher = Searcher::open(tmp_dir.path()).unwrap();
        
        let results = searcher.search("p1").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "p1");
        
        let results = searcher.search("o1").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "o1");

        let results = searcher.search("Person").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "p1");
    }
}
