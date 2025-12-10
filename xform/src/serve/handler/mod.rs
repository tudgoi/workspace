use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::extract::State;
use rusqlite::Connection;

use crate::LibrarySql;
use crate::SchemaSql;
use crate::dto;
use crate::{
    context::{self, Page},
    serve::{AppError, AppState},
};

pub mod entity;
pub mod person;
pub mod office;
pub mod filters;

#[derive(Template, WebTemplate)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub persons: u32,
    pub offices: u32,
    pub config: Arc<context::Config>,
    pub page: context::Page,
}

pub async fn index(State(state): State<Arc<AppState>>) -> Result<IndexTemplate, AppError> {
    let conn = state.get_conn()?;

    let (persons, offices) = conn.get_entity_counts(|row| {
        let persons: u32 = row.get(0)?;
        let offices: u32 = row.get(1)?;

        Ok((persons, offices))
    })?;

    Ok(IndexTemplate {
        persons,
        offices,
        config: state.config.clone(),
        page: Page {
            dynamic: state.dynamic,
            base: String::from("./"),
        },
    })
}

#[derive(Template, WebTemplate)]
#[template(path = "uncommitted.html")]
pub struct UncommittedTemplate {
    pub entities: Vec<dto::Entity>,
    pub config: Arc<context::Config>,
    pub page: context::Page,
}

#[axum::debug_handler]
pub async fn uncommitted(
    State(state): State<Arc<AppState>>,
) -> Result<UncommittedTemplate, AppError> {
    let conn = state.get_conn()?;
    let mut entities = Vec::new();
    conn.get_entity_uncommitted(|row| {
        let entity = dto::Entity {
            typ: row.get(0)?,
            id: row.get(1)?,
            name: row.get(2)?,
        };
        entities.push(entity);

        Ok(())
    })?;

    Ok(UncommittedTemplate {
        entities,
        config: state.config.clone(),
        page: Page {
            base: String::from("./"),
            dynamic: true,
        },
    })
}

#[axum::debug_handler]
pub async fn search_db(State(state): State<Arc<AppState>>) -> Result<Vec<u8>, AppError> {
    let conn = Connection::open_in_memory()?;
    conn.create_entity_tables()?;
    let db_path_str = state
        .db
        .to_str()
        .ok_or_else(|| AppError::Unexpected(format!("could not convert path {:?}", state.db)))?;
    conn.attach_db(db_path_str)?;
    conn.copy_entity_from_db()?;
    conn.detach_db()?;
    let db_bytes = conn.serialize("main")?;

    Ok(db_bytes.to_vec())
}
