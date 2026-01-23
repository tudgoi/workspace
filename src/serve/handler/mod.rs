use std::collections::HashMap;
use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::extract::State;
use rusqlite::Connection;

use crate::CONFIG;
use crate::LibrarySql;
use crate::SchemaSql;
use crate::config::Config;
use crate::context::Metadata;
use crate::dto;
use crate::record::RecordDiff;
use crate::record::RecordKey;
use crate::record::RecordRepo;
use crate::{
    context::{self},
    serve::{AppError, AppState},
};

pub mod entity;
pub mod filters;
pub mod office;
pub mod person;

#[derive(Template, WebTemplate)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub persons: u32,
    pub offices: u32,
    pub config: &'static Config,
    pub page: context::Page,
    pub metadata: context::Metadata,
}

pub async fn index(State(state): State<Arc<AppState>>) -> Result<IndexTemplate, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);

    let (persons, offices) = conn.get_entity_counts(|row| {
        let persons: u32 = row.get(0)?;
        let offices: u32 = row.get(1)?;

        Ok((persons, offices))
    })?;
    let commit_id = repo.working()?.commit_id()?;

    Ok(IndexTemplate {
        persons,
        offices,
        config: &CONFIG,
        page: state.page_context(),
        metadata: Metadata {
            commit_id,
            maintenance: context::Maintenance { incomplete: false },
        },
    })
}

#[derive(Debug, Clone)]
pub struct EntityChange {
    pub entity: dto::Entity,
    pub removed: bool,
    pub diffs: Vec<RecordDiff>,
}

#[derive(Template, WebTemplate)]
#[template(path = "uncommitted.html")]
pub struct UncommittedTemplate {
    pub changes: Vec<EntityChange>,
    pub config: &'static Config,
    pub page: context::Page,
}

#[axum::debug_handler]
pub async fn uncommitted(
    State(state): State<Arc<AppState>>,
) -> Result<UncommittedTemplate, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);
    let mut changes = Vec::new();

    let mut entity_changes: HashMap<(dto::EntityType, String), (bool, Vec<RecordDiff>)> =
        HashMap::new();
    if let Ok(diff_iter) = repo.iterate_diff() {
        for diff_result in diff_iter {
            if let Ok(diff) = diff_result {
                let (info, removed) = match &diff {
                    RecordDiff::Added(rk, _) => (rk.entity_info(), false),
                    RecordDiff::Changed(rk, _, _) => (rk.entity_info(), false),
                    RecordDiff::Removed(rk, _) => {
                        (rk.entity_info(), matches!(rk, RecordKey::Name(_)))
                    }
                };

                let (_, current_diffs) =
                    entity_changes.entry(info).or_insert((removed, Vec::new()));
                current_diffs.push(diff);
            }
        }
    }

    for ((typ, id), (removed, diffs)) in entity_changes {
        let name = conn
            .get_entity_name(&typ, &id, |row| row.get(0))
            .unwrap_or_else(|_| id.clone());
        changes.push(EntityChange {
            entity: dto::Entity { typ, id, name },
            removed,
            diffs,
        });
    }

    changes.sort_by(|a, b| a.entity.name.cmp(&b.entity.name));

    Ok(UncommittedTemplate {
        changes,
        config: &CONFIG,
        page: state.page_context(),
    })
}

#[axum::debug_handler]
pub async fn commit(
    State(state): State<Arc<AppState>>,
) -> Result<axum::response::Response, AppError> {
    let conn = state.get_conn()?;
    let mut repo = RecordRepo::new(&conn);
    repo.commit()?;

    crate::serve::hx_redirect("/")
}

#[axum::debug_handler]
pub async fn abandon(
    State(state): State<Arc<AppState>>,
) -> Result<axum::response::Response, AppError> {
    let mut conn = state.get_conn()?;
    crate::record::abandon_changes(&mut conn)?;

    crate::serve::hx_redirect("/")
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
