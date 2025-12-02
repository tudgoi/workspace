pub mod name;

use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::extract::{Path, State};

use crate::{LibrarySql, context, serve::{AppError, AppState}};

#[derive(Template, WebTemplate)]
#[template(path = "entity/edit.html")]
pub struct EditTemplate {
    config: Arc<context::Config>,
    typ: String,
    id: String,
    name: String,
}

#[axum::debug_handler]
pub async fn edit(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(String, String)>,
) -> Result<EditTemplate, AppError> {
    let conn = state.get_conn()?;
    let name = conn.get_entity(&typ, &id, |row| {
        let name: String = row.get(0)?;
        Ok(name)
    })?;

    Ok(EditTemplate {
        id,
        typ,
        name,
        config: state.config.clone(),
    })
}
