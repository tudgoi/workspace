use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::{Form, extract::{Path, State}};
use serde::Deserialize;

use crate::{LibrarySql, context, serve::{AppError, AppState}};

#[derive(Template, WebTemplate)]
#[template(path = "edit_entity.html")]
pub struct EditTemplate {
    config: Arc<context::Config>,
    typ: String,
    id: String,
    name: String,
}

#[axum::debug_handler]
pub async fn edit_entity(
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

#[derive(Template, WebTemplate)]
#[template(path = "edit_entity_name_partial.html")]
pub struct EditNameTemplate {
    typ: String,
    id: String,
    name: String,
    config: Arc<context::Config>,
}

#[axum::debug_handler]
pub async fn edit_entity_name(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(String, String)>,
) -> Result<EditNameTemplate, AppError> {
    let conn = state.get_conn()?;
    let name = conn.get_entity_name(&typ, &id, |row| {
        let name: String = row.get(0)?;
        Ok(name)
    })?;
    Ok(EditNameTemplate {
        id,
        typ,
        name,
        config: state.config.clone(),
    })
}

#[derive(Template, WebTemplate)]
#[template(path = "edit_entity.html", block = "name")]
pub struct ViewNameTemplate {
    typ: String,
    id: String,
    name: String,
    config: Arc<context::Config>,
}

#[axum::debug_handler]
pub async fn view_entity_name(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(String, String)>,
) -> Result<ViewNameTemplate, AppError> {
    let conn = state.get_conn()?;
    let name = conn.get_entity_name(&typ, &id, |row| {
        let name: String = row.get(0)?;
        Ok(name)
    })?;
    Ok(ViewNameTemplate {
        id,
        typ,
        name,
        config: state.config.clone(),
    })
}

#[derive(Deserialize)]
pub struct EditNameForm {
    name: String,
}
#[axum::debug_handler]
pub async fn put_entity_name(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(String, String)>,
    Form(form): Form<EditNameForm>,
) -> Result<ViewNameTemplate, AppError> {
    let conn = state.get_conn()?;
    conn.save_entity_name(&typ, &id, &form.name)?;

    Ok(ViewNameTemplate {
        id,
        typ,
        name: form.name,
        config: state.config.clone(),
    })
}