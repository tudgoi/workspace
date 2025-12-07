use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::{Form, extract::{Path, State}};
use serde::Deserialize;

use crate::serve::{AppError, AppState};
use crate::LibrarySql;
use crate::dto;

#[derive(Template, WebTemplate)]
#[template(path = "entity/name/edit_partial.html")]
pub struct EditNamePartial {
    typ: dto::EntityType,
    id: String,
    name: String,
}

#[axum::debug_handler]
pub async fn edit(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<EditNamePartial, AppError> {
    let conn = state.get_conn()?;
    let name = conn.get_entity_name(&typ, &id, |row| {
        let name: String = row.get(0)?;
        Ok(name)
    })?;
    Ok(EditNamePartial {
        id,
        typ,
        name,
    })
}

#[derive(Template, WebTemplate)]
#[template(path = "entity/edit.html", block = "name")]
pub struct ViewNamePartial {
    typ: dto::EntityType,
    id: String,
    name: String,
}

#[axum::debug_handler]
pub async fn view(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<ViewNamePartial, AppError> {
    let conn = state.get_conn()?;
    let name = conn.get_entity_name(&typ, &id, |row| {
        let name: String = row.get(0)?;
        Ok(name)
    })?;
    Ok(ViewNamePartial {
        id,
        typ,
        name,
    })
}

#[derive(Deserialize)]
pub struct EditNameForm {
    name: String,
}
#[axum::debug_handler]
pub async fn save(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
    Form(form): Form<EditNameForm>,
) -> Result<ViewNamePartial, AppError> {
    let conn = state.get_conn()?;
    conn.save_entity_name(&typ, &id, &form.name)?;
    
    let name = conn.get_entity_name(&typ, &id, |row| {
        let name: String = row.get(0)?;
        Ok(name)
    })?;

    Ok(ViewNamePartial {
        id,
        typ,
        name,
    })
}