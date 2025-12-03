pub mod name;
pub mod photo;

use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::{Form, extract::{Path, State}, response::Response};
use serde::Deserialize;

use crate::{
    LibrarySql, context, data, serve::{AppError, AppState, hx_redirect}
};

#[derive(Template, WebTemplate)]
#[template(path = "entity/new.html")]
pub struct NewTemplate {
    config: Arc<context::Config>,
    typ: String,
}

#[axum::debug_handler]
pub async fn new_form(
    State(state): State<Arc<AppState>>,
    Path(typ): Path<String>,
) -> Result<NewTemplate, AppError> {
    Ok(NewTemplate {
        typ,
        config: state.config.clone(),
    })
}

#[derive(Deserialize)]
pub struct NewForm {
    pub id: String,
    pub name: String,
}

#[axum::debug_handler]
pub async fn new(
    State(state): State<Arc<AppState>>,
    Path(typ): Path<String>,
    Form(form): Form<NewForm>,
) -> Result<Response, AppError> {
    let conn = state.get_conn()?;
    conn.new_entity(&typ, &form.id, &form.name)?;

    Ok(hx_redirect(&format!("/{}/{}/edit", &typ, &form.id))?)
}

#[derive(Template, WebTemplate)]
#[template(path = "entity/edit.html")]
pub struct EditTemplate {
    config: Arc<context::Config>,
    typ: String,
    id: String,
    name: String,
    photo: Option<data::Photo>,
}

#[axum::debug_handler]
pub async fn edit(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(String, String)>,
) -> Result<EditTemplate, AppError> {
    let conn = state.get_conn()?;
    let name = conn.get_entity_name(&typ, &id, |row| {
        let name: String = row.get(0)?;
        Ok(name)
    })?;
    let photo = match conn.get_entity_photo(&typ, &id, |row| {
        let url: String = row.get(0)?;
        let attribution: Option<String> = row.get(1)?;

        Ok(data::Photo { url, attribution })
    }) {
        Ok(photo) => Some(photo),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e.into()),
    };

    Ok(EditTemplate {
        id,
        typ,
        name,
        photo,
        config: state.config.clone(),
    })
}
