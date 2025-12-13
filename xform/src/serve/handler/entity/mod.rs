pub mod name;
pub mod photo;
pub mod contact;

use std::{collections::BTreeMap, sync::Arc};

use askama::Template;
use askama_web::WebTemplate;
use axum::{
    Form,
    extract::{Path, State},
    response::Response,
};
use rusqlite::OptionalExtension;
use serde::Deserialize;
use strum::VariantArray;

use crate::{
    LibrarySql, context, data, dto,
    serve::{AppError, AppState, hx_redirect},
};

#[derive(Template, WebTemplate)]
#[template(path = "entity/new.html")]
pub struct NewTemplate {
    config: Arc<context::Config>,
    page: context::Page,
    typ: dto::EntityType,
}

#[axum::debug_handler]
pub async fn new_form(
    State(state): State<Arc<AppState>>,
    Path(typ): Path<dto::EntityType>,
) -> Result<NewTemplate, AppError> {
    Ok(NewTemplate {
        typ,
        config: state.config.clone(),
        page: context::Page {
            dynamic: state.dynamic,
            base: String::from("../"),
        },
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
    Path(typ): Path<dto::EntityType>,
    Form(form): Form<NewForm>,
) -> Result<Response, AppError> {
    let conn = state.get_conn()?;
    conn.new_entity(&typ, &form.id, &form.name)?;

    Ok(hx_redirect(&format!("/{}/{}/edit", typ, &form.id))?)
}

#[derive(Template, WebTemplate)]
#[template(path = "entity/edit.html")]
pub struct EditTemplate {
    pub typ: dto::EntityType,
    pub id: String,
    pub name: String,
    pub photo: Option<data::Photo>,
    pub contacts: BTreeMap<data::ContactType, String>,

    pub config: Arc<context::Config>,
    pub page: context::Page,
}

#[axum::debug_handler]
pub async fn edit(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<EditTemplate, AppError> {
    let conn = state.get_conn()?;
    let name = conn.get_entity_name(&typ, &id, |row| {
        let name: String = row.get(0)?;
        Ok(name)
    })?;
    let photo = conn
        .get_entity_photo(&typ, &id, |row| {
            Ok(data::Photo {
                url: row.get(0)?,
                attribution: row.get(1)?,
            })
        })
        .optional()?;
    let mut contacts: BTreeMap<data::ContactType, String> = BTreeMap::new();
    conn.get_entity_contacts(&dto::EntityType::Person, &id, |row| {
        contacts.insert(row.get(0)?, row.get(1)?);

        Ok(())
    })?;

    Ok(EditTemplate {
        id,
        typ,
        name,
        photo,
        contacts,
        config: state.config.clone(),
        page: context::Page {
            dynamic: state.dynamic,
            base: String::from("../../"),
        },
    })
}
