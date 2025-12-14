use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use askama::Template;
use askama_web::WebTemplate;
use axum::{
    Form,
    extract::{Path, State},
};
use rusqlite::Connection;
use serde::Deserialize;
use strum::VariantArray;

use crate::{
    LibrarySql, data, dto,
    serve::{AppError, AppState},
};

#[derive(Template, WebTemplate)]
#[template(path = "entity/contact/add_partial.html")]
pub struct AddContactPartial {
    typ: dto::EntityType,
    id: String,
    missing_contacts: Vec<data::ContactType>,
}

#[axum::debug_handler]
pub async fn add(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<AddContactPartial, AppError> {
    let conn = state.get_conn()?;
    let mut contacts: HashSet<data::ContactType> = HashSet::new();
    conn.get_entity_contacts(&typ, &id, |row| {
        contacts.insert(row.get(0)?);

        Ok(())
    })?;
    let missing_contacts = data::ContactType::VARIANTS
        .iter()
        .filter(|variant| !contacts.contains(variant))
        .cloned()
        .collect();

    Ok(AddContactPartial {
        id,
        typ,
        missing_contacts,
    })
}

#[derive(Template, WebTemplate)]
#[template(path = "entity/contact/view_partial.html")]
pub struct ViewContactPartial {
    typ: dto::EntityType,
    id: String,
    contacts: BTreeMap<data::ContactType, String>,
}

impl ViewContactPartial {
    pub fn new(conn: &Connection, typ: dto::EntityType, id: String) -> Result<Self, AppError> {
        let mut contacts: BTreeMap<data::ContactType, String> = BTreeMap::new();
        conn.get_entity_contacts(&typ, &id, |row| {
            contacts.insert(row.get(0)?, row.get(1)?);

            Ok(())
        })?;
        Ok(ViewContactPartial {
            id,
            typ: typ,
            contacts,
        })
    }
}

#[axum::debug_handler]
pub async fn view(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<ViewContactPartial, AppError> {
    let conn = state.get_conn()?;
    
    ViewContactPartial::new(&conn, typ, id)
}

#[derive(Deserialize)]
pub struct ContactEntry {
    pub contact_type: data::ContactType,
    pub value: String,
}

#[axum::debug_handler]
pub async fn save(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
    Form(contact_form): Form<ContactEntry>,
) -> Result<ViewContactPartial, AppError> {
    let conn = state.get_conn()?;
    conn.save_entity_contact(&typ, &id, &contact_form.contact_type, &contact_form.value)?;

    ViewContactPartial::new(&conn, typ, id)
}
