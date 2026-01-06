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
    LibrarySql, context,
    data::{self, SupervisingRelation},
    dto,
    serve::{AppError, AppState},
};

#[derive(Template, WebTemplate)]
#[template(path = "office/supervisor/add_partial.html")]
pub struct AddSupervisorPartial {
    id: String,
    missing_relations: Vec<data::SupervisingRelation>,
}

#[axum::debug_handler]
pub async fn add(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<AddSupervisorPartial, AppError> {
    let conn = state.get_conn()?;
    let mut supervisors: HashSet<data::SupervisingRelation> = HashSet::new();
    conn.get_office_supervising_offices(&id, |row| {
        supervisors.insert(row.get(0)?);

        Ok(())
    })?;
    let missing_relations = data::SupervisingRelation::VARIANTS
        .iter()
        .filter(|variant| !supervisors.contains(variant))
        .cloned()
        .collect();

    Ok(AddSupervisorPartial {
        id,
        missing_relations,
    })
}

#[derive(Template, WebTemplate)]
#[template(path = "office/supervisor/view_partial.html")]
pub struct ViewSupervisorPartial {
    id: String,
    supervisors: BTreeMap<data::SupervisingRelation, context::Office>,
}

impl ViewSupervisorPartial {
    pub fn new(conn: &Connection, id: String) -> Result<Self, AppError> {
        let mut supervisors: BTreeMap<data::SupervisingRelation, context::Office> = BTreeMap::new();
        conn.get_office_supervising_offices(&id, |row| {
            let relation = row.get(0)?;
            let supervising_office_id: String = row.get(1)?;
            let name =
                conn.get_entity_name(&dto::EntityType::Office, &supervising_office_id, |row| {
                    row.get(0)
                })?;
            supervisors.insert(
                relation,
                context::Office {
                    id: supervising_office_id,
                    name,
                },
            );

            Ok(())
        })?;
        Ok(ViewSupervisorPartial { id, supervisors })
    }
}

#[axum::debug_handler]
pub async fn view(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<ViewSupervisorPartial, AppError> {
    let conn = state.get_conn()?;

    ViewSupervisorPartial::new(&conn, id)
}

#[derive(Deserialize)]
pub struct SupervisorEntry {
    pub relation: SupervisingRelation,
    pub office_id: String,
}

#[axum::debug_handler]
pub async fn save(
    State(state): State<Arc<AppState>>,
    Path(office_id): Path<String>,
    Form(form): Form<SupervisorEntry>,
) -> Result<ViewSupervisorPartial, AppError> {
    let conn = state.get_conn()?;
    conn.save_office_supervisor(&office_id, &form.relation, &form.office_id)?;

    ViewSupervisorPartial::new(&conn, office_id)
}
