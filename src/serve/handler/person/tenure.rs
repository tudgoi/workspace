use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::{
    Form,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};
use chrono::NaiveDate;
use rusqlite::Connection;
use serde::Deserialize;

use crate::{
    LibrarySql, data,
    record::{Key, PersonPath, RecordRepo},
    serve::{AppState, handler::AppError},
};

#[derive(Template, WebTemplate)]
#[template(path = "person/tenure/add_partial.html")]
pub struct AddTenurePartial {
    id: String,
}

#[axum::debug_handler]
pub async fn add(Path(id): Path<String>) -> Result<AddTenurePartial, AppError> {
    Ok(AddTenurePartial { id })
}

#[derive(Template, WebTemplate)]
#[template(path = "person/tenure/edit_partial.html")]
pub struct EditTenurePartial {
    id: String,
    tenure: data::Tenure,
}

#[derive(Deserialize)]
pub struct EditTenureParams {
    pub office_id: String,
    pub start: Option<String>,
}

#[axum::debug_handler]
pub async fn edit(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<EditTenureParams>,
) -> Result<EditTenurePartial, AppError> {
    let conn = state.get_conn()?;
    let mut tenures = Vec::new();
    conn.get_tenures(&id, |row| {
        let office_id: String = row.get(0)?;
        let start: Option<String> = row.get(1)?;
        let end: Option<String> = row.get(2)?;
        
        if office_id == params.office_id && start == params.start {
             tenures.push(data::Tenure {
                office_id,
                start,
                end,
            });
        }
       
        Ok(())
    })?;

    let tenure = tenures.into_iter().next().ok_or_else(|| AppError::from("Tenure not found".to_string()))?;

    Ok(EditTenurePartial { id, tenure })
}

#[derive(Template, WebTemplate)]
#[template(path = "person/tenure/view_partial.html")]
pub struct ViewTenurePartial {
    id: String,
    tenures: Vec<data::Tenure>,
}

impl ViewTenurePartial {
    pub fn new(conn: &Connection, id: String) -> Result<Self, AppError> {
        let mut tenures = Vec::new();
        conn.get_tenures(&id, |row| {
            tenures.push(data::Tenure {
                office_id: row.get(0)?,
                start: row.get(1)?,
                end: row.get(2)?,
            });

            Ok(())
        })?;

        Ok(ViewTenurePartial { id, tenures })
    }
}

#[axum::debug_handler]
pub async fn view(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<ViewTenurePartial, AppError> {
    let conn = state.get_conn()?;

    ViewTenurePartial::new(&conn, id)
}

#[derive(Deserialize)]
pub struct TenureEntry {
    pub office_id: String,
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
}

#[axum::debug_handler]
pub async fn save(
    State(state): State<Arc<AppState>>,
    Path(person_id): Path<String>,
    Form(form): Form<TenureEntry>,
) -> Result<Response, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);
    repo.working()?.save(
        Key::<PersonPath, ()>::new(&person_id).tenure(&form.office_id, form.start),
        &form.end,
    )?;

    let partial = ViewTenurePartial::new(&conn, person_id)?;
    let mut response = partial.into_response();
    response
        .headers_mut()
        .insert("HX-Trigger", "entity_updated".parse().unwrap());
    Ok(response)
}

#[derive(Deserialize)]
pub struct DeleteTenureEntry {
    pub office_id: String,
    pub start: Option<NaiveDate>,
}

#[axum::debug_handler]
pub async fn delete(
    State(state): State<Arc<AppState>>,
    Path(person_id): Path<String>,
    Form(form): Form<DeleteTenureEntry>,
) -> Result<Response, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);
    repo.working()?
        .delete(Key::<PersonPath, ()>::new(&person_id).tenure(&form.office_id, form.start))?;

    let partial = ViewTenurePartial::new(&conn, person_id)?;
    let mut response = partial.into_response();
    response
        .headers_mut()
        .insert("HX-Trigger", "entity_updated".parse().unwrap());
    Ok(response)
}
