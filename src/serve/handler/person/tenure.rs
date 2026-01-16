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
    office_id: Option<String>,
    start: Option<String>,
    end: Option<String>,
    error: Option<String>,
}

#[axum::debug_handler]
pub async fn add(Path(id): Path<String>) -> Result<AddTenurePartial, AppError> {
    Ok(AddTenurePartial {
        id,
        office_id: None,
        start: None,
        end: None,
        error: None,
    })
}

#[derive(Template, WebTemplate)]
#[template(path = "person/tenure/edit_partial.html")]
pub struct EditTenurePartial {
    id: String,
    tenure: data::Tenure,
    error: Option<String>,
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

    Ok(EditTenurePartial { id, tenure, error: None })
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
    pub start: Option<String>,
    pub end: Option<String>,
}

fn parse_date(s: Option<String>) -> Result<Option<NaiveDate>, String> {
    match s {
        Some(s) if !s.is_empty() => {
            NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .map(Some)
                .map_err(|e| format!("Invalid date format: {}", e))
        }
        _ => Ok(None),
    }
}

#[axum::debug_handler]
pub async fn save_add(
    State(state): State<Arc<AppState>>,
    Path(person_id): Path<String>,
    Form(form): Form<TenureEntry>,
) -> Result<Response, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);

    let start_date = parse_date(form.start.clone());
    let end_date = parse_date(form.end.clone());

    let result = match (start_date, end_date) {
        (Ok(start), Ok(end)) => {
            repo.working()?.save(
                Key::<PersonPath, ()>::new(&person_id).tenure(&form.office_id, start),
                &end,
            ).map_err(AppError::from)
        }
        (Err(e), _) => Err(AppError::from(e)),
        (_, Err(e)) => Err(AppError::from(e)),
    };

    match result {
        Ok(_) => {
            let partial = ViewTenurePartial::new(&conn, person_id)?;
            let mut response = partial.into_response();
            response
                .headers_mut()
                .insert("HX-Trigger", "entity_updated".parse().unwrap());
            Ok(response)
        }
        Err(e) => {
            Ok(AddTenurePartial {
                id: person_id,
                office_id: Some(form.office_id),
                start: form.start,
                end: form.end,
                error: Some(e.to_string()),
            }.into_response())
        }
    }
}

#[axum::debug_handler]
pub async fn save_edit(
    State(state): State<Arc<AppState>>,
    Path(person_id): Path<String>,
    Form(form): Form<TenureEntry>,
) -> Result<Response, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);

    let start_date = parse_date(form.start.clone());
    let end_date = parse_date(form.end.clone());

    let result = match (start_date, end_date) {
        (Ok(start), Ok(end)) => {
            repo.working()?.save(
                Key::<PersonPath, ()>::new(&person_id).tenure(&form.office_id, start),
                &end,
            ).map_err(AppError::from)
        }
        (Err(e), _) => Err(AppError::from(e)),
        (_, Err(e)) => Err(AppError::from(e)),
    };

    match result {
        Ok(_) => {
            let partial = ViewTenurePartial::new(&conn, person_id)?;
            let mut response = partial.into_response();
            response
                .headers_mut()
                .insert("HX-Trigger", "entity_updated".parse().unwrap());
            Ok(response)
        }
        Err(e) => {
            Ok(EditTenurePartial {
                id: person_id,
                tenure: data::Tenure {
                    office_id: form.office_id,
                    start: form.start,
                    end: form.end,
                },
                error: Some(e.to_string()),
            }.into_response())
        }
    }
}

#[derive(Deserialize)]
pub struct DeleteTenureEntry {
    pub office_id: String,
    pub start: Option<String>,
}

#[axum::debug_handler]
pub async fn delete(
    State(state): State<Arc<AppState>>,
    Path(person_id): Path<String>,
    Form(form): Form<DeleteTenureEntry>,
) -> Result<Response, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);

    let start_date = parse_date(form.start.clone()).map_err(AppError::from)?;

    repo.working()?
        .delete(Key::<PersonPath, ()>::new(&person_id).tenure(&form.office_id, start_date))?;

    let partial = ViewTenurePartial::new(&conn, person_id)?;
    let mut response = partial.into_response();
    response
        .headers_mut()
        .insert("HX-Trigger", "entity_updated".parse().unwrap());
    Ok(response)
}
