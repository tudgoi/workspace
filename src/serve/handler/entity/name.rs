use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::{
    Form,
    extract::{Path, State},
    response::IntoResponse,
};
use rusqlite::Connection;
use serde::Deserialize;

use crate::LibrarySql;
use crate::dto;
use crate::record::{Key, OfficePath, PersonPath, RecordRepo};
use crate::serve::{AppError, AppState};

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
    Ok(EditNamePartial { id, typ, name })
}

#[derive(Template, WebTemplate)]
#[template(path = "entity/name/view_partial.html")]
pub struct ViewNamePartial {
    typ: dto::EntityType,
    id: String,
    name: String,
    error: Option<String>,
    deletable: bool,
}

impl ViewNamePartial {
    pub fn new(
        conn: &Connection,
        typ: dto::EntityType,
        id: String,
        error: Option<String>,
    ) -> Result<ViewNamePartial, AppError> {
        let name = conn
            .get_entity_name(&typ, &id, |row| {
                let name: String = row.get(0)?;
                Ok(name)
            })
            .unwrap_or_else(|_| String::from("Deleted")); // Handle case where entity is deleted

        let repo = RecordRepo::new(conn);
        let other_props_exist = match typ {
            dto::EntityType::Person => {
                let key = Key::<PersonPath, ()>::new(&id);
                let items: Vec<_> = repo.working()?.scan(key)?.collect::<Result<Vec<_>, _>>()?;
                items.len() > 1
            }
            dto::EntityType::Office => {
                let key = Key::<OfficePath, ()>::new(&id);
                let items: Vec<_> = repo.working()?.scan(key)?.collect::<Result<Vec<_>, _>>()?;
                items.len() > 1
            }
        };

        Ok(ViewNamePartial {
            id,
            typ,
            name,
            error,
            deletable: !other_props_exist,
        })
    }
}

#[axum::debug_handler]
pub async fn view(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<ViewNamePartial, AppError> {
    let conn = state.get_conn()?;

    ViewNamePartial::new(&conn, typ, id, None)
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
    let repo = RecordRepo::new(&conn);
    match typ {
        dto::EntityType::Person => {
            repo.working()?
                .save(Key::<PersonPath, ()>::new(&id).name(), &form.name)?;
        }
        dto::EntityType::Office => {
            repo.working()?
                .save(Key::<OfficePath, ()>::new(&id).name(), &form.name)?;
        }
    }

    ViewNamePartial::new(&conn, typ, id, None)
}

#[axum::debug_handler]
pub async fn delete_handler(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<axum::response::Response, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);

    // Check if other properties exist
    let other_props_exist = match typ {
        dto::EntityType::Person => {
            let key = Key::<PersonPath, ()>::new(&id);
            let items: Vec<_> = repo.working()?.scan(key)?.collect::<Result<Vec<_>, _>>()?;
            items.len() > 1
        }
        dto::EntityType::Office => {
            let key = Key::<OfficePath, ()>::new(&id);
            let items: Vec<_> = repo.working()?.scan(key)?.collect::<Result<Vec<_>, _>>()?;
            items.len() > 1
        }
    };

    if other_props_exist {
        let partial = ViewNamePartial::new(
            &conn,
            typ,
            id,
            Some("Cannot delete name while other properties exist.".to_string()),
        )?;
        return Ok(partial.into_response());
    }

    match typ {
        dto::EntityType::Person => {
            repo.working()?
                .delete(Key::<PersonPath, ()>::new(&id).name())?;
        }
        dto::EntityType::Office => {
            repo.working()?
                .delete(Key::<OfficePath, ()>::new(&id).name())?;
        }
    }

    crate::serve::hx_redirect("/")
}
