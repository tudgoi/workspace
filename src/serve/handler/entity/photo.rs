use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::{
    Form,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use rusqlite::{Connection, OptionalExtension};

use crate::record::{Key, OfficePath, PersonPath, RecordRepo};
use crate::{LibrarySql, dto};
use crate::{
    data,
    serve::{AppError, AppState},
};

#[derive(Template, WebTemplate)]
#[template(path = "entity/photo/edit_partial.html")]
pub struct EditPhotoPartial {
    typ: dto::EntityType,
    id: String,
    url: String,
    attribution: String,
}

#[axum::debug_handler]
pub async fn edit(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<EditPhotoPartial, AppError> {
    let conn = state.get_conn()?;
    let photo = conn
        .get_entity_photo(&typ, &id, |row| {
            Ok(data::Photo {
                url: row.get(0)?,
                attribution: row.get(1)?,
            })
        })
        .optional()?;
    let (url, attribution) = if let Some(photo) = photo {
        (photo.url, photo.attribution.unwrap_or_default())
    } else {
        (String::new(), String::new())
    };

    Ok(EditPhotoPartial {
        typ,
        id,
        url,
        attribution,
    })
}

#[derive(Template, WebTemplate)]
#[template(path = "entity/photo/view_partial.html")]
pub struct ViewPhotoPartial {
    typ: dto::EntityType,
    id: String,
    photo: Option<data::Photo>,
}

impl ViewPhotoPartial {
    pub fn new(conn: &Connection, typ: dto::EntityType, id: String) -> Result<Self, AppError> {
        let photo = conn
            .get_entity_photo(&typ, &id, |row| {
                Ok(data::Photo {
                    url: row.get(0)?,
                    attribution: row.get(1)?,
                })
            })
            .optional()?;
        Ok(ViewPhotoPartial { id, typ, photo })
    }
}

#[axum::debug_handler]
pub async fn view(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<ViewPhotoPartial, AppError> {
    let conn = state.get_conn()?;

    ViewPhotoPartial::new(&conn, typ, id)
}

#[axum::debug_handler]
pub async fn save(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
    Form(photo_form): Form<data::Photo>, // Renamed to avoid conflict with `photo` variable below
) -> Result<Response, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);
    match typ {
        dto::EntityType::Person => {
            repo.working()?
                .save(Key::<PersonPath, ()>::new(&id).photo(), &photo_form)?;
        }
        dto::EntityType::Office => {
            repo.working()?
                .save(Key::<OfficePath, ()>::new(&id).photo(), &photo_form)?;
        }
    }

    let partial = ViewPhotoPartial::new(&conn, typ, id)?;
    let mut response = partial.into_response();
    response
        .headers_mut()
        .insert("HX-Trigger", "entity_updated".parse().unwrap());
    Ok(response)
}

#[axum::debug_handler]
pub async fn delete(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<Response, AppError> {
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);
    match typ {
        dto::EntityType::Person => {
            repo.working()?
                .delete(Key::<PersonPath, ()>::new(&id).photo())?;
        }
        dto::EntityType::Office => {
            repo.working()?
                .delete(Key::<OfficePath, ()>::new(&id).photo())?;
        }
    }

    let partial = ViewPhotoPartial::new(&conn, typ, id)?;
    let mut response = partial.into_response();
    response
        .headers_mut()
        .insert("HX-Trigger", "entity_updated".parse().unwrap());
    Ok(response)
}
