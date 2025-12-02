use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::{
    Form,
    extract::{Path, State},
};

use crate::LibrarySql;
use crate::{
    data,
    serve::{AppError, AppState},
};

#[derive(Template, WebTemplate)]
#[template(path = "entity/photo/edit_partial.html")]
pub struct EditPhotoPartial {
    typ: String,
    id: String,
    photo: Option<data::Photo>,
}

#[axum::debug_handler]
pub async fn edit(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(String, String)>,
) -> Result<EditPhotoPartial, AppError> {
    let conn = state.get_conn()?;
    let photo = match conn.get_entity_photo(&typ, &id, |row| {
        let url: String = row.get(0)?;
        let attribution: Option<String> = Some(row.get(1)?);

        Ok(data::Photo { url, attribution })
    }) {
        Ok(photo) => Some(photo),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e.into()),
    };
    Ok(EditPhotoPartial { id, typ, photo })
}

#[derive(Template, WebTemplate)]
#[template(path = "entity/edit.html", block = "photo")]
pub struct ViewPhotoPartial {
    typ: String,
    id: String,
    photo: Option<data::Photo>,
}

#[axum::debug_handler]
pub async fn view(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(String, String)>,
) -> Result<ViewPhotoPartial, AppError> {
    let conn = state.get_conn()?;
    let photo = match conn.get_entity_photo(&typ, &id, |row| {
        let url: String = row.get(0)?;
        let attribution: Option<String> = Some(row.get(1)?);

        Ok(data::Photo { url, attribution })
    }) {
        Ok(photo) => Some(photo),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e.into()),
    };
    Ok(ViewPhotoPartial { id, typ, photo })
}

#[axum::debug_handler]
pub async fn save(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(String, String)>,
    Form(photo_form): Form<data::Photo>, // Renamed to avoid conflict with `photo` variable below
) -> Result<ViewPhotoPartial, AppError> {
    let conn = state.get_conn()?;
    conn.save_entity_photo(&typ, &id, &photo_form.url, photo_form.attribution.as_deref())?;

    let photo = match conn.get_entity_photo(&typ, &id, |row| {
        let url: String = row.get(0)?;
        let attribution: Option<String> = Some(row.get(1)?);

        Ok(data::Photo { url, attribution })
    }) {
        Ok(photo) => Some(photo),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e.into()),
    };

    Ok(ViewPhotoPartial { id, typ, photo })
}
