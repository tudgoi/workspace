pub mod name;
pub mod photo;
pub mod contact;

use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::{
    Form,
    extract::{Path, State},
    response::Response,
};
use serde::Deserialize;

use crate::{
    LibrarySql, context, dto,
    serve::{AppError, AppState, handler::{entity::{contact::ViewContactPartial, name::ViewNamePartial, photo::ViewPhotoPartial}, office::supervisor::ViewSupervisorPartial, person::tenure::ViewTenurePartial}, hx_redirect},
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
    pub name_partial: ViewNamePartial,
    pub photo_partial: ViewPhotoPartial,
    pub contact_partial: ViewContactPartial,
    pub tenure_partial: ViewTenurePartial,
    pub supervisor_partial: ViewSupervisorPartial,

    pub config: Arc<context::Config>,
    pub page: context::Page,
}

#[axum::debug_handler]
pub async fn edit(
    State(state): State<Arc<AppState>>,
    Path((typ, id)): Path<(dto::EntityType, String)>,
) -> Result<EditTemplate, AppError> {
    let conn = state.get_conn()?;
    let name_partial = ViewNamePartial::new(&conn, typ.clone(), id.clone())?;
    let photo_partial = ViewPhotoPartial::new(&conn, typ.clone(), id.clone())?;
    let contact_partial = ViewContactPartial::new(&conn, typ.clone(), id.clone())?;
    let tenure_partial = ViewTenurePartial::new(&conn, id.clone())?;
    let supervisor_partial = ViewSupervisorPartial::new(&conn, id.clone())?;

    Ok(EditTemplate {
        typ,
        id,
        name_partial,
        photo_partial,
        contact_partial,
        tenure_partial,
        supervisor_partial,
        config: state.config.clone(),
        page: context::Page {
            dynamic: state.dynamic,
            base: String::from("../../"),
        },
    })
}
