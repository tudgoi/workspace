use std::sync::Arc;

use askama::Template;
use askama_web::WebTemplate;
use axum::extract::State;

use crate::{context::{self, Page}, serve::{AppError, AppState}};
use crate::LibrarySql;

pub mod entity;

#[derive(Template, WebTemplate)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub persons: u32,
    pub offices: u32,
    pub config: Arc<context::Config>,
    pub page: context::Page,   
}

pub async fn index(State(state): State<Arc<AppState>>) -> Result<IndexTemplate, AppError> {
    let conn = state.get_conn()?;

    let (persons, offices) = conn.get_entity_counts(|row| {
        let persons: u32 = row.get(0)?;
        let offices: u32 = row.get(1)?;
        
        Ok((persons, offices))
    })?;

    Ok(IndexTemplate {
        persons,
        offices,
        config: state.config.clone(),
        page: Page {
            dynamic: state.dynamic,
            base: String::from("./"),
        },
    })
}