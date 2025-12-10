pub mod handler;

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Router,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header::InvalidHeaderValue},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;

use r2d2::Error as R2D2Error;
use tower_http::services::ServeDir;

use crate::{
    context, from_toml_file,
};
use tower_livereload::LiveReloadLayer;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Unexpected: {0}")]
    Unexpected(String),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error(transparent)]
    Askama(#[from] askama::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    R2D2(#[from] R2D2Error),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Internal(#[from] InvalidHeaderValue),
}

impl From<String> for AppError {
    fn from(err: String) -> Self {
        AppError::Unexpected(err)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        #[cfg(debug_assertions)]
        let message = format!("Error: {:?}", self);

        #[cfg(not(debug_assertions))]
        let message = "Internal Server Error".to_string();

        (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
    }
}

#[tokio::main]
pub async fn run(
    db: PathBuf,
    templates: PathBuf,
    static_files: PathBuf,
    port: Option<&str>,
) -> Result<()> {
    let state = AppState::new(db, templates, true)?;

    let app = Router::new()
        .route("/", get(handler::index))
        .route("/person/{id}", get(handler::person::page))
        .route("/office/{id}", get(handler::office::page))
        .route("/search.db", get(handler::search_db))
        .route("/uncommitted", get(handler::uncommitted))
        .route("/new/{typ}", get(handler::entity::new_form))
        .route("/new/{typ}", post(handler::entity::new))
        .route("/{typ}/{id}/edit", get(handler::entity::edit))
        .route("/{typ}/{id}/name/edit", get(handler::entity::name::edit))
        .route("/{typ}/{id}/name", get(handler::entity::name::view))
        .route("/{typ}/{id}/name", put(handler::entity::name::save))
        .route("/{typ}/{id}/photo/edit", get(handler::entity::photo::edit))
        .route("/{typ}/{id}/photo", get(handler::entity::photo::view))
        .route("/{typ}/{id}/photo", put(handler::entity::photo::save))
        .layer(LiveReloadLayer::new())
        .with_state(Arc::new(state))
        .nest_service("/static", ServeDir::new(static_files));

    let addr = format!("0.0.0.0:{}", port.unwrap_or("8080"));
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("could not listen"))?;

    println!("Serving at http://{}/", addr);
    axum::serve(listener, app)
        .await
        .with_context(|| format!("could not start server"))?;

    Ok(())
}

pub struct AppState {
    pub dynamic: bool,
    pub db: PathBuf,
    pub db_pool: Pool<SqliteConnectionManager>,
    pub config: Arc<context::Config>,
}

impl AppState {
    pub fn new(db: PathBuf, templates: PathBuf, dynamic: bool) -> Result<Self> {
        let config: context::Config = from_toml_file(templates.join("config.toml"))
            .with_context(|| format!("could not parse config"))?;

        let manager = SqliteConnectionManager::file(&db);
        let db_pool = r2d2::Pool::builder()
            .max_size(15) // Max connections to keep open
            .build(manager)?;

        Ok(AppState {
            dynamic,
            db,
            db_pool,
            config: Arc::new(config),
        })
    }

    pub fn get_conn(&self) -> Result<PooledConnection<SqliteConnectionManager>, R2D2Error> {
        self.db_pool.get()
    }
}

pub fn hx_redirect(url: &str) -> Result<Response, AppError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("hx-redirect"),
        HeaderValue::from_str(url)?,
    );
    
    Ok((StatusCode::OK, headers).into_response())
}