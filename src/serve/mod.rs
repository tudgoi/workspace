pub mod handler;

use std::{path::PathBuf, sync::Arc};
use rust_embed::Embed;
use axum_embed::ServeEmbed;
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
use thiserror::Error;

use crate::{record::{RecordRepoError, sqlitebe::SqlitePoolBackend}, repo::sync::server::RepoServer};

use tower_livereload::LiveReloadLayer;

#[derive(Embed, Clone)]
#[folder = "static/"]
pub struct StaticDir;

#[derive(Debug, Error)]
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
    RecordRepo(#[from] RecordRepoError),
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

pub async fn run(
    db: PathBuf,
    port: Option<&str>,
) -> Result<()> {
    let state = AppState::new(db.clone(), true)?;

    let backend = SqlitePoolBackend::new(state.db_pool.clone());
    let repo_server = RepoServer::new(backend);
    let (endpoint_id, _repo_router) = repo_server.start().await.context("failed to start repo server")?;

    let app = Router::new()
        .route("/", get(handler::index))
        .route("/person/{id}", get(handler::person::page))
        .route("/office/{id}", get(handler::office::page))
        .route("/search.db", get(handler::search_db))
        .route("/uncommitted", get(handler::uncommitted))
        .route("/commit", post(handler::commit))
        .route("/new/{typ}", get(handler::entity::new_form))
        .route("/new/{typ}", post(handler::entity::new))
        .route("/{typ}/{id}/edit", get(handler::entity::edit))
        .route("/{typ}/{id}/name/edit", get(handler::entity::name::edit))
        .route("/{typ}/{id}/name", get(handler::entity::name::view))
        .route("/{typ}/{id}/name", put(handler::entity::name::save))
        .route("/{typ}/{id}/photo/edit", get(handler::entity::photo::edit))
        .route("/{typ}/{id}/photo", get(handler::entity::photo::view))
        .route("/{typ}/{id}/photo", put(handler::entity::photo::save))
        .route(
            "/{typ}/{id}/contact/add",
            get(handler::entity::contact::add),
        )
        .route("/{typ}/{id}/contact", get(handler::entity::contact::view))
        .route("/{typ}/{id}/contact", post(handler::entity::contact::save))
        .route("/person/{id}/tenure/add", get(handler::person::tenure::add))
        .route("/person/{id}/tenure", get(handler::person::tenure::view))
        .route("/person/{id}/tenure", post(handler::person::tenure::save))
        .route(
            "/office/{id}/supervisor/add",
            get(handler::office::supervisor::add),
        )
        .route(
            "/office/{id}/supervisor",
            get(handler::office::supervisor::view),
        )
        .route(
            "/office/{id}/supervisor",
            post(handler::office::supervisor::save),
        )
        .layer(LiveReloadLayer::new())
        .with_state(Arc::new(state))
        .nest_service("/static", ServeEmbed::<StaticDir>::new());

    let addr = format!("0.0.0.0:{}", port.unwrap_or("8080"));
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context("could not listen")?;

    println!("Iroh ID: {}", endpoint_id);
    println!("Serving at http://{}/", addr);
    axum::serve(listener, app)
        .await
        .context("could not start server")?;

    Ok(())
}

pub struct AppState {
    pub dynamic: bool,
    pub db: PathBuf,
    pub db_pool: Pool<SqliteConnectionManager>,
}

impl AppState {
    pub fn new(db: PathBuf, dynamic: bool) -> Result<Self> {
        let manager = SqliteConnectionManager::file(&db);
        let db_pool = r2d2::Pool::builder()
            .max_size(15) // Max connections to keep open
            .build(manager)?;

        Ok(AppState {
            dynamic,
            db,
            db_pool,
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
