pub mod handler;

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header::InvalidHeaderValue},
    response::{Html, IntoResponse, Response},
    routing::{get, post, put},
};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;

use r2d2::Error as R2D2Error;
use tower_http::services::ServeDir;

use crate::{
    OutputFormat, context, from_toml_file,
    render::{self, ContextFetcher, Renderer},
};
use tower_livereload::LiveReloadLayer;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
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
        .route("/person/{id}", get(person_page))
        .route("/office/{id}", get(office_page))
        .route("/search.db", get(search_db))
        .route("/changes", get(changes))
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
    pub templates: PathBuf,
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
            templates,
            config: Arc::new(config),
        })
    }

    pub fn get_conn(&self) -> Result<PooledConnection<SqliteConnectionManager>, R2D2Error> {
        self.db_pool.get()
    }
}

#[axum::debug_handler]
async fn person_page(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id_with_ext): axum::extract::Path<String>,
) -> Result<Html<String>, AppError> {
    println!("Request called for {}", id_with_ext);
    let id = id_with_ext.trim_end_matches(".html");
    let mut pooled_conn = state.get_conn()?;
    let context_fetcher = ContextFetcher::new(&mut pooled_conn, state.config.as_ref().clone())
        .with_context(|| format!("could not create context fetcher"))?;
    let renderer = Renderer::new(&state.templates, OutputFormat::Html)?;

    let person_context = context_fetcher.fetch_person(true, id)?;

    let body = renderer
        .render_person(&person_context)
        .with_context(|| "could not render person page")?;

    Ok(Html(body))
}

#[axum::debug_handler]
async fn office_page(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id_with_ext): axum::extract::Path<String>,
) -> Result<Html<String>, AppError> {
    println!("Request called for {}", id_with_ext);
    let id = id_with_ext.trim_end_matches(".html");
    let mut pooled_conn = state.get_conn()?;

    let context_fetcher = ContextFetcher::new(&mut pooled_conn, state.config.as_ref().clone())
        .with_context(|| format!("could not create context fetcher"))?;
    let renderer = Renderer::new(&state.templates, OutputFormat::Html)?;

    let office_context = context_fetcher.fetch_office(true, id)?;

    let body = renderer
        .render_office(&office_context)
        .with_context(|| "could not render office page")?;

    Ok(Html(body))
}

#[axum::debug_handler]
async fn search_db(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match render::create_search_database_in_memory(&state.db) {
        Ok(db_bytes) => (StatusCode::OK, db_bytes),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not build search.db: {}", error.to_string()).into(),
        ),
    }
}

#[axum::debug_handler]
async fn changes(State(state): State<Arc<AppState>>) -> Result<Html<String>, AppError> {
    let mut pooled_conn = state.get_conn()?;
    let context_fetcher = ContextFetcher::new(&mut pooled_conn, state.config.as_ref().clone())
        .with_context(|| format!("could not create context fetcher"))?;
    let renderer = Renderer::new(&state.templates, OutputFormat::Html)?;

    let context = context_fetcher
        .fetch_changes()
        .with_context(|| "could not fetch changes context")?;
    let body = renderer
        .render_changes(&context)
        .with_context(|| "could not render index")?;

    Ok(Html(body))
}


pub fn hx_redirect(url: &str) -> Result<Response, AppError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("hx-redirect"),
        HeaderValue::from_str(url)?,
    );
    
    Ok((StatusCode::OK, headers).into_response())
}