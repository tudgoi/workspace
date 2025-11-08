use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use tower_http::services::ServeDir;

use crate::{
    OutputFormat,
    render::{self, ContextFetcher, Renderer},
};

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[tokio::main]
pub async fn run(
    db: PathBuf,
    templates: PathBuf,
    static_files: PathBuf,
    port: Option<&str>,
) -> Result<()> {
    let state = AppState::new(db, templates)?;

    let app = Router::new()
        .route("/", get(root))
        .route("/person/{id}", get(person_page))
        .route("/person/edit/{id}", get(person_edit))
        .route("/office/{id}", get(office_page))
        .route("/search.db", get(search_db))
        .route("/changes", get(changes))
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

struct AppState {
    db: PathBuf,
    templates: PathBuf,
}

impl AppState {
    pub fn new(db: PathBuf, templates: PathBuf) -> Result<Self> {
        Ok(AppState { db, templates })
    }
}

#[axum::debug_handler]
async fn root(State(state): State<Arc<AppState>>) -> Result<Html<String>, AppError> {
    let context_fetcher = ContextFetcher::new(&state.db, &state.templates)
        .with_context(|| format!("could not create context fetcher"))?;
    let renderer = Renderer::new(&state.templates, OutputFormat::Html)?;

    let context = context_fetcher
        .fetch_index()
        .with_context(|| format!("could not fetch index context"))?;
    let body = renderer
        .render_index(&context)
        .with_context(|| format!("could not render index"))?;

    Ok(Html(body))
}

#[axum::debug_handler]
async fn person_page(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id_with_ext): axum::extract::Path<String>,
) -> Result<Html<String>, AppError> {
    println!("Request called for {}", id_with_ext);
    let id = id_with_ext.trim_end_matches(".html");

    let context_fetcher = ContextFetcher::new(&state.db, &state.templates)
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

    let context_fetcher = ContextFetcher::new(&state.db, &state.templates)
        .with_context(|| format!("could not create context fetcher"))?;
    let renderer = Renderer::new(&state.templates, OutputFormat::Html)?;

    let office_context = context_fetcher.fetch_office(id)?;

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
    let context_fetcher = ContextFetcher::new(&state.db, &state.templates)
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

#[axum::debug_handler]
async fn person_edit(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Html<String>, AppError> {
    let context_fetcher = ContextFetcher::new(&state.db, &state.templates)
        .with_context(|| format!("could not create context fetcher"))?;
    let renderer = Renderer::new(&state.templates, OutputFormat::Html)?;

    let edit_context = context_fetcher.fetch_person_edit(true, &id)?;

    let body = renderer
         .render_person_edit(&edit_context)
         .with_context(|| "could not render person page")?;

    Ok(Html(body))
}