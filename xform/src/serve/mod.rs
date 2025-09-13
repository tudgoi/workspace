use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{State},
    response::{Html, IntoResponse},
    routing::get,
};

use crate::{
    OutputFormat,
    render::{ContextFetcher, Renderer},
};

#[tokio::main]
pub async fn run(db: &Path, templates: &Path, port: Option<&str>) -> Result<()> {
    let state = AppState::new(db, templates)?;

    let app = Router::new()
        .route("/", get(root))
        .route("/person/{id}", get(person_page))
        .route("/changes", get(changes))
        .with_state(Arc::new(state));

    let addr = format!("0.0.0.0:{}", port.unwrap_or("8080"));
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("could not listen"))?;

    println!("Listening at http://{}/", addr);
    axum::serve(listener, app)
        .await
        .with_context(|| format!("could not start server"))?;

    Ok(())
}

struct AppState {
    context_fetcher: Mutex<ContextFetcher>,
    renderer: Renderer,
}

impl AppState {
    pub fn new(db: &Path, templates: &Path) -> Result<Self> {
        let context_fetcher = ContextFetcher::new(db, templates)
            .with_context(|| format!("could not create context fetcher"))?;

        let renderer = Renderer::new(templates, OutputFormat::Html)?;

        Ok(AppState {
            context_fetcher: Mutex::new(context_fetcher),
            renderer,
        })
    }
}

async fn root(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let context = state
        .context_fetcher
        .lock()
        .expect("should be able to acquire lock")
        .fetch_index()
        .expect("could not fetch index context");
    let body = state
        .renderer
        .render_index(&context)
        .expect("could not render index");
    Html(body)
}

async fn person_page(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id_with_ext): axum::extract::Path<String>,
) -> impl IntoResponse {
    println!("Request called for {}", id_with_ext);
    let id = id_with_ext.trim_end_matches(".html");

    let context_fetcher = state
        .context_fetcher
        .lock()
        .expect("should be able to acquire lock");

    let person_context = match context_fetcher.fetch_person(id) {
        Ok(context) => context,
        Err(e) => return Html(format!("Person not found<p>{}", e)), // Or a proper 404 page
    };

    let body = state
        .renderer
        .render_person(&person_context)
        .expect("could not render person page");

    Html(body)
}

async fn changes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let context = state
        .context_fetcher
        .lock()
        .expect("should be able to acquire lock")
        .fetch_changes()
        .expect("could not fetch changes context");
    let body = state
        .renderer
        .render_changes(&context)
        .expect("could not render index");
    Html(body)
}