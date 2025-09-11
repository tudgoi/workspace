use std::path::Path;

use anyhow::{Context, Result};
use axum::{Router, routing::get};

#[tokio::main]
pub async fn run(db: &Path, port: Option<&str>) -> Result<()> {
    let app = Router::new().route("/", get(root));
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

async fn root() -> &'static str {
    "Hello, World!"
}
