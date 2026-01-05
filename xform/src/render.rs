use anyhow::{Context, Result};
use askama::Template;
use axum::extract::{self, State};
use rusqlite::Connection;
use std::path::Path;
use std::{fs, sync::Arc};

use crate::{LibrarySql, SchemaSql};
use crate::{
    dto,
    serve::{self, AppState},
};

#[tokio::main]
pub async fn run(db: &Path, output: &Path) -> Result<()> {
    let state = Arc::new(AppState::new(
        db.to_path_buf(),
        false,
    )?);
    let conn = state.db_pool.get()?;

    fs::create_dir(output).with_context(|| format!("could not create output dir {:?}", output))?;

    // persons
    render_persons(&conn, State(state.clone()), output)
        .await
        .context("could not render persons")?;

    // offices
    render_offices(&conn, State(state.clone()), output)
        .await
        .context("could not render offices")?;

    // render index
    let template = serve::handler::index(State(state.clone())).await?;
    let str = template.render()?;
    let output_path = output.join("index.html");
    fs::write(output_path.as_path(), str)
        .with_context(|| format!("could not write rendered file {:?}", output_path))?;

    let search_db_path = output.join("search.db");
    create_search_database(&search_db_path, db)?;

    Ok(())
}

async fn render_persons(
    conn: &Connection,
    state: State<Arc<AppState>>,
    output: &Path,
) -> Result<()> {
    let person_path = output.join("person");
    fs::create_dir(person_path.as_path())
        .with_context(|| format!("could not create person dir {:?}", person_path))?;

    let mut ids: Vec<String> = Vec::new();
    conn.get_entity_ids(&dto::EntityType::Person, |row| {
        ids.push(row.get(0)?);
        Ok(())
    })?;

    for id in ids {
        let template =
            serve::handler::person::page(state.clone(), extract::Path(format!("{}.html", id)))
                .await?;
        let str = template.render()?;
        let output_path = person_path.join(format!("{}.html", id));
        fs::write(output_path.as_path(), str)
            .with_context(|| format!("could not write rendered file {:?}", output_path))?;
    }

    Ok(())
}

async fn render_offices(
    conn: &Connection,
    state: State<Arc<AppState>>,
    output: &Path,
) -> Result<()> {
    let office_path = output.join("office");
    fs::create_dir(office_path.as_path())
        .with_context(|| format!("could not create office dir {:?}", office_path))?;

    let mut ids: Vec<String> = Vec::new();
    conn.get_entity_ids(&dto::EntityType::Office, |row| {
        ids.push(row.get(0)?);
        Ok(())
    })?;

    for id in ids {
        let template =
            serve::handler::office::page(state.clone(), extract::Path(format!("{}.html", id)))
                .await
                .with_context(|| format!("could not render office for {}", id))?;
        let str = template.render()?;
        let output_path = office_path.join(format!("{}.html", id));
        fs::write(output_path.as_path(), str)
            .with_context(|| format!("could not write rendered file {:?}", output_path))?;
    }

    Ok(())
}

pub fn create_search_database(search_db_path: &Path, db_path: &Path) -> Result<()> {
    let conn = Connection::open(search_db_path).context("could not create search database")?;
    conn.create_entity_tables()?;
    let db_path_str = db_path
        .to_str()
        .with_context(|| format!("could not convert path {:?}", db_path))?;
    conn.attach_db(db_path_str)?;
    conn.copy_entity_from_db()?;
    conn.detach_db()?;

    // The error from `close` is `(Connection, Error)`, so we map it to just the error.
    conn.close()
        .map_err(|(_, err)| err)
        .context("could not close search database")?;

    Ok(())
}
