use crate::LibrarySql;
use crate::serve::handler::filters;
use crate::{
    context, data, dto,
    serve::{AppError, AppState},
};
use anyhow::Context;
use askama::Template;
use askama_web::WebTemplate;
use axum::extract::State;
use rusqlite::OptionalExtension;
use std::{collections::BTreeMap, sync::Arc};

pub mod supervisor;

#[derive(Template, WebTemplate)]
#[template(path = "office.html")]
pub struct OfficePageTemplate {
    pub office: context::Office,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
    pub incumbent: Option<context::Person>,
    pub quondams: Option<Vec<context::Quondam>>,
    pub supervisors: Option<BTreeMap<data::SupervisingRelation, context::Office>>,

    pub sources: Option<Vec<String>>,
    pub config: Arc<context::Config>,
    pub page: context::Page,
    pub metadata: context::Metadata,
}

#[axum::debug_handler]
pub async fn page(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id_with_ext): axum::extract::Path<String>,
) -> Result<OfficePageTemplate, AppError> {
    let id = id_with_ext.trim_end_matches(".html");
    let conn = state.get_conn()?;

    let name = conn
        .get_entity_name(&dto::EntityType::Office, id, |row| row.get(0))
        .with_context(|| format!("could not get name for office: {}", id))?;

    let photo = conn
        .get_entity_photo(&dto::EntityType::Office, id, |row| {
            Ok(data::Photo {
                url: row.get(0)?,
                attribution: row.get(1)?,
            })
        })
        .optional()
        .with_context(|| format!("could not get photo for office: {}", id))?;

    let mut contacts: BTreeMap<data::ContactType, String> = BTreeMap::new();
    conn.get_entity_contacts(&dto::EntityType::Office, id, |row| {
        contacts.insert(row.get(0)?, row.get(1)?);

        Ok(())
    })?;

    let mut supervisors: BTreeMap<data::SupervisingRelation, context::Office> = BTreeMap::new();
    conn.get_office_supervising_offices(id, |row| {
        let relation = row.get(0)?;
        let supervising_office_id: String = row.get(1)?;
        let name =
            conn.get_entity_name(&dto::EntityType::Office, &supervising_office_id, |row| {
                row.get(0)
            })?;
        supervisors.insert(
            relation,
            context::Office {
                id: supervising_office_id,
                name,
            },
        );

        Ok(())
    })
    .with_context(|| format!("could not get supervising offices for office: {}", id))?;

    let incumbent = conn
        .get_office_incumbent(id, |row| {
            Ok(context::Person {
                id: row.get(0)?,
                name: row.get(1)?,
            })
        })
        .optional()?;

    let mut quondams = Vec::new();
    conn.get_office_quondams(id, |row| {
        quondams.push(context::Quondam {
            person: context::Person {
                id: row.get(0)?,
                name: row.get(1)?,
            },
            start: row.get(2)?,
            end: row.get(3)?,
        });

        Ok(())
    })?;

    let commit_date = conn
        .get_entity_commit_date(&dto::EntityType::Office, id, |row| {
            row.get::<_, chrono::NaiveDate>(0)
        })
        .optional()?
        .map(|d| d.to_string());

    // page
    let page = context::Page {
        base: "../".to_string(),
        dynamic: state.dynamic,
    };

    // metadata
    let metadata = context::Metadata {
        maintenance: context::Maintenance { incomplete: false },
        commit_date,
    };

    Ok(OfficePageTemplate {
        office: context::Office {
            id: id.to_string(),
            name,
        },
        photo,
        contacts: Some(contacts).filter(|v| !v.is_empty()),
        supervisors: Some(supervisors).filter(|v| !v.is_empty()),
        incumbent,
        quondams: Some(quondams).filter(|v| !v.is_empty()),
        sources: None,
        config: state.config.clone(),
        page,
        metadata,
    })
}
