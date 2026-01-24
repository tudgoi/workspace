use crate::config::Config;
use crate::record::RecordRepo;
use crate::serve::handler::filters;
use crate::{CONFIG, LibrarySql};
use crate::{
    context, data, dto,
    serve::{AppError, AppState},
};
use askama::Template;
use askama_web::WebTemplate;
use axum::extract::State;
use rusqlite::OptionalExtension;
use std::{collections::BTreeMap, sync::Arc};

pub mod tenure;

#[derive(Template, WebTemplate)]
#[template(path = "person.html")]
pub struct PersonPageTemplate {
    pub person: context::Person,
    pub photo: Option<data::Photo>,
    pub contacts: Option<BTreeMap<data::ContactType, String>>,
    pub offices: Option<Vec<context::OfficeDetails>>,
    pub past_tenures: Option<Vec<context::TenureDetails>>,

    pub sources: Option<Vec<String>>,
    pub config: &'static Config,
    pub page: context::Page,
    pub metadata: context::Metadata,
}

#[axum::debug_handler]
pub async fn page(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id_with_ext): axum::extract::Path<String>,
) -> Result<PersonPageTemplate, AppError> {
    let id = id_with_ext.trim_end_matches(".html");
    let conn = state.get_conn()?;
    let repo = RecordRepo::new(&conn);

    let name = conn.get_entity_name(&dto::EntityType::Person, id, |row| row.get(0))?;
    let photo = conn
        .get_entity_photo(&dto::EntityType::Person, id, |row| {
            Ok(data::Photo {
                url: row.get(0)?,
                attribution: row.get(1)?,
            })
        })
        .optional()?;

    let mut contacts: BTreeMap<data::ContactType, String> = BTreeMap::new();
    conn.get_entity_contacts(&dto::EntityType::Person, id, |row| {
        contacts.insert(row.get(0)?, row.get(1)?);

        Ok(())
    })?;
    let mut offices_for_person = Vec::new();
    conn.get_person_incumbent_office_details(id, |row| {
        let mut contacts: BTreeMap<data::ContactType, String> = BTreeMap::new();
        conn.get_entity_contacts(&dto::EntityType::Office, &row.get::<_, String>(0)?, |row| {
            contacts.insert(row.get(0)?, row.get(1)?);

            Ok(())
        })?;
        offices_for_person.push(dto::Office {
            id: row.get(0)?,
            name: row.get(1)?,
            photo: if let Some(url) = row.get(2)? {
                Some(data::Photo {
                    url,
                    attribution: row.get(3)?,
                })
            } else {
                None
            },
            contacts: if contacts.is_empty() {
                None
            } else {
                Some(contacts)
            },
            start: row.get(4)?,
        });

        Ok(())
    })?;
    // office, official_contacts, supervisors, subordinates
    let mut offices = Vec::new();
    for office_dto in offices_for_person {
        // supervisors
        let mut supervisors: BTreeMap<data::SupervisingRelation, context::Officer> =
            BTreeMap::new();
        conn.get_office_supervisors(&office_dto.id, |row| {
            let person = if let (Some(id), Some(name)) = (row.get(3)?, row.get(4)?) {
                Some(context::Person { id, name })
            } else {
                None
            };
            supervisors.insert(
                row.get(0)?,
                context::Officer {
                    office_id: row.get(1)?,
                    office_name: row.get(2)?,
                    person,
                },
            );

            Ok(())
        })?;

        // subordinates
        let mut subordinates: BTreeMap<data::SupervisingRelation, Vec<context::Officer>> =
            BTreeMap::new();
        conn.get_office_subordinates(&office_dto.id, |row| {
            let relation: data::SupervisingRelation = row.get(0)?;
            let officer = context::Officer {
                office_id: row.get(1)?,
                office_name: row.get(2)?,
                person: if let (Some(id), Some(name)) = (row.get(3)?, row.get(4)?) {
                    Some(context::Person { id, name })
                } else {
                    None
                },
            };
            subordinates.entry(relation).or_default().push(officer);

            Ok(())
        })?;

        offices.push(context::OfficeDetails {
            office: context::Office {
                id: office_dto.id,
                name: office_dto.name,
            },
            photo: office_dto.photo,
            contacts: office_dto.contacts,
            supervisors: if supervisors.is_empty() {
                None
            } else {
                Some(supervisors)
            },
            subordinates: if subordinates.is_empty() {
                None
            } else {
                Some(subordinates)
            },
            start: office_dto.start,
        });
    }

    let mut past_tenures = Vec::new();
    conn.get_past_tenures(id, |row| {
        past_tenures.push(context::TenureDetails {
            office: context::Office {
                id: row.get(0)?,
                name: row.get(1)?,
            },
            start: row.get(2)?,
            end: row.get(3)?,
        });

        Ok(())
    })?;
    let commit_id = repo.working()?.commit_id()?;
    Ok(PersonPageTemplate {
        person: context::Person {
            id: id.to_string(),
            name,
        },
        photo,
        contacts: if contacts.is_empty() {
            None
        } else {
            Some(contacts)
        },
        offices: if offices.is_empty() {
            None
        } else {
            Some(offices)
        },
        past_tenures: if past_tenures.is_empty() {
            None
        } else {
            Some(past_tenures)
        },
        sources: None, // Initialize sources as None
        config: &CONFIG,
        page: state.page_context(),
        metadata: context::Metadata {
            commit_id,
            maintenance: context::Maintenance { incomplete: false },
        },
    })
}
