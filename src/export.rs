use anyhow::{Context, Result};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write,
    path::Path,
};

use crate::{
    data::{self, ContactType, Office, Person, SupervisingRelation, Tenure},
    record::{Key, OfficePath, PersonPath, RecordKey, RecordRepo, RecordValue},
};

pub fn run(db: &Path, output: &Path) -> Result<()> {
    // Create output directories
    let person_dir = output.join("person");
    fs::create_dir_all(&person_dir)
        .with_context(|| format!("could not create person directory at {:?}", person_dir))?;

    let office_dir = output.join("office");
    fs::create_dir_all(&office_dir)
        .with_context(|| format!("could not create office directory at {:?}", office_dir))?;

    // Open repository
    let conn = rusqlite::Connection::open(db)
        .with_context(|| format!("could not open database at {:?}", db))?;
    let repo = RecordRepo::new(&conn);
    let repo_ref = repo.committed()?;

    // Export persons
    struct PersonBuilder {
        name: Option<String>,
        photo: Option<data::Photo>,
        tenures: Vec<Tenure>,
        contacts: BTreeMap<ContactType, String>,
    }

    let flush_person = |id: &str, builder: PersonBuilder, dir: &Path| -> Result<()> {
        if let Some(name) = builder.name {
            let person_data = Person {
                name,
                photo: builder.photo,
                contacts: if builder.contacts.is_empty() {
                    None
                } else {
                    Some(builder.contacts)
                },
                tenures: if builder.tenures.is_empty() {
                    None
                } else {
                    Some(builder.tenures)
                },
            };
            let toml_string = toml::to_string_pretty(&person_data)
                .context("could not serialize person to TOML")?;

            let file_path = dir.join(format!("{}.toml", id));
            let mut file = File::create(&file_path)
                .with_context(|| format!("could not create {:?}", file_path))?;
            file.write_all(toml_string.as_bytes())
                .with_context(|| format!("could not write to {:?}", file_path))?;
        }
        Ok(())
    };

    let mut current_id: Option<String> = None;
    let mut current_person: Option<PersonBuilder> = None;

    for item in repo_ref.scan(Key::<PersonPath, ()>::all())? {
        let (key, value) = item?;

        let id = match &key {
            RecordKey::Name(k) => &k.entity_id,
            RecordKey::Photo(k) => &k.entity_id,
            RecordKey::Contact(k) => &k.entity_id,
            RecordKey::Tenure(k) => &k.entity_id,
            _ => continue,
        };

        if current_id.as_deref() != Some(id) {
            if let (Some(cid), Some(builder)) = (current_id.take(), current_person.take()) {
                flush_person(&cid, builder, &person_dir)?;
            }
            current_id = Some(id.clone());
            current_person = Some(PersonBuilder {
                name: None,
                photo: None,
                tenures: Vec::new(),
                contacts: BTreeMap::new(),
            });
        }

        let builder = current_person.as_mut().unwrap();

        match (key, value) {
            (RecordKey::Name(_), RecordValue::Name(v)) => builder.name = Some(v),
            (RecordKey::Photo(_), RecordValue::Photo(v)) => builder.photo = Some(v),
            (RecordKey::Contact(k), RecordValue::Contact(v)) => {
                builder.contacts.insert(k.state.typ, v);
            }
            (RecordKey::Tenure(k), RecordValue::Tenure(v)) => {
                builder.tenures.push(Tenure {
                    office_id: k.state.office_id,
                    start: k.state.start.map(|d| d.to_string()),
                    end: v.map(|d| d.to_string()),
                });
            }
            _ => {}
        }
    }

    if let (Some(cid), Some(builder)) = (current_id, current_person) {
        flush_person(&cid, builder, &person_dir)?;
    }

    // Export offices
    struct OfficeBuilder {
        name: Option<String>,
        photo: Option<data::Photo>,
        supervisors: BTreeMap<SupervisingRelation, String>,
        contacts: BTreeMap<ContactType, String>,
    }

    let flush_office = |id: &str, builder: OfficeBuilder, dir: &Path| -> Result<()> {
        if let Some(name) = builder.name {
            let office_data = Office {
                name,
                photo: builder.photo,
                contacts: if builder.contacts.is_empty() {
                    None
                } else {
                    Some(builder.contacts)
                },
                supervisors: if builder.supervisors.is_empty() {
                    None
                } else {
                    Some(builder.supervisors)
                },
            };

            let toml_string = toml::to_string_pretty(&office_data)
                .context("could not serialize office to TOML")?;

            let file_path = dir.join(format!("{}.toml", id));
            let mut file = File::create(&file_path)
                .with_context(|| format!("could not create {:?}", file_path))?;
            file.write_all(toml_string.as_bytes())
                .with_context(|| format!("could not write to {:?}", file_path))?;
        }
        Ok(())
    };

    let mut current_id: Option<String> = None;
    let mut current_office: Option<OfficeBuilder> = None;

    for item in repo_ref.scan(Key::<OfficePath, ()>::all())? {
        let (key, value) = item?;

        let id = match &key {
            RecordKey::Name(k) => &k.entity_id,
            RecordKey::Photo(k) => &k.entity_id,
            RecordKey::Contact(k) => &k.entity_id,
            RecordKey::Supervisor(k) => &k.entity_id,
            _ => continue,
        };

        if current_id.as_deref() != Some(id) {
            if let (Some(cid), Some(builder)) = (current_id.take(), current_office.take()) {
                flush_office(&cid, builder, &office_dir)?;
            }
            current_id = Some(id.clone());
            current_office = Some(OfficeBuilder {
                name: None,
                photo: None,
                supervisors: BTreeMap::new(),
                contacts: BTreeMap::new(),
            });
        }

        let builder = current_office.as_mut().unwrap();

        match (key, value) {
            (RecordKey::Name(_), RecordValue::Name(v)) => builder.name = Some(v),
            (RecordKey::Photo(_), RecordValue::Photo(v)) => builder.photo = Some(v),
            (RecordKey::Contact(k), RecordValue::Contact(v)) => {
                builder.contacts.insert(k.state.typ, v);
            }
            (RecordKey::Supervisor(k), RecordValue::Supervisor(v)) => {
                builder.supervisors.insert(k.state.relation, v);
            }
            _ => {}
        }
    }

    if let (Some(cid), Some(builder)) = (current_id, current_office) {
        flush_office(&cid, builder, &office_dir)?;
    }

    println!(
        "Successfully exported data to `{}`",
        output.to_string_lossy()
    );

    Ok(())
}
