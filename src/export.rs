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
    let commit_id = repo_ref.commit_id()?;
    let commit_id_path = output.join("commit_id.txt");
    let mut commit_id_file = File::create(&commit_id_path)
        .with_context(|| format!("could not create {:?}", commit_id_path))?;
    commit_id_file
        .write_all(commit_id.to_hex().as_bytes())
        .with_context(|| format!("could not write to {:?}", commit_id_path))?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::sqlitebe::SqliteBackend;
    use crate::repo::Repo;
    use rusqlite::Connection;

    fn setup_db(conn: &Connection) {
        conn.execute_batch(
            r#"
            CREATE TABLE repo (
              hash BLOB NOT NULL PRIMARY KEY,
              blob BLOB NOT NULL
            );
            CREATE TABLE refs (
              name TEXT NOT NULL PRIMARY KEY,
              hash BLOB NOT NULL
            );
            CREATE TABLE secrets (
              name TEXT NOT NULL PRIMARY KEY,
              value BLOB NOT NULL
            );
            CREATE TABLE entity (
              type TEXT NOT NULL,
              id TEXT NOT NULL,
              name TEXT NOT NULL,
              PRIMARY KEY(type, id)
            );
            CREATE TABLE entity_photo (
              entity_type TEXT NOT NULL,
              entity_id TEXT NOT NULL,
              url TEXT NOT NULL,
              attribution TEXT,
              PRIMARY KEY(entity_type, entity_id)
            );
            CREATE TABLE person_office_tenure (
              person_id TEXT NOT NULL,
              office_id TEXT NOT NULL,
              start TEXT,
              end TEXT
            );
        "#,
        )
        .unwrap();
    }

    #[test]
    fn test_export_commit_id() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");
        let output_dir = temp_dir.path().join("output");

        {
            let conn = Connection::open(&db_path)?;
            setup_db(&conn);
            let backend = SqliteBackend::new(&conn);
            let mut repo = Repo::new(backend);
            repo.init()?;
            repo.commit()?;
        }

        run(&db_path, &output_dir)?;

        let commit_id_path = output_dir.join("commit_id.txt");
        assert!(commit_id_path.exists());

        let content = fs::read_to_string(commit_id_path)?;
        assert_eq!(content.len(), 64);

        // Verify it matches the repo commit id
        let conn = Connection::open(&db_path)?;
        let repo = RecordRepo::new(&conn);
        let committed_ref = repo.committed()?;
        let expected_hash = committed_ref.commit_id()?.to_hex();

        assert_eq!(content, expected_hash);

        Ok(())
    }
}
