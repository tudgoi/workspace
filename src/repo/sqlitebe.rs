use rusqlite::{Connection, OptionalExtension};

use crate::repo::{Backend, Hash, RepoError};

pub struct SqliteBackend<'a> {
    conn: &'a Connection,
}

impl<'a> SqliteBackend<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }
}

impl<'a> Backend for SqliteBackend<'a> {
    fn read(&self, hash: &Hash) -> Result<Vec<u8>, RepoError> {
        self.conn
            .query_row("SELECT blob FROM repo WHERE hash = ?1", [hash.0], |row| {
                row.get(0)
            })
            .map_err(RepoError::from)
    }

    fn write(&self, hash: &Hash, blob: &[u8]) -> Result<(), RepoError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO repo (hash, blob) VALUES (?1, ?2)",
            (hash.0, blob),
        )?;
        Ok(())
    }

    fn get_ref(&self, name: &str) -> Result<Option<Hash>, RepoError> {
        self.conn
            .query_row("SELECT hash FROM refs WHERE name = ?1", [name], |row| {
                row.get(0)
            })
            .optional()
            .map_err(RepoError::from)
            .map(|opt| opt.map(|v| Hash(v)))
    }

    fn set_ref(&self, name: &str, hash: &Hash) -> Result<(), RepoError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO refs (name, hash) VALUES (?1, ?2)",
            (name, hash.0),
        )?;
        Ok(())
    }

    fn stats(&self) -> Result<(usize, std::collections::BTreeMap<usize, usize>), RepoError> {
        let mut stmt = self.conn.prepare("SELECT length(blob) as size FROM repo")?;
        let rows = stmt.query_map([], |row| row.get::<_, usize>(0))?;

        let mut count = 0;
        let mut distribution = std::collections::BTreeMap::new();

        for size in rows {
            let size = size?;
            count += 1;
            *distribution.entry(size).or_insert(0) += 1;
        }

        Ok((count, distribution))
    }
}
