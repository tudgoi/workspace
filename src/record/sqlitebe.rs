use rusqlite::{Connection, OptionalExtension};

use crate::repo::{Backend, Hash, RepoError};

pub struct SqliteBackend<'a> {
    pub conn: &'a Connection,
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

    fn list_refs(&self) -> Result<Vec<(String, Hash)>, RepoError> {
        let mut stmt = self.conn.prepare("SELECT name, hash FROM refs")?;
        let rows = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let hash_bytes: [u8; 32] = row.get(1)?;
            Ok((name, Hash(hash_bytes)))
        })?;

        let mut refs = Vec::new();
        for r in rows {
            refs.push(r?);
        }
        Ok(refs)
    }

    fn delete_nodes(&self, hashes: &[Hash]) -> Result<usize, RepoError> {
        if hashes.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.unchecked_transaction()?;
        let mut deleted = 0;
        {
            let mut stmt = tx.prepare("DELETE FROM repo WHERE hash = ?1")?;
            for hash in hashes {
                deleted += stmt.execute([hash.0])?;
            }
        }
        tx.commit()?;
        Ok(deleted)
    }

    fn list_all_node_hashes(&self) -> Result<Vec<Hash>, RepoError> {
        let mut stmt = self.conn.prepare("SELECT hash FROM repo")?;
        let rows = stmt.query_map([], |row| {
            let hash_bytes: [u8; 32] = row.get(0)?;
            Ok(Hash(hash_bytes))
        })?;

        let mut hashes = Vec::new();
        for h in rows {
            hashes.push(h?);
        }
        Ok(hashes)
    }

    fn vacuum(&self) -> Result<(), RepoError> {
        self.conn.execute("VACUUM", [])?;
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
