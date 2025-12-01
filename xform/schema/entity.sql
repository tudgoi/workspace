-- [entity]
CREATE TABLE entity (
  type TEXT NOT NULL,
  id TEXT NOT NULL,
  name TEXT NOT NULL,
  PRIMARY KEY(type, id),
  UNIQUE(type, id)
);
-- FTS
CREATE VIRTUAL TABLE entity_idx USING fts5(id, name, content = 'entity');
CREATE TRIGGER entity_ai_fts
AFTER
INSERT ON entity BEGIN
INSERT INTO entity_idx(rowid, id, name)
VALUES (new.rowid, new.id, new.name);
END;
CREATE TRIGGER entity_ad_fts
AFTER DELETE ON entity BEGIN
INSERT INTO entity_idx(entity_idx, rowid, id, name)
VALUES('delete', old.rowid, old.id, old.name);
END;
CREATE TRIGGER entity_au_fts
AFTER
UPDATE ON entity BEGIN
INSERT INTO entity_idx(entity_idx, rowid, id, name)
VALUES('delete', old.rowid, old.id, old.name);
INSERT INTO entity_idx(rowid, id, name)
VALUES (new.rowid, new.id, new.name);
END;
-- TODO Shouldn't this be in property.sql?
-- [entity_photo]
CREATE TABLE entity_photo (
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  url TEXT NOT NULL,
  attribution TEXT,
  thumbnail_url TEXT AS (
    CASE
      WHEN url LIKE 'https://upload.wikimedia.org/wikipedia/commons/%' AND url NOT LIKE '%.svg' THEN
      REPLACE(url, '/commons/', '/commons/thumb/') || '/' || '320px-'
      -- Extract the Filename. 
      -- We skip '/commons/' (9 chars) + the hash folders 'a/bc/' (5 chars)
      -- Total offset = 14 characters from the start of '/commons/'
      || SUBSTR(url, INSTR(url, '/commons/') + 14)
      ELSE url
    END
  ),
  PRIMARY KEY(entity_type, entity_id) FOREIGN KEY(entity_type, entity_id) REFERENCES entity(type, id)
);