-- person
CREATE TABLE person (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    photo_url TEXT,
    photo_attribution TEXT,
    updated TEXT
);
CREATE VIRTUAL TABLE person_idx USING fts5(id, name, content = 'person');
-- Triggers to keep the FTS index up to date.
CREATE TRIGGER person_ai AFTER INSERT ON person BEGIN
  INSERT INTO person_idx(rowid, id, name) VALUES (new.rowid, new.id, new.name);
END;
CREATE TRIGGER person_ad AFTER DELETE ON person BEGIN
  INSERT INTO person_idx(fts_idx, rowid, id, name) VALUES('delete', old.rowid, old.id, old.name);
END;
CREATE TRIGGER person_au AFTER UPDATE ON person BEGIN
  INSERT INTO person_idx(fts_idx, rowid, id, name) VALUES('delete', old.rowid, old.id, old.name);
    INSERT INTO person_idx(rowid, id, name) VALUES (new.rowid, new.id, new.name);
END;
CREATE TABLE person_contact (
    id TEXT NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (id, type)
);
-- office
CREATE TABLE office (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    photo_url TEXT,
    photo_attribution TEXT
);
CREATE VIRTUAL TABLE office_idx USING fts5(id, name, content = 'office');
-- Triggers to keep the FTS index up to date.
CREATE TRIGGER office_ai AFTER INSERT ON office BEGIN
  INSERT INTO office_idx(rowid, id, name) VALUES (new.rowid, new.id, new.name);
END;
CREATE TRIGGER office_ad AFTER DELETE ON office BEGIN
  INSERT INTO office_idx(fts_idx, rowid, id, name) VALUES('delete', old.rowid, old.id, old.name);
END;
CREATE TRIGGER office_au AFTER UPDATE ON office BEGIN
  INSERT INTO office_idx(fts_idx, rowid, id, name) VALUES('delete', old.rowid, old.id, old.name);
    INSERT INTO office_idx(rowid, id, name) VALUES (new.rowid, new.id, new.name);
END;
CREATE TABLE office_contact (
    id TEXT NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (id, type)
);
CREATE TABLE supervisor (
    office_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    supervisor_office_id TEXT NOT NULL
);
CREATE TABLE tenure (
    person_id TEXT NOT NULL,
    office_id TEXT NOT NULL,
    start TEXT,
end TEXT
);
CREATE VIEW incumbent (office_id, person_id) AS
SELECT office_id,
    person_id
FROM tenure
WHERE
end IS NULL;