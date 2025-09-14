-- [commit_tracking]
CREATE TABLE commit_tracking (
  id INTEGER PRIMARY KEY,
  enabled INTEGER NOT NULL DEFAULT 0
);
-- [person]
CREATE TABLE person (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  photo_url TEXT,
  photo_attribution TEXT,
  commit_date TEXT
);
-- FTS
CREATE VIRTUAL TABLE person_idx USING fts5(id, name, content = 'person');
CREATE TRIGGER person_ai
AFTER
INSERT ON person BEGIN
INSERT INTO person_idx(rowid, id, name)
VALUES (new.rowid, new.id, new.name);
END;
CREATE TRIGGER person_ad
AFTER DELETE ON person BEGIN
INSERT INTO person_idx(person_idx, rowid, id, name)
VALUES('delete', old.rowid, old.id, old.name);
END;
CREATE TRIGGER person_au
AFTER
UPDATE ON person BEGIN
INSERT INTO person_idx(person_idx, rowid, id, name)
VALUES('delete', old.rowid, old.id, old.name);
INSERT INTO person_idx(rowid, id, name)
VALUES (new.rowid, new.id, new.name);
END;
-- [person_contact]
CREATE TABLE person_contact (
  id TEXT NOT NULL,
  type TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (id, type) FOREIGN KEY(id) REFERENCES person(id)
);
--- commit tracking
CREATE TRIGGER person_contact_ai
AFTER
INSERT ON person_contact
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
UPDATE person
SET commit_date = NULL
WHERE id = new.id;
END;
CREATE TRIGGER person_contact_au
AFTER
UPDATE ON person_contact
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
UPDATE person
SET commit_date = NULL
WHERE id = new.id;
END;
CREATE TRIGGER person_contact_ad
AFTER DELETE ON person_contact
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
UPDATE person
SET commit_date = NULL
WHERE id = old.id;
END;
-- [office]
CREATE TABLE office (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  photo_url TEXT,
  photo_attribution TEXT
);
--- FTS
CREATE VIRTUAL TABLE office_idx USING fts5(id, name, content = 'office');
CREATE TRIGGER office_ai
AFTER
INSERT ON office BEGIN
INSERT INTO office_idx(rowid, id, name)
VALUES (new.rowid, new.id, new.name);
END;
CREATE TRIGGER office_ad
AFTER DELETE ON office BEGIN
INSERT INTO office_idx(office_idx, rowid, id, name)
VALUES('delete', old.rowid, old.id, old.name);
END;
CREATE TRIGGER office_au
AFTER
UPDATE ON office BEGIN
INSERT INTO office_idx(office_idx, rowid, id, name)
VALUES('delete', old.rowid, old.id, old.name);
INSERT INTO office_idx(rowid, id, name)
VALUES (new.rowid, new.id, new.name);
END;
-- [office_contact]
CREATE TABLE office_contact (
  id TEXT NOT NULL,
  type TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (id, type) FOREIGN KEY(id) REFERENCES office(id)
);
CREATE TABLE supervisor (
  office_id TEXT NOT NULL,
  relation TEXT NOT NULL,
  supervisor_office_id TEXT NOT NULL,
  FOREIGN KEY(office_id) REFERENCES office(id)
);
-- [tenure]
CREATE TABLE tenure (
  person_id TEXT NOT NULL,
  office_id TEXT NOT NULL,
  start TEXT,
end TEXT,
FOREIGN KEY(person_id) REFERENCES person(id),
FOREIGN KEY(office_id) REFERENCES office(id)
);
--- commit tracking
CREATE TRIGGER tenure_ai
AFTER
INSERT ON tenure
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
UPDATE person
SET commit_date = NULL
WHERE id = new.person_id;
END;
CREATE TRIGGER tenure_au
AFTER
UPDATE ON tenure
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
UPDATE person
SET commit_date = NULL
WHERE id = new.person_id;
END;
CREATE TRIGGER tenure_ad
AFTER DELETE ON tenure
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
UPDATE person
SET commit_date = NULL
WHERE id = old.person_id;
END;
-- [incumbent]
CREATE VIEW incumbent (office_id, person_id, start) AS
SELECT office_id,
  person_id,
  start
FROM tenure
WHERE
end IS NULL;
--[quondam]
CREATE VIEW quondam (office_id, person_id, start, end) AS
SELECT office_id,
  person_id,
  start,
  end
FROM tenure
WHERE
end IS NOT NULL;