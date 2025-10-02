-- [entity_contact]
CREATE TABLE entity_contact (
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  type TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (entity_type, entity_id, type) FOREIGN KEY(entity_type, entity_id) REFERENCES entity(type, id)
);
-- [person]
CREATE VIEW person (id, name) AS
SELECT id,
  name
FROM entity
WHERE type = "person";
-- [office]
CREATE VIEW office (id, name) AS
SELECT id,
  name
FROM entity
WHERE type = "office";
-- [office_supervisor]
CREATE TABLE office_supervisor (
  office_id TEXT NOT NULL,
  relation TEXT NOT NULL,
  supervisor_office_id TEXT NOT NULL,
  PRIMARY KEY(office_id, relation)
);
-- [person_office_tenure]
CREATE TABLE person_office_tenure (
  person_id TEXT NOT NULL,
  office_id TEXT NOT NULL,
  start TEXT,
end TEXT
);
-- [person_office_incumbent]
CREATE VIEW person_office_incumbent (person_id, office_id, start) AS
SELECT person_id,
  office_id,
  start
FROM person_office_tenure
WHERE
end IS NULL;
--[person_office_quondam]
CREATE VIEW person_office_quondam (person_id, office_id, start,end
) AS
SELECT office_id,
  person_id,
  start,
end
FROM person_office_tenure
WHERE
end IS NOT NULL;
-- [commit_tracking]
CREATE TABLE commit_tracking (
  id INTEGER PRIMARY KEY,
  enabled INTEGER NOT NULL DEFAULT 0
);
-- [entity_commit]
CREATE TABLE entity_commit (
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  date TEXT NOT NULL,
  PRIMARY KEY(entity_type, entity_id) FOREIGN KEY(entity_type, entity_id) REFERENCES entity(type, id) UNIQUE(entity_type, entity_id)
);
--- for [entity]
CREATE TRIGGER entity_au_commit
AFTER
UPDATE ON entity
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = new.type
  AND entity_id = new.id;
END;
CREATE TRIGGER entity_ad_commit
AFTER DELETE ON entity
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = old.type
  AND entity_id = old.id;
END;
--- for entity_photo
CREATE TRIGGER entity_photo_ai_commit
AFTER
INSERT ON entity_photo
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = new.entity_type
  AND entity_id = new.entity_id;
END;
CREATE TRIGGER entity_photo_au_commit
AFTER
UPDATE ON entity_photo
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = new.entity_type
  AND entity_id = new.entity_id;
END;
CREATE TRIGGER entity_photo_ad_commit
AFTER DELETE ON entity_photo
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = old.entity_type
  AND entity_id = old.entity_id;
END;
--- for entity_contact
CREATE TRIGGER entity_contact_ai_commit
AFTER
INSERT ON entity_contact
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = new.entity_type
  AND entity_id = new.entity_id;
END;
CREATE TRIGGER entity_contact_au_commit
AFTER
UPDATE ON entity_contact
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = new.entity_type
  AND entity_id = new.entity_id;
END;
CREATE TRIGGER entity_contact_ad_commit
AFTER DELETE ON entity_contact
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = old.entity_type
  AND entity_id = old.entity_id;
END;
--- for office_supervisor
CREATE TRIGGER office_supervisor_ai_commit
AFTER
INSERT ON office_supervisor
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = 'office'
  AND entity_id = new.office_id;
END;
CREATE TRIGGER office_supervisor_au_commit
AFTER
UPDATE ON office_supervisor
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = 'office'
  AND entity_id = new.office_id;
END;
CREATE TRIGGER office_supervisor_ad_commit
AFTER DELETE ON office_supervisor
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = 'office'
  AND entity_id = old.office_id;
END;
--- for person_office_tenure
CREATE TRIGGER person_office_tenure_ai_commit
AFTER
INSERT ON person_office_tenure
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = 'person'
  AND entity_id = new.person_id;
END;
CREATE TRIGGER person_office_tenure_au_commit
AFTER
UPDATE ON person_office_tenure
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = 'person'
  AND entity_id = new.person_id;
END;
CREATE TRIGGER person_office_tenure_ad_commit
AFTER DELETE ON person_office_tenure
  WHEN (
    SELECT enabled
    FROM commit_tracking
  ) > 0 BEGIN
DELETE FROM entity_commit
WHERE entity_type = 'person'
  AND entity_id = old.person_id;
END;