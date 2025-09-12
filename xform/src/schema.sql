CREATE TABLE person (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    photo_url TEXT,
    photo_attribution TEXT,
    updated TEXT
);
CREATE TABLE person_contact (
    id TEXT NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (id, type)
);
CREATE TABLE office (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    photo_url TEXT,
    photo_attribution TEXT
);
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
CREATE VIEW incumbent (
    office_id,
    person_id
) AS
SELECT office_id,
    person_id
FROM tenure
WHERE
end IS NULL;