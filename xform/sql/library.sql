-- name: get_entity_counts->
-- Returns the number of person and office entities present
SELECT 
    COUNT(CASE WHEN type = 'person' THEN 1 END) AS persons,
    COUNT(CASE WHEN type = 'office' THEN 1 END) AS offices
FROM entity;
/
-- name: get_entity_uncommitted?
-- Returns the entities that are local to the DB and not yet committed to git
SELECT e.type, e.id, e.name
FROM entity AS e
LEFT JOIN entity_commit AS c
ON e.id=c.entity_id AND e.type = c.entity_type
WHERE c.date IS NULL
ORDER BY e.name;
/
-- name: new_entity!
-- Adds a new entity of the given type
-- param: typ: &str - entity type
-- param: id: &str - entity ID
-- param: name: &str - name
INSERT INTO entity (type, id, name)
VALUES (:typ, :id, :name);
/
-- name: get_entity_name->
-- Returns the name of the entity of the given type with the given id
-- # Parameters
-- param: typ: &crate::dto::EntityType - entity type
-- param: id: &str - entity ID
SELECT name
FROM entity
WHERE type = :typ
    AND id = :id
LIMIT 1
/
-- name: save_entity_name!
-- Save the entity of the given type with the given id
-- # Parameters
-- param: typ: &crate::dto::EntityType - entity type
-- param: id: &str - entity ID
-- param: name: &str - name
UPDATE entity
SET name = :name
WHERE type = :typ
    AND id = :id
/
-- name: exists_entity_photo->
-- Returns if an entity has a photo
-- # Parameters
-- param: typ: &dto::EntityType - entity type
-- param: id: &str - entity ID
SELECT EXISTS(
    SELECT 1
    FROM entity_photo
    WHERE entity_type = :typ AND entity_id = :id
)
/
-- name: get_entity_photo->
-- Returns the photo of the entity of the given type with the given id
-- # Parameters
-- param: typ: &dto::EntityType - entity type
-- param: id: &str - entity ID
SELECT url, attribution
FROM entity_photo
WHERE entity_type = :typ
    AND entity_id = :id
LIMIT 1
/
-- name: save_entity_photo!
-- Save the photo for the given type with the given id
-- # Parameters
-- param: typ: &dto::EntityType - entity type
-- param: id: &str - entity ID
-- param: url: &str - url
-- param: attribution: Option<&str> - attribution
INSERT INTO entity_photo (entity_type, entity_id, url, attribution)
VALUES (:typ, :id, :url, :attribution)
ON CONFLICT (entity_type, entity_id) DO UPDATE
SET
    url = :url,
    attribution = :attribution
WHERE
    entity_type = :typ AND entity_id = :id
/
-- name: attach_db!
-- Attaches the given DB as 'db'
-- # Parameters
-- param: path: &str
ATTACH DATABASE :path AS db;
/
-- name: copy_entity_from_db!
-- Copies all the rows from entity table from the previously attached DB
INSERT INTO entity SELECT * FROM db.entity;
/
-- name: detach_db!
-- Detaches the previously attached db
DETACH DATABASE db;
/