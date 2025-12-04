-- name: get_entity_counts->
-- Returns the number of person and office entities present
SELECT 
    COUNT(CASE WHEN type = 'person' THEN 1 END) AS persons,
    COUNT(CASE WHEN type = 'office' THEN 1 END) AS offices
FROM entity;
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
-- param: typ: &str - entity type
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
-- param: typ: &str - entity type
-- param: id: &str - entity ID
-- param: name: &str - name
UPDATE entity
SET name = :name
WHERE type = :typ
    AND id = :id
/
-- name: get_entity_photo->
-- Returns the photo of the entity of the given type with the given id
-- # Parameters
-- param: typ: &str - entity type
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
-- param: typ: &str - entity type
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