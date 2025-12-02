-- name: get_entity->
-- Returns the entity of the given type with the given id
-- # Parameters
-- param: typ: &str - entity type
-- param: id: &str - entity ID
SELECT name
FROM entity
WHERE type = :typ
    AND id = :id
LIMIT 1
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
