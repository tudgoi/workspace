-- name: get_entity->
-- Returns the entity of the given type with the given id
-- # Parameters
-- param: typ: &str - entity type
-- param: id: &str - entity ID
SELECT name
FROM entity
WHERE type = :typ
    AND id = :id
LIMIT 1 /