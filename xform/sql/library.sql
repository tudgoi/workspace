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
-- name: get_tenures->
-- Returns the tenures of the person with the given id
-- # Parameters
-- param: id: &str - person ID
SELECT office_id, start, end 
FROM person_office_tenure
WHERE person_id = :id
/
-- name: get_past_tenures?
-- Returns the past tenures of the person with the given id
-- # Parameters
-- param: id: &str - person ID
SELECT
    q.office_id,
    o.name,
    q.start,
    q.end
FROM person_office_quondam AS q
INNER JOIN office AS o ON o.id = q.office_id
WHERE q.person_id = :id
ORDER BY q.end DESC
/
-- name: save_tenure!
-- Save tenure of person in an office
-- # Parameters
-- param: person_id: &str
-- param: office_id: &str
-- param: start: Option<&chrono::NaiveDate>
-- param: end: Option<&chrono::NaiveDate>
INSERT INTO person_office_tenure (person_id, office_id, start, end)
VALUES (:person_id, :office_id, :start, :end)
/
-- name: save_office_supervisor!
-- Save tenure of person in an office
-- # Parameters
-- param: office_id: &str
-- param: relation: &crate::data::SupervisingRelation
-- param: supervisor_office_id: &str
INSERT INTO office_supervisor (office_id, relation, supervisor_office_id)
VALUES (:office_id, :relation, :supervisor_office_id)
/
-- name: exists_office_supervisor->
-- Returns if an office has a supervisor with the given relation
-- # Parameters
-- param: office_id: &str - office ID
-- param: relation: &crate::data::SupervisingRelation
SELECT EXISTS(
    SELECT 1 FROM office_supervisor WHERE office_id = :office_id AND relation = :relation
)
/
-- name: get_office_quondams?
-- Returns the quondams for a given office
-- # Parameters
-- param: office_id: &str
SELECT q.person_id, p.name, q.start, q.end FROM person_office_quondam AS q
JOIN person AS p ON q.person_id = p.id
WHERE q.office_id = :office_id ORDER BY q.end DESC
/
-- name: get_office_incumbent->
-- Returns the incumbent for a given office
-- # Parameters
-- param: office_id: &str
SELECT p.id, p.name FROM person_office_incumbent AS i
JOIN person AS p ON i.person_id = p.id
WHERE i.office_id = :office_id 
LIMIT 1
/
-- name: get_person_incumbent_office_details?
-- # Parameter
-- param: person_id: &str
SELECT i.office_id, e.name, p.url, p.attribution
FROM person_office_incumbent AS i
JOIN entity AS e ON i.office_id = e.id AND e.type = 'office'
LEFT JOIN entity_photo AS p ON i.office_id = p.entity_id AND p.entity_type = 'office'
WHERE i.person_id = :person_id
/
-- name: get_entity_commit_date->
-- # Parameter
-- param: typ: &crate::dto::EntityType
-- param: id: &str
SELECT
    date
FROM entity_commit
WHERE entity_type = :typ AND entity_id = :id
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