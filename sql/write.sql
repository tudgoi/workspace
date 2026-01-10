-- name: save_entity_name!
-- Save the entity of the given type with the given id
-- # Parameters
-- param: typ: &crate::dto::EntityType - entity type
-- param: id: &str - entity ID
-- param: name: &str - name
INSERT INTO entity (type, id, name)
VALUES (:typ, :id, :name)
ON CONFLICT (type, id) DO UPDATE SET name = :name;
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
-- name: save_entity_contact!
-- # Parameters
-- param: entity_type: &dto::EntityType
-- param: entity_id: &str
-- param: typ: &data::ContactType
-- param: value: &str
INSERT INTO entity_contact (entity_type, entity_id, type, value)
VALUES (:entity_type, :entity_id, :typ, :value)
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