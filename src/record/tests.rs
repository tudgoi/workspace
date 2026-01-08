use super::*;
use crate::data::{Photo, ContactType, SupervisingRelation};
use crate::repo::test_backend::TestBackend;

#[test]
fn test_person_name_save_load() {
    let backend = TestBackend::new();
    let mut repo = RecordRepo::new(backend);
    let person_key = Key::<PersonPath, ()>::new("p1");
    let name_key = person_key.name();
    
    let name = "Alice".to_string();
    repo.save(name_key.clone(), &name).unwrap();
    
    let loaded_name = repo.load(name_key).unwrap();
    assert_eq!(loaded_name, Some("Alice".to_string()));
}

#[test]
fn test_person_photo_save_load() {
    let backend = TestBackend::new();
    let mut repo = RecordRepo::new(backend);
    let person_key = Key::<PersonPath, ()>::new("p2");
    let photo_key = person_key.photo();
    
    let photo = Photo {
        url: "http://example.com/photo.jpg".to_string(),
        attribution: Some("Photographer".to_string()),
    };
    
    repo.save(photo_key.clone(), &photo).unwrap();
    
    let loaded_photo = repo.load(photo_key).unwrap();
    assert_eq!(loaded_photo, Some(photo));
}

#[test]
fn test_person_contact_save_load() {
    let backend = TestBackend::new();
    let mut repo = RecordRepo::new(backend);
    let person_key = Key::<PersonPath, ()>::new("p3");
    let contact_key = person_key.contact(ContactType::Email);
    
    let email = "alice@example.com".to_string();
    
    repo.save(contact_key.clone(), &email).unwrap();
    
    let loaded_email = repo.load(contact_key).unwrap();
    assert_eq!(loaded_email, Some(email));
}

#[test]
fn test_office_supervisor_save_load() {
    let backend = TestBackend::new();
    let mut repo = RecordRepo::new(backend);
    let office_key = Key::<OfficePath, ()>::new("o1");
    let supervisor_key = office_key.supervisor(SupervisingRelation::Head);
    
    let supervisor_id = "p1".to_string();
    
    repo.save(supervisor_key.clone(), &supervisor_id).unwrap();
    
    let loaded_supervisor = repo.load(supervisor_key).unwrap();
    assert_eq!(loaded_supervisor, Some(supervisor_id));
}

#[test]
fn test_tenure_save_load() {
    let backend = TestBackend::new();
    let mut repo = RecordRepo::new(backend);
    let person_key = Key::<PersonPath, ()>::new("p4");
    let tenure_key = person_key.tenure("o1", Some(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()));
    
    let end_date = Some(NaiveDate::from_ymd_opt(2023, 12, 31).unwrap());
    
    repo.save(tenure_key.clone(), &end_date).unwrap();
    
    let loaded_end_date = repo.load(tenure_key).unwrap();
    assert_eq!(loaded_end_date, Some(end_date));
}

#[test]

fn test_name_photo_separation() {

    let backend = TestBackend::new();

    let mut repo = RecordRepo::new(backend);

    let person_key = Key::<PersonPath, ()>::new("p5");

    let name_key = person_key.name();

    let photo_key = person_key.photo();

    

    let name = "Bob".to_string();

    let photo = Photo {

        url: "http://example.com/bob.jpg".to_string(),

        attribution: None,

    };

    

    repo.save(name_key.clone(), &name).unwrap();

    repo.save(photo_key.clone(), &photo).unwrap();

    

    let loaded_name = repo.load(name_key).unwrap();

    assert_eq!(loaded_name, Some(name));



    let loaded_photo = repo.load(photo_key).unwrap();

    assert_eq!(loaded_photo, Some(photo));

}
