use crate::repo::{Repo, test_backend::TestBackend};

#[test]
fn test_repo() {
    let backend = TestBackend::new();
    let mut repo = Repo::new(backend);

    // key level 0
    let k1: Vec<u8> = "name-0".as_bytes().to_vec();
    let k2: Vec<u8> = "age-0".as_bytes().to_vec();

    assert_eq!(repo.read(&k1).unwrap(), None);

    repo.write(k1.clone(), "val1".as_bytes().to_vec()).unwrap();
    assert_eq!(repo.read(&k1).unwrap(), Some("val1".as_bytes().to_vec()));

    repo.write(k2.clone(), "val2".as_bytes().to_vec()).unwrap();
    assert_eq!(repo.read(&k2).unwrap(), Some("val2".as_bytes().to_vec()));
    assert_eq!(repo.read(&k1).unwrap(), Some("val1".as_bytes().to_vec()));

    repo.write(k1.clone(), "val1_updated".as_bytes().to_vec()).unwrap();
    assert_eq!(repo.read(&k1).unwrap(), Some("val1_updated".as_bytes().to_vec()));
}