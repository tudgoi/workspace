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

    repo.write(k1.clone(), "val1_updated".as_bytes().to_vec())
        .unwrap();
    assert_eq!(
        repo.read(&k1).unwrap(),
        Some("val1_updated".as_bytes().to_vec())
    );
}

#[test]
fn test_hash_display() {
    use crate::repo::Hash;
    let mut data = [0u8; 32];
    data[0] = 0x12;
    data[1] = 0xab;
    data[31] = 0xff;
    let hash = Hash(data);
    let s = hash.to_string();
    assert!(s.starts_with("12ab00"));
    assert!(s.ends_with("ff"));
    assert_eq!(s.len(), 64);
}

#[test]
fn test_iter_prefix() {
    let backend = TestBackend::new();
    let mut repo = Repo::new(backend);

    repo.write(b"apple".to_vec(), b"val1".to_vec()).unwrap();
    repo.write(b"apricot".to_vec(), b"val2".to_vec()).unwrap();
    repo.write(b"banana".to_vec(), b"val3".to_vec()).unwrap();
    repo.write(b"application".to_vec(), b"val4".to_vec())
        .unwrap();
    repo.write(b"apply".to_vec(), b"val5".to_vec()).unwrap();

    let mut visited = Vec::new();
    for item in repo.iter_prefix(b"app").unwrap() {
        let (k, _) = item.unwrap();
        visited.push(k);
    }

    assert_eq!(
        visited,
        vec![
            b"apple".to_vec(),
            b"application".to_vec(),
            b"apply".to_vec()
        ]
    );

    visited.clear();
    for item in repo.iter_prefix(b"ban").unwrap() {
        let (k, _) = item.unwrap();
        visited.push(k);
    }
    assert_eq!(visited, vec![b"banana".to_vec()]);

    visited.clear();
    for item in repo.iter_prefix(b"z").unwrap() {
        let (k, _) = item.unwrap();
        visited.push(k);
    }
    assert!(visited.is_empty());
}
