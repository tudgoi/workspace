use crate::repo::{Repo, test_backend::TestBackend, Backend};

#[test]
fn test_repo() {
    let backend = TestBackend::new();
    let repo = Repo::new(backend);

    // key level 0
    let k1: Vec<u8> = "name-0".as_bytes().to_vec();
    let k2: Vec<u8> = "age-0".as_bytes().to_vec();

    assert_eq!(repo.root().unwrap().read(&k1).unwrap(), None);

    repo.root().unwrap().write(k1.clone(), "val1".as_bytes().to_vec()).unwrap();
    assert_eq!(repo.root().unwrap().read(&k1).unwrap(), Some("val1".as_bytes().to_vec()));

    repo.root().unwrap().write(k2.clone(), "val2".as_bytes().to_vec()).unwrap();
    assert_eq!(repo.root().unwrap().read(&k2).unwrap(), Some("val2".as_bytes().to_vec()));
    assert_eq!(repo.root().unwrap().read(&k1).unwrap(), Some("val1".as_bytes().to_vec()));

    repo.root().unwrap().write(k1.clone(), "val1_updated".as_bytes().to_vec())
        .unwrap();
    assert_eq!(
        repo.root().unwrap().read(&k1).unwrap(),
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
    let repo = Repo::new(backend);

    repo.root().unwrap().write(b"apple".to_vec(), b"val1".to_vec()).unwrap();
    repo.root().unwrap().write(b"apricot".to_vec(), b"val2".to_vec()).unwrap();
    repo.root().unwrap().write(b"banana".to_vec(), b"val3".to_vec()).unwrap();
    repo.root().unwrap().write(b"application".to_vec(), b"val4".to_vec())
        .unwrap();
    repo.root().unwrap().write(b"apply".to_vec(), b"val5".to_vec()).unwrap();

    let mut visited = Vec::new();
    for item in repo.root().unwrap().iter_prefix(b"app").unwrap() {
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
    for item in repo.root().unwrap().iter_prefix(b"ban").unwrap() {
        let (k, _) = item.unwrap();
        visited.push(k);
    }
    assert_eq!(visited, vec![b"banana".to_vec()]);

    visited.clear();
    for item in repo.root().unwrap().iter_prefix(b"z").unwrap() {
        let (k, _) = item.unwrap();
        visited.push(k);
    }
    assert!(visited.is_empty());
}

#[test]
fn test_committed_ref() {
    use crate::repo::{ROOT_REF, COMMITTED_REF};
    let backend = TestBackend::new();
    let mut repo = Repo::new(backend);

    repo.root().unwrap().write(b"k1".to_vec(), b"v1".to_vec()).unwrap();
    let root_hash = repo.backend.get_ref(ROOT_REF).unwrap();
    let committed_hash = repo.backend.get_ref(COMMITTED_REF).unwrap();
    
    assert!(root_hash.is_some());
    assert!(committed_hash.is_none());

    repo.commit().unwrap();
    let committed_hash = repo.backend.get_ref(COMMITTED_REF).unwrap();
    assert_eq!(root_hash, committed_hash);

    repo.root().unwrap().write(b"k2".to_vec(), b"v2".to_vec()).unwrap();
    let new_root_hash = repo.backend.get_ref(ROOT_REF).unwrap();
    let committed_hash_after = repo.backend.get_ref(COMMITTED_REF).unwrap();
    
    assert_ne!(new_root_hash, committed_hash);
    assert_eq!(committed_hash_after, committed_hash);
}

#[test]
fn test_iterate_diff() {
    use crate::repo::{Diff};
    let backend = TestBackend::new();
    let mut repo = Repo::new(backend);

    // Initial state
    repo.root().unwrap().write(b"a".to_vec(), b"1".to_vec()).unwrap();
    repo.root().unwrap().write(b"b".to_vec(), b"2".to_vec()).unwrap();
    repo.commit().unwrap();

    // Modifications
    repo.root().unwrap().write(b"b".to_vec(), b"22".to_vec()).unwrap(); // Changed
    repo.root().unwrap().write(b"c".to_vec(), b"3".to_vec()).unwrap();  // Added
    // MST doesn't support deletion yet, so we only test Added and Changed.
    
    let root = repo.root().unwrap();
    let committed = repo.committed().unwrap();
    let diffs: Vec<_> = committed.iterate_diff(&root).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    
    assert_eq!(diffs.len(), 2);
    
    let mut found_added = false;
    let mut found_changed = false;
    
    for diff in diffs {
        match diff {
            Diff::Added(k, v) => {
                assert_eq!(k, b"c");
                assert_eq!(v, b"3");
                found_added = true;
            }
            Diff::Changed(k, old_v, new_v) => {
                assert_eq!(k, b"b");
                assert_eq!(old_v, b"2");
                assert_eq!(new_v, b"22");
                found_changed = true;
            }
            Diff::Removed(_, _) => panic!("Unexpected removal"),
        }
    }
    
    assert!(found_added);
    assert!(found_changed);
}
