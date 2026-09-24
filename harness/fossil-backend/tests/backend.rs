// Copyright 2026 The Jujutsu Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::assert_matches;
use std::sync::Arc;

use futures::AsyncReadExt as _;
use jj_core::backend::Backend as _;
use jj_core::backend::BackendError;
use jj_core::backend::ChangeId;
use jj_core::backend::Commit;
use jj_core::backend::CommitId;
use jj_core::backend::CopyHistory;
use jj_core::backend::CopyId;
use jj_core::backend::FileId;
use jj_core::backend::MillisSinceEpoch;
use jj_core::backend::RelatedCopy;
use jj_core::backend::SecureSig;
use jj_core::backend::Signature;
use jj_core::backend::Timestamp;
use jj_core::backend::Tree;
use jj_core::backend::TreeId;
use jj_core::backend::TreeValue;
use jj_core::merge::Merge;
use jj_core::object_id::ObjectId as _;
use jj_core::repo_path::RepoPath;
use jj_core::repo_path::RepoPathBuf;
use jj_core::repo_path::RepoPathComponentBuf;
use jj_fossil_backend::FossilBackend;
use jj_fossil_cas::ArtifactHash;
use jj_fossil_cas::sql::RusqliteConn;
use jj_fossil_cas::sql::SqlConn;
use pollster::FutureExt as _;

/// The empty tree's id. Changing it changes every tree id in every repo.
const EMPTY_TREE_ID: &str = "9a2d81e10d0ef2dcc7dd7191b65202b682334b8d35b2676dbd140f18fe89f9ee";

fn backend() -> FossilBackend {
    let conn: Arc<dyn SqlConn> = Arc::new(RusqliteConn::open_in_memory().unwrap());
    FossilBackend::init(conn).unwrap()
}

fn signature() -> Signature {
    Signature {
        name: "Someone".to_owned(),
        email: "someone@example.com".to_owned(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(0),
            tz_offset: 0,
        },
    }
}

fn commit(backend: &FossilBackend, parents: Vec<CommitId>) -> Commit {
    Commit {
        parents,
        predecessors: vec![],
        root_tree: Merge::resolved(backend.empty_tree_id().clone()),
        conflict_labels: Merge::resolved(String::new()),
        change_id: ChangeId::from_hex("abc123"),
        description: String::new(),
        author: signature(),
        committer: signature(),
        secure_sig: None,
    }
}

fn write_file(backend: &FossilBackend, data: &[u8]) -> FileId {
    backend
        .write_file(RepoPath::root(), &mut &data[..])
        .block_on()
        .unwrap()
}

#[test]
fn empty_tree_id_is_pinned() {
    let backend = backend();
    assert_eq!(backend.empty_tree_id().hex(), EMPTY_TREE_ID);
    assert_eq!(
        backend
            .write_tree(RepoPath::root(), &Tree::default())
            .block_on()
            .unwrap(),
        *backend.empty_tree_id()
    );
    assert_eq!(
        backend
            .read_tree(RepoPath::root(), backend.empty_tree_id())
            .block_on()
            .unwrap(),
        Tree::default()
    );
}

#[test]
fn root_commit_is_virtual() {
    let backend = backend();
    assert_eq!(backend.root_commit_id().as_bytes(), &[0; 32]);
    let root = backend
        .read_commit(backend.root_commit_id())
        .block_on()
        .unwrap();
    assert!(root.parents.is_empty());
    assert_eq!(root.change_id, *backend.root_change_id());
}

#[test]
fn file_id_is_sha3_of_contents() {
    let backend = backend();
    let id = write_file(&backend, b"hello\n");
    assert_eq!(id.as_bytes(), ArtifactHash::of(b"hello\n").as_bytes());
    let mut out = vec![];
    backend
        .read_file(RepoPath::root(), &id)
        .block_on()
        .unwrap()
        .read_to_end(&mut out)
        .block_on()
        .unwrap();
    assert_eq!(out, b"hello\n");
}

#[test]
fn file_and_symlink_with_equal_bytes_share_a_blob() {
    let backend = backend();
    let file_id = write_file(&backend, b"target");
    let symlink_id = backend
        .write_symlink(RepoPath::root(), "target")
        .block_on()
        .unwrap();
    assert_eq!(file_id.as_bytes(), symlink_id.as_bytes());
    assert_eq!(
        backend
            .read_symlink(RepoPath::root(), &symlink_id)
            .block_on()
            .unwrap(),
        "target"
    );
    // Readable as a file too: file and symlink blobs are untyped.
    assert!(
        backend
            .read_file(RepoPath::root(), &file_id)
            .block_on()
            .is_ok()
    );
}

#[test]
fn symlink_with_invalid_utf8_is_an_error() {
    let backend = backend();
    let id = write_file(&backend, b"\xff\xfe");
    let symlink_id = jj_core::backend::SymlinkId::new(id.to_bytes());
    assert_matches!(
        backend
            .read_symlink(RepoPath::root(), &symlink_id)
            .block_on(),
        Err(BackendError::InvalidUtf8 { .. })
    );
}

#[test]
fn tree_round_trip_with_placeholder_copy_id() {
    let backend = backend();
    let file = write_file(&backend, b"x");
    let tree = Tree::from_sorted_entries(vec![(
        RepoPathComponentBuf::new("f").unwrap(),
        TreeValue::File {
            id: file,
            executable: false,
            copy_id: CopyId::placeholder(),
        },
    )]);
    let id = backend
        .write_tree(RepoPath::root(), &tree)
        .block_on()
        .unwrap();
    assert_eq!(
        backend.read_tree(RepoPath::root(), &id).block_on().unwrap(),
        tree
    );
}

#[test]
fn lookups_validate_ids() {
    let backend = backend();
    assert_matches!(
        backend.read_commit(&CommitId::from_hex("abcd")).block_on(),
        Err(BackendError::InvalidHashLength {
            expected: 32,
            actual: 2,
            ..
        })
    );
    assert_matches!(
        backend.read_copy(&CopyId::placeholder()).block_on(),
        Err(BackendError::InvalidHashLength { .. })
    );
    assert_matches!(
        backend
            .read_tree(RepoPath::root(), &TreeId::new(vec![1; 32]))
            .block_on(),
        Err(BackendError::ObjectNotFound { .. })
    );
    // A commit read as a tree fails to decode instead of being misread.
    let (commit_id, _) = backend
        .write_commit(
            commit(&backend, vec![backend.root_commit_id().clone()]),
            None,
        )
        .block_on()
        .unwrap();
    assert_matches!(
        backend
            .read_tree(RepoPath::root(), &TreeId::new(commit_id.to_bytes()))
            .block_on(),
        Err(BackendError::ReadObject { .. })
    );
}

/// Ported from `SimpleBackend`'s `write_commit_parents` test.
#[test]
fn write_commit_parents() {
    let backend = backend();
    let mut commit = commit(&backend, vec![]);
    let write = |commit: Commit| backend.write_commit(commit, None).block_on();

    commit.parents = vec![];
    assert_matches!(
        write(commit.clone()),
        Err(BackendError::Other(err)) if err.to_string().contains("no parents")
    );

    commit.parents = vec![backend.root_commit_id().clone()];
    let first_id = write(commit.clone()).unwrap().0;
    assert_eq!(backend.read_commit(&first_id).block_on().unwrap(), commit);

    commit.parents = vec![first_id.clone()];
    let second_id = write(commit.clone()).unwrap().0;
    assert_eq!(backend.read_commit(&second_id).block_on().unwrap(), commit);

    commit.parents = vec![first_id.clone(), second_id.clone()];
    let merge_id = write(commit.clone()).unwrap().0;
    assert_eq!(backend.read_commit(&merge_id).block_on().unwrap(), commit);

    commit.parents = vec![first_id, backend.root_commit_id().clone()];
    let root_merge_id = write(commit.clone()).unwrap().0;
    assert_eq!(
        backend.read_commit(&root_merge_id).block_on().unwrap(),
        commit
    );
}

#[test]
fn identical_commits_get_identical_ids() {
    let backend = backend();
    let c = commit(&backend, vec![backend.root_commit_id().clone()]);
    let (a, _) = backend.write_commit(c.clone(), None).block_on().unwrap();
    let (b, _) = backend.write_commit(c, None).block_on().unwrap();
    assert_eq!(a, b);
}

#[test]
fn conflicted_commit_round_trip() {
    let backend = backend();
    let mut c = commit(&backend, vec![backend.root_commit_id().clone()]);
    c.root_tree = Merge::from_vec(vec![
        TreeId::new(vec![1; 32]),
        TreeId::new(vec![2; 32]),
        TreeId::new(vec![3; 32]),
    ]);
    c.conflict_labels = Merge::from_vec(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]);
    let (id, returned) = backend.write_commit(c.clone(), None).block_on().unwrap();
    assert_eq!(returned, c);
    assert_eq!(backend.read_commit(&id).block_on().unwrap(), c);
}

#[test]
fn write_commit_returns_what_read_commit_returns() {
    let backend = backend();
    let mut c = commit(&backend, vec![backend.root_commit_id().clone()]);
    // Resolved tree with a non-empty label is normalized away.
    c.conflict_labels = Merge::resolved("stray".to_owned());
    let (id, returned) = backend.write_commit(c, None).block_on().unwrap();
    assert_eq!(returned.conflict_labels, Merge::resolved(String::new()));
    assert_eq!(backend.read_commit(&id).block_on().unwrap(), returned);
}

#[test]
fn signed_commits() {
    let backend = backend();
    let c = commit(&backend, vec![backend.root_commit_id().clone()]);
    let (unsigned_id, _) = backend.write_commit(c.clone(), None).block_on().unwrap();

    let mut signed_data = None;
    let mut sign = |data: &[u8]| {
        signed_data = Some(data.to_vec());
        Ok(b"sig".to_vec())
    };
    let (signed_id, returned) = backend
        .write_commit(c.clone(), Some(&mut sign))
        .block_on()
        .unwrap();
    assert_ne!(signed_id, unsigned_id);
    let expected_sig = SecureSig {
        data: signed_data.unwrap(),
        sig: b"sig".to_vec(),
    };
    assert_eq!(returned.secure_sig.as_ref(), Some(&expected_sig));
    let read = backend.read_commit(&signed_id).block_on().unwrap();
    assert_eq!(read, returned);
    // The signed data is the unsigned object, whose hash is the unsigned id.
    assert_eq!(
        ArtifactHash::of(&expected_sig.data).as_bytes(),
        unsigned_id.as_bytes()
    );

    // A caller-provided signature is refused.
    let mut presigned = c;
    presigned.secure_sig = Some(expected_sig);
    assert_matches!(
        backend.write_commit(presigned, None).block_on(),
        Err(BackendError::Other(_))
    );
}

fn copy_history(path: &str, parents: &[CopyId]) -> CopyHistory {
    CopyHistory {
        current_path: RepoPathBuf::from_internal_string(path).unwrap(),
        parents: parents.to_vec(),
        salt: vec![],
    }
}

/// Mirrors `TestBackend`'s `get_related_copies` test.
#[test]
fn related_copies() {
    let backend = backend();
    let copy1 = copy_history("foo1", &[]);
    let copy1_id = backend.write_copy(&copy1).block_on().unwrap();
    let copy2 = copy_history("foo2", std::slice::from_ref(&copy1_id));
    let copy2_id = backend.write_copy(&copy2).block_on().unwrap();
    let copy3 = copy_history("foo3", std::slice::from_ref(&copy2_id));
    let copy3_id = backend.write_copy(&copy3).block_on().unwrap();
    assert_eq!(backend.read_copy(&copy2_id).block_on().unwrap(), copy2);

    assert_matches!(
        backend
            .get_related_copies(&CopyId::new(vec![7; 32]))
            .block_on(),
        Err(BackendError::ObjectNotFound { .. })
    );

    let expected = vec![
        RelatedCopy {
            id: copy3_id.clone(),
            history: copy3,
        },
        RelatedCopy {
            id: copy2_id.clone(),
            history: copy2,
        },
        RelatedCopy {
            id: copy1_id.clone(),
            history: copy1,
        },
    ];
    for id in [&copy1_id, &copy2_id, &copy3_id] {
        assert_eq!(backend.get_related_copies(id).block_on().unwrap(), expected);
    }

    // An unrelated copy is not included.
    let other = backend
        .write_copy(&copy_history("other", &[]))
        .block_on()
        .unwrap();
    assert_eq!(
        backend.get_related_copies(&other).block_on().unwrap().len(),
        1
    );
}

#[test]
fn copy_with_unknown_parent_is_rejected() {
    let backend = backend();
    assert_matches!(
        backend
            .write_copy(&copy_history("x", &[CopyId::new(vec![9; 32])]))
            .block_on(),
        Err(BackendError::ObjectNotFound { .. })
    );
    // A parent that exists but isn't a copy history is rejected too.
    let file = write_file(&backend, b"not a copy");
    assert_matches!(
        backend
            .write_copy(&copy_history("x", &[CopyId::new(file.to_bytes())]))
            .block_on(),
        Err(BackendError::ReadObject { .. })
    );
}

#[test]
fn reload_from_disk() {
    let dir = tempfile_dir();
    let store_path = dir.join("store");
    std::fs::create_dir(&store_path).unwrap();
    let (commit_id, written) = {
        let backend = FossilBackend::init_at(&store_path).unwrap();
        let c = commit(&backend, vec![backend.root_commit_id().clone()]);
        backend.write_commit(c, None).block_on().unwrap()
    };
    assert!(dir.join(FossilBackend::DB_FILE).exists());
    let backend = FossilBackend::load_at(&store_path).unwrap();
    assert_eq!(backend.read_commit(&commit_id).block_on().unwrap(), written);
    std::fs::remove_dir_all(&dir).unwrap();
}

fn tempfile_dir() -> std::path::PathBuf {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    static N: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "jj-fossil-backend-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Pins the commit encoding: changing it changes every commit id.
#[test]
fn commit_id_is_pinned() {
    let backend = backend();
    let c = commit(&backend, vec![backend.root_commit_id().clone()]);
    let (id, _) = backend.write_commit(c, None).block_on().unwrap();
    assert_eq!(
        id.hex(),
        "9ae94ce857d89ef2fdd52783d527dad22dbda459b29a57d3305944226e038b10"
    );
}

#[test]
fn large_files_stream_through_chunks() {
    // A small value limit, as on a Durable Object, forces chunking.
    let conn: Arc<dyn SqlConn> = Arc::new(
        RusqliteConn::open_in_memory()
            .unwrap()
            .with_max_value_len(4096),
    );
    let backend = FossilBackend::init(conn).unwrap();
    let mut state: u32 = 7;
    let data: Vec<u8> = (0..100_000)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect();
    let id = write_file(&backend, &data);
    assert_eq!(id.as_bytes(), ArtifactHash::of(&data).as_bytes());
    let mut out = vec![];
    backend
        .read_file(RepoPath::root(), &id)
        .block_on()
        .unwrap()
        .read_to_end(&mut out)
        .block_on()
        .unwrap();
    assert_eq!(out, data);
    // Writing the same content again dedupes to the same blob.
    assert_eq!(write_file(&backend, &data), id);
}

/// Children come before parents, and the order doesn't depend on which copy
/// the walk starts from.
#[test]
fn related_copies_order_is_deterministic() {
    let backend = backend();
    let root = backend
        .write_copy(&copy_history("root", &[]))
        .block_on()
        .unwrap();
    let left = backend
        .write_copy(&copy_history("left", std::slice::from_ref(&root)))
        .block_on()
        .unwrap();
    let right = backend
        .write_copy(&copy_history("right", std::slice::from_ref(&root)))
        .block_on()
        .unwrap();
    let merged = backend
        .write_copy(&copy_history("merged", &[left.clone(), right.clone()]))
        .block_on()
        .unwrap();
    let orders: Vec<Vec<CopyId>> = [&root, &left, &right, &merged]
        .into_iter()
        .map(|id| {
            backend
                .get_related_copies(id)
                .block_on()
                .unwrap()
                .into_iter()
                .map(|related| related.id)
                .collect()
        })
        .collect();
    for order in &orders {
        assert_eq!(order, &orders[0]);
        let pos = |id: &CopyId| order.iter().position(|x| x == id).unwrap();
        assert!(pos(&merged) < pos(&left) && pos(&merged) < pos(&right));
        assert!(pos(&left) < pos(&root) && pos(&right) < pos(&root));
    }
    assert_eq!(orders[0].len(), 4);
}

#[test]
fn related_copies_survive_cyclic_edges() {
    let backend = backend();
    let a = backend
        .write_copy(&copy_history("a", &[]))
        .block_on()
        .unwrap();
    let b = backend
        .write_copy(&copy_history("b", std::slice::from_ref(&a)))
        .block_on()
        .unwrap();
    // Corrupt the edge index with a cycle (b -> a -> b). The walk must still
    // terminate, and ordering follows the copies' own parent lists.
    let conn = backend.blob_store().conn().clone();
    jj_fossil_cas::sql::with(conn.as_ref(), jj_fossil_cas::sql::Access::Write, |x| {
        x.exec(
            "INSERT INTO jj_copy_edge(child, parent) SELECT pa.rid, ch.rid FROM blob pa, blob ch \
             WHERE pa.uuid = ? AND ch.uuid = ?",
            &[
                jj_fossil_cas::sql::Param::Text(
                    &ArtifactHash::from_slice(a.as_bytes()).unwrap().to_uuid(),
                ),
                jj_fossil_cas::sql::Param::Text(
                    &ArtifactHash::from_slice(b.as_bytes()).unwrap().to_uuid(),
                ),
            ],
        )
    })
    .unwrap();
    let ids: Vec<CopyId> = backend
        .get_related_copies(&a)
        .block_on()
        .unwrap()
        .into_iter()
        .map(|related| related.id)
        .collect();
    assert_eq!(ids, vec![b, a]);
}
