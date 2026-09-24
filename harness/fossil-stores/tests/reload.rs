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

//! Phase-1 acceptance test: a whole jj repo round-trips through one SQLite
//! file.

use std::collections::BTreeSet;
use std::path::Path;

use jj_fossil_stores::FossilBackend;
use jj_fossil_stores::SqlOpHeadsStore;
use jj_fossil_stores::SqlOpStore;
use jj_fossil_stores::SqlWorkspaceStore;
use jj_fossil_stores::init_repo;
use jj_fossil_stores::init_workspace;
use jj_fossil_stores::store_factories;
use jj_lib::default_index::DefaultIndexStore;
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::ObjectId as _;
use jj_lib::object_id::PrefixResolution;
use jj_lib::op_store::OpStore as _;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::Repo as _;
use jj_lib::repo::RepoLoader;
use jj_lib::rewrite::rebase_commit;
use jj_lib::signing_factory::signer_from_settings;
use pollster::FutureExt as _;
use testutils::create_tree;
use testutils::repo_path;
use testutils::user_settings;

fn op_log(repo: &ReadonlyRepo) -> Vec<OperationId> {
    let mut ids = vec![];
    let mut op = repo.operation().clone();
    loop {
        ids.push(op.id().clone());
        let parents = op.parents().block_on().unwrap();
        let Some(parent) = parents.into_iter().next() else {
            break;
        };
        op = parent;
    }
    ids
}

/// Lists everything under `dir` except the jj index cache.
fn files_except_index(dir: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for entry in walk(dir) {
        let rel = entry
            .strip_prefix(dir)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        if !rel.starts_with("index") {
            out.insert(rel);
        }
    }
    out
}

fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = vec![];
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

#[test]
fn repo_round_trips_through_one_sqlite_file() {
    let settings = user_settings();
    let temp_dir = testutils::new_temp_dir();
    let repo_dir = temp_dir.path().join("repo");
    std::fs::create_dir(&repo_dir).unwrap();

    let repo = init_repo(
        &settings,
        &repo_dir,
        signer_from_settings(&settings).unwrap(),
    )
    .block_on()
    .unwrap();
    assert!(repo.store().backend_impl::<FossilBackend>().is_some());

    // base ─┬─ one
    //       └─ two   (then rebased onto `one`, which conflicts)
    let mut tx = repo.start_transaction();
    let root_id = repo.store().root_commit_id().clone();
    let base = tx
        .repo_mut()
        .new_commit(
            vec![root_id],
            create_tree(&repo, &[(repo_path("a"), "base\n")]),
        )
        .set_description("base")
        .write()
        .block_on()
        .unwrap();
    let one = tx
        .repo_mut()
        .new_commit(
            vec![base.id().clone()],
            create_tree(&repo, &[(repo_path("a"), "one\n")]),
        )
        .set_description("one")
        .write()
        .block_on()
        .unwrap();
    let two = tx
        .repo_mut()
        .new_commit(
            vec![base.id().clone()],
            create_tree(&repo, &[(repo_path("a"), "two\n")]),
        )
        .set_description("two")
        .write()
        .block_on()
        .unwrap();
    let repo = tx.commit("create commits").block_on().unwrap();

    let mut tx = repo.start_transaction();
    let rebased = rebase_commit(tx.repo_mut(), two, vec![one.id().clone()])
        .block_on()
        .unwrap();
    tx.repo_mut().rebase_descendants().block_on().unwrap();
    let repo = tx.commit("rebase two onto one").block_on().unwrap();
    assert!(rebased.has_conflict());

    let heads_before = repo.view().heads().clone();
    let ops_before = op_log(&repo);
    let rebased_before = repo.store().get_commit(rebased.id()).unwrap();
    assert_eq!(ops_before.len(), 3); // rebase, create, root

    // Only `type` markers and the database live outside the index cache.
    assert_eq!(
        files_except_index(&repo_dir),
        [
            "fossil.sqlite",
            "fossil.sqlite-shm",
            "fossil.sqlite-wal",
            "op_heads/type",
            "op_store/type",
            "store/type",
            "submodule_store/type",
            "workspace_store/type",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
    drop(repo);

    // The index is a cache: throw it away and let jj rebuild it on load.
    let index_dir = repo_dir.join("index");
    std::fs::remove_dir_all(&index_dir).unwrap();
    std::fs::create_dir(&index_dir).unwrap();
    DefaultIndexStore::init(&index_dir).unwrap();
    std::fs::write(index_dir.join("type"), DefaultIndexStore::NAME).unwrap();

    let loader =
        RepoLoader::init_from_file_system(&settings, &repo_dir, &store_factories()).unwrap();
    let repo = loader.load_at_head().block_on().unwrap();
    assert_eq!(repo.view().heads(), &heads_before);
    assert_eq!(op_log(&repo), ops_before);
    let rebased_after = repo.store().get_commit(rebased.id()).unwrap();
    assert_eq!(rebased_after, rebased_before);
    assert!(rebased_after.has_conflict());
    assert_eq!(rebased_after.description(), "two");
    assert!(
        repo.index()
            .is_ancestor(one.id(), rebased.id())
            .block_on()
            .unwrap()
    );

    // Operation ids resolve by prefix through SQL.
    let op_store = repo.op_store().downcast_ref::<SqlOpStore>().unwrap();
    let head_hex = repo.op_id().hex();
    assert_eq!(
        op_store
            .resolve_operation_id_prefix(&HexPrefix::try_from_hex(&head_hex[..12]).unwrap())
            .block_on()
            .unwrap(),
        PrefixResolution::SingleMatch(repo.op_id().clone())
    );
    assert_eq!(
        op_store
            .resolve_operation_id_prefix(&HexPrefix::try_from_hex("").unwrap())
            .block_on()
            .unwrap(),
        PrefixResolution::AmbiguousMatch
    );
    assert!(
        repo.op_heads_store()
            .downcast_ref::<SqlOpHeadsStore>()
            .is_some()
    );
}

#[test]
fn workspace_uses_sql_workspace_store() {
    let settings = user_settings();
    let temp_dir = testutils::new_temp_dir();
    let workspace_root = temp_dir.path().join("ws");
    std::fs::create_dir(&workspace_root).unwrap();
    let (workspace, repo) = init_workspace(
        &settings,
        &workspace_root,
        signer_from_settings(&settings).unwrap(),
    )
    .block_on()
    .unwrap();
    let workspace_store = repo.loader().workspace_store();
    assert_eq!(workspace_store.name(), SqlWorkspaceStore::NAME);
    assert_eq!(
        workspace_store
            .get_workspace_path(WorkspaceName::DEFAULT)
            .unwrap()
            .map(|path| workspace.repo_path().join(path).canonicalize().unwrap()),
        Some(workspace_root.canonicalize().unwrap())
    );
    // The working-copy commit is recorded in the SQL-backed view.
    assert!(
        repo.view()
            .get_wc_commit_id(WorkspaceName::DEFAULT)
            .is_some()
    );
}
