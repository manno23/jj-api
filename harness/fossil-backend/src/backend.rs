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

//! [`FossilBackend`]: `jj_core::backend::Backend` over a [`BlobStore`].

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::AsyncRead;
use futures::AsyncReadExt as _;
use futures::StreamExt as _;
use futures::io::AllowStdIo;
use futures::stream;
use futures::stream::BoxStream;
use jj_core::backend::Backend;
use jj_core::backend::BackendError;
use jj_core::backend::BackendInitError;
use jj_core::backend::BackendLoadError;
use jj_core::backend::BackendResult;
use jj_core::backend::ChangeId;
use jj_core::backend::Commit;
use jj_core::backend::CommitId;
use jj_core::backend::CopyHistory;
use jj_core::backend::CopyId;
use jj_core::backend::CopyRecord;
use jj_core::backend::FileId;
use jj_core::backend::RelatedCopy;
use jj_core::backend::SigningFn;
use jj_core::backend::SymlinkId;
use jj_core::backend::Tree;
use jj_core::backend::TreeId;
use jj_core::backend::make_root_commit;
use jj_core::dag_walk::topo_order_reverse;
use jj_core::object_id::ObjectId;
use jj_core::repo_path::RepoPath;
use jj_core::repo_path::RepoPathBuf;
use jj_fossil_cas::ArtifactHash;
use jj_fossil_cas::BlobStore;
use jj_fossil_cas::CasError;
use jj_fossil_cas::schema::ensure_version;
use jj_fossil_cas::sql::Access;
use jj_fossil_cas::sql::Param;
use jj_fossil_cas::sql::SqlConn;
use jj_fossil_cas::sql::SqlError;
use jj_fossil_cas::sql::SqlExec;
use jj_fossil_cas::sql::with;

use crate::codec;
use crate::codec::Kind;

/// Length of commit ids (and every other object id): SHA3-256.
pub const COMMIT_ID_LENGTH: usize = ArtifactHash::LEN;
/// Length of change ids.
pub const CHANGE_ID_LENGTH: usize = 16;

/// Tables owned by the backend. `jj_object` indexes the structured objects
/// (trees, commits, copies) for GC and tooling; reads never consult it.
/// `jj_copy_edge` lets related copies be found in both directions.
pub const BACKEND_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS jj_object(
  rid INTEGER PRIMARY KEY REFERENCES blob,
  kind INTEGER NOT NULL CHECK(kind IN (1, 2, 3))
);
CREATE TABLE IF NOT EXISTS jj_copy_edge(
  child INTEGER NOT NULL REFERENCES blob,
  parent INTEGER NOT NULL REFERENCES blob,
  PRIMARY KEY(child, parent)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS jj_copy_edge_parent ON jj_copy_edge(parent, child);
";

/// Schema version of [`BACKEND_SCHEMA`].
pub const BACKEND_SCHEMA_VERSION: i64 = 1;

/// A jj commit backend storing every object in a Fossil-style blob table.
///
/// Every id is the SHA3-256 of the object's stored bytes, which is also its
/// blob `uuid`. File and symlink contents are stored verbatim (so they are
/// valid Fossil file artifacts); trees, commits and copy histories use the
/// canonical encoding in [`crate::codec`].
#[derive(Debug)]
pub struct FossilBackend {
    cas: BlobStore,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}

fn to_other_err(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> BackendError {
    BackendError::Other(err.into())
}

fn read_err(
    id: &impl ObjectId,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> BackendError {
    BackendError::ReadObject {
        object_type: id.object_type(),
        hash: id.hex(),
        source: err.into(),
    }
}

fn write_err(
    object_type: &'static str,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> BackendError {
    BackendError::WriteObject {
        object_type,
        source: err.into(),
    }
}

fn not_found(id: &impl ObjectId) -> BackendError {
    BackendError::ObjectNotFound {
        object_type: id.object_type(),
        hash: id.hex(),
        source: "no such blob".into(),
    }
}

/// Carries a [`BackendError`] out of a SQL scope.
struct ScopeError(BackendError);

impl From<SqlError> for ScopeError {
    fn from(err: SqlError) -> Self {
        Self(to_other_err(err))
    }
}

/// Runs `f` in a SQL scope, with backend errors passing through unchanged.
fn with_scope<T>(
    conn: &dyn SqlConn,
    access: Access,
    f: impl FnOnce(&dyn SqlExec) -> BackendResult<T>,
) -> BackendResult<T> {
    with(conn, access, |x| f(x).map_err(ScopeError)).map_err(|ScopeError(err)| err)
}

/// Converts an object id to its blob hash, rejecting ids of the wrong length.
fn hash_of(id: &impl ObjectId) -> BackendResult<ArtifactHash> {
    ArtifactHash::from_slice(id.as_bytes()).ok_or_else(|| BackendError::InvalidHashLength {
        expected: COMMIT_ID_LENGTH,
        actual: id.as_bytes().len(),
        object_type: id.object_type(),
        hash: id.hex(),
    })
}

impl FossilBackend {
    /// The name recorded in `.jj/repo/store/type`.
    pub const NAME: &str = "fossil";
    /// File name of the database inside a jj repo directory.
    pub const DB_FILE: &str = "fossil.sqlite";

    /// Initialises the backend's tables in `conn` and writes the empty tree.
    pub fn init(conn: Arc<dyn SqlConn>) -> Result<Self, BackendInitError> {
        let backend = Self::open(conn).map_err(|err| BackendInitError(err.into()))?;
        let (_, hash) = backend
            .cas
            .put(&codec::empty_tree_bytes())
            .map_err(|err| BackendInitError(err.into()))?;
        assert_eq!(hash.as_bytes(), backend.empty_tree_id.as_bytes());
        Ok(backend)
    }

    /// Loads the backend from a database created by [`Self::init`].
    pub fn load(conn: Arc<dyn SqlConn>) -> Result<Self, BackendLoadError> {
        Self::open(conn).map_err(|err| BackendLoadError(err.into()))
    }

    fn open(conn: Arc<dyn SqlConn>) -> Result<Self, CasError> {
        let cas = BlobStore::open(conn)?;
        with(cas.conn().as_ref(), Access::Write, |x| {
            x.exec_batch(BACKEND_SCHEMA)?;
            ensure_version(x, "jj-backend-schema", BACKEND_SCHEMA_VERSION)
        })?;
        Ok(Self {
            cas,
            root_commit_id: CommitId::from_bytes(&[0; COMMIT_ID_LENGTH]),
            root_change_id: ChangeId::from_bytes(&[0; CHANGE_ID_LENGTH]),
            empty_tree_id: TreeId::new(ArtifactHash::of(&codec::empty_tree_bytes()).0.to_vec()),
        })
    }

    /// Returns `store_path/../fossil.sqlite`: the database shared by all
    /// stores of the jj repo whose `store/` directory is `store_path`.
    pub fn db_path(store_path: &std::path::Path) -> std::path::PathBuf {
        store_path
            .parent()
            .unwrap_or(store_path)
            .join(Self::DB_FILE)
    }

    /// [`Self::init`] on the repo database next to `store_path`.
    #[cfg(feature = "rusqlite")]
    pub fn init_at(store_path: &std::path::Path) -> Result<Self, BackendInitError> {
        let conn = jj_fossil_cas::sql::RusqliteConn::shared(&Self::db_path(store_path))
            .map_err(|err| BackendInitError(err.into()))?;
        Self::init(conn)
    }

    /// [`Self::load`] from the repo database next to `store_path`.
    #[cfg(feature = "rusqlite")]
    pub fn load_at(store_path: &std::path::Path) -> Result<Self, BackendLoadError> {
        let conn = jj_fossil_cas::sql::RusqliteConn::shared(&Self::db_path(store_path))
            .map_err(|err| BackendLoadError(err.into()))?;
        Self::load(conn)
    }

    /// The underlying blob store.
    pub fn blob_store(&self) -> &BlobStore {
        &self.cas
    }

    fn get(&self, id: &impl ObjectId) -> BackendResult<Vec<u8>> {
        let hash = hash_of(id)?;
        self.cas
            .get(&hash)
            .map_err(|err| read_err(id, err))?
            .ok_or_else(|| not_found(id))
    }

    /// Stores an encoded structured object and indexes its kind.
    fn put_object(
        &self,
        x: &dyn SqlExec,
        kind: Kind,
        bytes: &[u8],
    ) -> Result<(i64, ArtifactHash), CasError> {
        let hash = ArtifactHash::of(bytes);
        let rid = self.cas.put_in(x, &hash, bytes)?;
        x.exec(
            "INSERT OR IGNORE INTO jj_object(rid, kind) VALUES (?, ?)",
            &[Param::Int(rid), Param::Int(kind.index_code())],
        )?;
        Ok((rid, hash))
    }

    fn write_object(&self, kind: Kind, bytes: &[u8]) -> Result<ArtifactHash, CasError> {
        with(self.cas.conn().as_ref(), Access::Write, |x| {
            self.put_object(x, kind, bytes).map(|(_, hash)| hash)
        })
    }
}

#[async_trait]
impl Backend for FossilBackend {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn commit_id_length(&self) -> usize {
        COMMIT_ID_LENGTH
    }

    fn change_id_length(&self) -> usize {
        CHANGE_ID_LENGTH
    }

    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }

    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn concurrency(&self) -> usize {
        1
    }

    async fn read_file(
        &self,
        path: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let hash = hash_of(id)?;
        // Stream the content so memory stays bounded for large files.
        let reader = self
            .cas
            .reader(&hash)
            .map_err(|err| BackendError::ReadFile {
                path: path.to_owned(),
                id: id.clone(),
                source: err.into(),
            })?
            .ok_or_else(|| not_found(id))?;
        Ok(Box::pin(AllowStdIo::new(reader)))
    }

    async fn write_file(
        &self,
        _path: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        // Hash and store incrementally so memory stays bounded for large
        // files.
        let mut writer = self.cas.writer();
        let mut buf = vec![0; 1 << 16];
        loop {
            let n = contents
                .read(&mut buf)
                .await
                .map_err(|err| write_err("file", err))?;
            if n == 0 {
                break;
            }
            writer
                .write(&buf[..n])
                .map_err(|err| write_err("file", err))?;
        }
        let (_, hash) = writer.finish().map_err(|err| write_err("file", err))?;
        Ok(FileId::new(hash.0.to_vec()))
    }

    async fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        let data = self.get(id)?;
        String::from_utf8(data).map_err(|err| BackendError::InvalidUtf8 {
            object_type: id.object_type(),
            hash: id.hex(),
            source: err.utf8_error(),
        })
    }

    async fn write_symlink(&self, _path: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        let (_, hash) = self
            .cas
            .put(target.as_bytes())
            .map_err(|err| write_err("symlink", err))?;
        Ok(SymlinkId::new(hash.0.to_vec()))
    }

    async fn read_copy(&self, id: &CopyId) -> BackendResult<CopyHistory> {
        let data = self.get(id)?;
        codec::decode_copy(&data).map_err(|err| read_err(id, err))
    }

    async fn write_copy(&self, copy: &CopyHistory) -> BackendResult<CopyId> {
        let parent_hashes = copy
            .parents
            .iter()
            .map(hash_of)
            .collect::<BackendResult<Vec<_>>>()?;
        let bytes = codec::encode_copy(copy);
        let hash = with_scope(self.cas.conn().as_ref(), Access::Write, |x| {
            // Parents must already be stored copy histories, so edges never
            // dangle and the related-copies walk never meets a missing node.
            let mut parent_rids = Vec::with_capacity(parent_hashes.len());
            for (parent, parent_hash) in copy.parents.iter().zip(&parent_hashes) {
                let data = self
                    .cas
                    .get_in(x, parent_hash)
                    .map_err(|err| read_err(parent, err))?
                    .ok_or_else(|| not_found(parent))?;
                codec::decode_copy(&data).map_err(|err| read_err(parent, err))?;
                let rid = jj_fossil_cas::store::rid_in(x, &parent_hash.to_uuid())
                    .map_err(|err| read_err(parent, err))?
                    .ok_or_else(|| not_found(parent))?;
                parent_rids.push(rid);
            }
            let (rid, hash) = self
                .put_object(x, Kind::Copy, &bytes)
                .map_err(|err| write_err("copy", err))?;
            for parent_rid in parent_rids {
                x.exec(
                    "INSERT OR IGNORE INTO jj_copy_edge(child, parent) VALUES (?, ?)",
                    &[Param::Int(rid), Param::Int(parent_rid)],
                )
                .map_err(|err| write_err("copy", err))?;
            }
            Ok(hash)
        })?;
        Ok(CopyId::new(hash.0.to_vec()))
    }

    async fn get_related_copies(&self, copy_id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        let hash = hash_of(copy_id)?;
        let rows = with_scope(self.cas.conn().as_ref(), Access::Read, |x| {
            let rid = jj_fossil_cas::store::rid_in(x, &hash.to_uuid())
                .map_err(|err| read_err(copy_id, err))?
                .ok_or_else(|| not_found(copy_id))?;
            // Ancestors of the copy, then every descendant of those. UNION
            // (not UNION ALL) terminates even on a corrupt cyclic graph.
            let rows = x
                .query(
                    "WITH RECURSIVE \
                       anc(rid) AS (SELECT ? UNION \
                         SELECT e.parent FROM jj_copy_edge e JOIN anc ON e.child = anc.rid), \
                       des(rid) AS (SELECT rid FROM anc UNION \
                         SELECT e.child FROM jj_copy_edge e JOIN des ON e.parent = des.rid) \
                     SELECT b.uuid FROM des JOIN blob b ON b.rid = des.rid",
                    &[Param::Int(rid)],
                )
                .map_err(|err| read_err(copy_id, err))?;
            Ok(rows)
        })?;
        let mut histories: HashMap<CopyId, CopyHistory> = HashMap::with_capacity(rows.len());
        for row in rows {
            let uuid = row.text(0).map_err(|err| read_err(copy_id, err))?;
            let id = ArtifactHash::from_uuid(uuid)
                .map(|h| CopyId::new(h.0.to_vec()))
                .ok_or_else(|| read_err(copy_id, format!("bad uuid {uuid}")))?;
            let history = self.read_copy(&id).await?;
            histories.insert(id, history);
        }
        let mut start: Vec<&CopyId> = histories.keys().collect();
        start.sort();
        let ordered = topo_order_reverse(
            start,
            |id| *id,
            |id| {
                histories[*id]
                    .parents
                    .iter()
                    .filter(|parent| histories.contains_key(*parent))
                    .collect::<Vec<_>>()
            },
            |id| to_other_err(format!("copy history graph has a cycle at {}", id.hex())),
        )?;
        Ok(ordered
            .into_iter()
            .map(|id| RelatedCopy {
                id: id.clone(),
                history: histories[id].clone(),
            })
            .collect())
    }

    async fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if *id == self.empty_tree_id {
            return Ok(Tree::default());
        }
        let data = self.get(id)?;
        codec::decode_tree(&data).map_err(|err| read_err(id, err))
    }

    async fn write_tree(&self, _path: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        let bytes = codec::encode_tree(tree);
        let hash = self
            .write_object(Kind::Tree, &bytes)
            .map_err(|err| write_err("tree", err))?;
        Ok(TreeId::new(hash.0.to_vec()))
    }

    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if *id == self.root_commit_id {
            return Ok(make_root_commit(
                self.root_change_id.clone(),
                self.empty_tree_id.clone(),
            ));
        }
        let data = self.get(id)?;
        codec::decode_commit(&data).map_err(|err| read_err(id, err))
    }

    async fn write_commit(
        &self,
        commit: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        if commit.secure_sig.is_some() {
            return Err(to_other_err(
                "Cannot write a commit with secure_sig already set; signing is done by the backend",
            ));
        }
        if commit.parents.is_empty() {
            return Err(to_other_err("Cannot write a commit with no parents"));
        }
        let unsigned = codec::encode_commit_unsigned(&commit);
        let bytes = match sign_with {
            Some(sign) => {
                let sig = sign(&unsigned).map_err(to_other_err)?;
                codec::attach_sig(unsigned, &sig)
            }
            None => unsigned,
        };
        let hash = self
            .write_object(Kind::Commit, &bytes)
            .map_err(|err| write_err("commit", err))?;
        // Return exactly what `read_commit` will return (normalised labels,
        // exact signed data).
        let stored = codec::decode_commit(&bytes).map_err(|err| write_err("commit", err))?;
        Ok((CommitId::new(hash.0.to_vec()), stored))
    }

    fn get_copy_records(
        &self,
        _paths: Option<&[RepoPathBuf]>,
        _root: &CommitId,
        _head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        Ok(stream::empty().boxed())
    }
}
