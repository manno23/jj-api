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

//! [`SqlOpStore`]: operations and views as blobs in the shared database.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use jj_fossil_cas::ArtifactHash;
use jj_fossil_cas::BlobStore;
use jj_fossil_cas::CasError;
use jj_fossil_cas::schema::ensure_version;
use jj_fossil_cas::sql::Access;
use jj_fossil_cas::sql::Param;
use jj_fossil_cas::sql::SqlConn;
use jj_fossil_cas::sql::with;
use jj_lib::backend::BackendInitError;
use jj_lib::backend::BackendLoadError;
use jj_lib::content_hash::blake2b_hash;
use jj_lib::object_id::HexPrefix;
use jj_lib::object_id::ObjectId;
use jj_lib::object_id::PrefixResolution;
use jj_lib::op_store::OpStore;
use jj_lib::op_store::OpStoreError;
use jj_lib::op_store::OpStoreResult;
use jj_lib::op_store::Operation;
use jj_lib::op_store::OperationId;
use jj_lib::op_store::RootOperationData;
use jj_lib::op_store::View;
use jj_lib::op_store::ViewId;
use jj_lib::simple_op_store;

/// Operation and view ids: BLAKE2b-512 of the struct's `ContentHash`, as in
/// jj's simple op store.
const ID_LENGTH: usize = 64;

/// Tables owned by the op store. The id → blob mapping is needed because op
/// ids hash the Rust struct, not the stored bytes.
pub const OP_STORE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS jj_view(
  id BLOB PRIMARY KEY,
  rid INTEGER NOT NULL REFERENCES blob
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS jj_operation(
  id BLOB PRIMARY KEY,
  hex TEXT UNIQUE NOT NULL,
  rid INTEGER NOT NULL REFERENCES blob
) WITHOUT ROWID;
";

/// Schema version of [`OP_STORE_SCHEMA`].
pub const OP_STORE_SCHEMA_VERSION: i64 = 1;

/// Header of stored views and operations: `\0jj`, kind, version. The body is
/// the simple op store's protobuf encoding.
const VIEW_HEADER: [u8; 5] = *b"\0jjV\x01";
const OPERATION_HEADER: [u8; 5] = *b"\0jjO\x01";

/// An [`OpStore`] keeping operations and views in the shared SQLite database.
#[derive(Debug)]
pub struct SqlOpStore {
    cas: BlobStore,
    root_data: RootOperationData,
    root_operation_id: OperationId,
    root_view_id: ViewId,
}

fn other(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> OpStoreError {
    OpStoreError::Other(err.into())
}

fn read_err(
    id: &impl ObjectId,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> OpStoreError {
    OpStoreError::ReadObject {
        object_type: id.object_type(),
        hash: id.hex(),
        source: err.into(),
    }
}

fn write_err(
    object_type: &'static str,
    err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> OpStoreError {
    OpStoreError::WriteObject {
        object_type,
        source: err.into(),
    }
}

impl SqlOpStore {
    /// The name recorded in `.jj/repo/op_store/type`.
    pub const NAME: &str = "fossil_op_store";

    /// Creates the op store's tables.
    pub fn init(
        conn: Arc<dyn SqlConn>,
        root_data: RootOperationData,
    ) -> Result<Self, BackendInitError> {
        Self::open(conn, root_data).map_err(|err| BackendInitError(err.into()))
    }

    /// Loads an op store created by [`Self::init`].
    pub fn load(
        conn: Arc<dyn SqlConn>,
        root_data: RootOperationData,
    ) -> Result<Self, BackendLoadError> {
        Self::open(conn, root_data).map_err(|err| BackendLoadError(err.into()))
    }

    fn open(conn: Arc<dyn SqlConn>, root_data: RootOperationData) -> Result<Self, CasError> {
        let cas = BlobStore::open(conn)?;
        with(cas.conn().as_ref(), Access::Write, |x| {
            x.exec_batch(OP_STORE_SCHEMA)?;
            ensure_version(x, "jj-op-store-schema", OP_STORE_SCHEMA_VERSION)
        })?;
        Ok(Self {
            cas,
            root_data,
            root_operation_id: OperationId::from_bytes(&[0; ID_LENGTH]),
            root_view_id: ViewId::from_bytes(&[0; ID_LENGTH]),
        })
    }

    /// Reads the body stored for `id` in `table`, checking its header.
    fn read_body(
        &self,
        table: &str,
        header: &[u8; 5],
        id: &impl ObjectId,
    ) -> OpStoreResult<Vec<u8>> {
        let sql =
            format!("SELECT b.uuid FROM {table} t JOIN blob b ON b.rid = t.rid WHERE t.id = ?");
        let uuid = with(self.cas.conn().as_ref(), Access::Read, |x| {
            x.query_row(&sql, &[Param::Blob(id.as_bytes())])?
                .map(|row| row.text(0).map(str::to_owned))
                .transpose()
        })
        .map_err(|err| read_err(id, err))?
        .ok_or_else(|| OpStoreError::ObjectNotFound {
            object_type: id.object_type(),
            hash: id.hex(),
            source: "no such object".into(),
        })?;
        let hash = ArtifactHash::from_uuid(&uuid).ok_or_else(|| read_err(id, "bad uuid"))?;
        let data = self
            .cas
            .get(&hash)
            .map_err(|err| read_err(id, err))?
            .ok_or_else(|| read_err(id, "blob missing"))?;
        let body = data
            .strip_prefix(header.as_slice())
            .ok_or_else(|| read_err(id, "bad header"))?;
        Ok(body.to_vec())
    }

    /// Stores `body` under `header` and maps `id` to it, in one transaction.
    fn write_body(
        &self,
        object_type: &'static str,
        header: &[u8; 5],
        body: &[u8],
        id: &[u8],
        index_sql: &str,
        extra: Option<&str>,
    ) -> OpStoreResult<()> {
        let mut data = header.to_vec();
        data.extend_from_slice(body);
        let hash = ArtifactHash::of(&data);
        with(self.cas.conn().as_ref(), Access::Write, |x| {
            let rid = self.cas.put_in(x, &hash, &data)?;
            let mut params = vec![Param::Blob(id)];
            if let Some(extra) = extra {
                params.push(Param::Text(extra));
            }
            params.push(Param::Int(rid));
            x.exec(index_sql, &params)?;
            Ok::<_, CasError>(())
        })
        .map_err(|err| write_err(object_type, err))
    }
}

#[async_trait]
impl OpStore for SqlOpStore {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn root_operation_id(&self) -> &OperationId {
        &self.root_operation_id
    }

    async fn read_view(&self, id: &ViewId) -> OpStoreResult<View> {
        if *id == self.root_view_id {
            return Ok(View::make_root(self.root_data.root_commit_id.clone()));
        }
        let body = self.read_body("jj_view", &VIEW_HEADER, id)?;
        simple_op_store::decode_view(&body).map_err(|err| read_err(id, err))
    }

    async fn write_view(&self, view: &View) -> OpStoreResult<ViewId> {
        let id = ViewId::new(blake2b_hash(view).to_vec());
        self.write_body(
            "view",
            &VIEW_HEADER,
            &simple_op_store::encode_view(view),
            id.as_bytes(),
            "INSERT OR IGNORE INTO jj_view(id, rid) VALUES (?, ?)",
            None,
        )?;
        Ok(id)
    }

    async fn read_operation(&self, id: &OperationId) -> OpStoreResult<Operation> {
        if *id == self.root_operation_id {
            return Ok(Operation::make_root(self.root_view_id.clone()));
        }
        let body = self.read_body("jj_operation", &OPERATION_HEADER, id)?;
        let mut operation =
            simple_op_store::decode_operation(&body).map_err(|err| read_err(id, err))?;
        if operation.parents.is_empty() {
            operation.parents.push(self.root_operation_id.clone());
        }
        Ok(operation)
    }

    async fn write_operation(&self, operation: &Operation) -> OpStoreResult<OperationId> {
        assert!(!operation.parents.is_empty());
        let id = OperationId::new(blake2b_hash(operation).to_vec());
        self.write_body(
            "operation",
            &OPERATION_HEADER,
            &simple_op_store::encode_operation(operation),
            id.as_bytes(),
            "INSERT OR IGNORE INTO jj_operation(id, hex, rid) VALUES (?, ?, ?)",
            Some(&id.hex()),
        )?;
        Ok(id)
    }

    async fn resolve_operation_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> OpStoreResult<PrefixResolution<OperationId>> {
        let hex = prefix.hex();
        // A range scan instead of LIKE: `g` sorts after every hex digit.
        let upper = format!("{hex}g");
        let rows = with(self.cas.conn().as_ref(), Access::Read, |x| {
            x.query(
                "SELECT id FROM jj_operation WHERE hex >= ? AND hex < ? ORDER BY hex LIMIT 2",
                &[Param::Text(&hex), Param::Text(&upper)],
            )
        })
        .map_err(other)?;
        let mut matches = Vec::with_capacity(3);
        if prefix.matches(&self.root_operation_id) {
            matches.push(self.root_operation_id.clone());
        }
        for row in rows {
            matches.push(OperationId::from_bytes(row.blob(0).map_err(other)?));
        }
        Ok(match matches.len() {
            0 => PrefixResolution::NoMatch,
            1 => PrefixResolution::SingleMatch(matches.pop().unwrap()),
            _ => PrefixResolution::AmbiguousMatch,
        })
    }

    /// Not implemented yet: unreachable operations are kept.
    async fn gc(&self, _head_ids: &[OperationId], _keep_newer: SystemTime) -> OpStoreResult<()> {
        Ok(())
    }
}
