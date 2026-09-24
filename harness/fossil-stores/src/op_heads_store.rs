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

//! [`SqlOpHeadsStore`]: the operation heads, the repo's only mutable pointer.

use std::sync::Arc;

use async_trait::async_trait;
use jj_fossil_cas::schema::ensure_version;
use jj_fossil_cas::sql::Access;
use jj_fossil_cas::sql::Param;
use jj_fossil_cas::sql::SqlConn;
use jj_fossil_cas::sql::SqlError;
use jj_fossil_cas::sql::with;
use jj_lib::backend::BackendInitError;
use jj_lib::backend::BackendLoadError;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_heads_store::OpHeadsStore;
use jj_lib::op_heads_store::OpHeadsStoreError;
use jj_lib::op_heads_store::OpHeadsStoreLock;
use jj_lib::op_store::OperationId;

/// Table owned by the op-heads store.
pub const OP_HEADS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS jj_op_head(id BLOB PRIMARY KEY) WITHOUT ROWID;
";

/// Schema version of [`OP_HEADS_SCHEMA`].
pub const OP_HEADS_SCHEMA_VERSION: i64 = 1;

/// An [`OpHeadsStore`] backed by one table. Each update is a single write
/// transaction, so heads are never observed half-updated.
#[derive(Debug)]
pub struct SqlOpHeadsStore {
    conn: Arc<dyn SqlConn>,
}

struct NoLock;

impl OpHeadsStoreLock for NoLock {}

impl SqlOpHeadsStore {
    /// The name recorded in `.jj/repo/op_heads/type`.
    pub const NAME: &str = "fossil_op_heads_store";

    /// Creates the table with `root_operation_id` as the only head.
    pub fn init(
        conn: Arc<dyn SqlConn>,
        root_operation_id: &OperationId,
    ) -> Result<Self, BackendInitError> {
        let store = Self::open(conn).map_err(|err| BackendInitError(err.into()))?;
        with(store.conn.as_ref(), Access::Write, |x| {
            x.exec(
                "INSERT OR IGNORE INTO jj_op_head(id) VALUES (?)",
                &[Param::Blob(root_operation_id.as_bytes())],
            )
        })
        .map_err(|err| BackendInitError(err.into()))?;
        Ok(store)
    }

    /// Loads a store created by [`Self::init`].
    pub fn load(conn: Arc<dyn SqlConn>) -> Result<Self, BackendLoadError> {
        Self::open(conn).map_err(|err| BackendLoadError(err.into()))
    }

    fn open(conn: Arc<dyn SqlConn>) -> Result<Self, SqlError> {
        with(conn.as_ref(), Access::Write, |x| {
            x.exec_batch(OP_HEADS_SCHEMA)?;
            ensure_version(x, "jj-op-heads-schema", OP_HEADS_SCHEMA_VERSION)
        })?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl OpHeadsStore for SqlOpHeadsStore {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn update_op_heads(
        &self,
        old_ids: &[OperationId],
        new_id: &OperationId,
    ) -> Result<(), OpHeadsStoreError> {
        with(self.conn.as_ref(), Access::Write, |x| {
            x.exec(
                "INSERT OR IGNORE INTO jj_op_head(id) VALUES (?)",
                &[Param::Blob(new_id.as_bytes())],
            )?;
            for old_id in old_ids.iter().filter(|id| *id != new_id) {
                x.exec(
                    "DELETE FROM jj_op_head WHERE id = ?",
                    &[Param::Blob(old_id.as_bytes())],
                )?;
            }
            Ok::<_, SqlError>(())
        })
        .map_err(|err| OpHeadsStoreError::Write {
            new_op_id: new_id.clone(),
            source: err.into(),
        })
    }

    async fn get_op_heads(&self) -> Result<Vec<OperationId>, OpHeadsStoreError> {
        let rows = with(self.conn.as_ref(), Access::Read, |x| {
            x.query("SELECT id FROM jj_op_head ORDER BY id", &[])
        })
        .map_err(|err| OpHeadsStoreError::Read(err.into()))?;
        let heads = rows
            .iter()
            .map(|row| row.blob(0).map(OperationId::from_bytes))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| OpHeadsStoreError::Read(err.into()))?;
        if heads.is_empty() {
            return Err(OpHeadsStoreError::Read(
                "no operation heads recorded".into(),
            ));
        }
        Ok(heads)
    }

    /// Scopes are already serialised per database, so no extra lock is taken.
    async fn lock(&self) -> Result<Box<dyn OpHeadsStoreLock + '_>, OpHeadsStoreError> {
        Ok(Box::new(NoLock))
    }
}
