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

//! [`SqlWorkspaceStore`]: workspace name → location.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use jj_fossil_cas::schema::ensure_version;
use jj_fossil_cas::sql::Access;
use jj_fossil_cas::sql::Param;
use jj_fossil_cas::sql::SqlConn;
use jj_fossil_cas::sql::SqlError;
use jj_fossil_cas::sql::with;
use jj_lib::backend::BackendInitError;
use jj_lib::backend::BackendLoadError;
use jj_lib::file_util::path_from_bytes;
use jj_lib::file_util::path_to_bytes;
use jj_lib::file_util::relative_path;
use jj_lib::file_util::slash_path;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::workspace_store::WorkspaceStore;
use jj_lib::workspace_store::WorkspaceStoreError;

/// Table owned by the workspace store.
pub const WORKSPACE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS jj_workspace(
  name TEXT PRIMARY KEY NOT NULL,
  path BLOB NOT NULL
) WITHOUT ROWID;
";

/// Schema version of [`WORKSPACE_SCHEMA`].
pub const WORKSPACE_SCHEMA_VERSION: i64 = 1;

/// A [`WorkspaceStore`] backed by one table. Paths are stored relative to the
/// repo directory when possible, like jj's simple workspace store.
#[derive(Debug)]
pub struct SqlWorkspaceStore {
    conn: Arc<dyn SqlConn>,
    repo_path: PathBuf,
}

fn store_err(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> WorkspaceStoreError {
    WorkspaceStoreError::Other(err.into())
}

impl SqlWorkspaceStore {
    /// The name recorded in `.jj/repo/workspace_store/type`.
    pub const NAME: &str = "fossil_workspace_store";

    /// Creates the table. `repo_path` is the `.jj/repo` directory.
    pub fn init(conn: Arc<dyn SqlConn>, repo_path: &Path) -> Result<Self, BackendInitError> {
        Self::open(conn, repo_path).map_err(|err| BackendInitError(err.into()))
    }

    /// Loads a store created by [`Self::init`].
    pub fn load(conn: Arc<dyn SqlConn>, repo_path: &Path) -> Result<Self, BackendLoadError> {
        Self::open(conn, repo_path).map_err(|err| BackendLoadError(err.into()))
    }

    fn open(conn: Arc<dyn SqlConn>, repo_path: &Path) -> Result<Self, SqlError> {
        with(conn.as_ref(), Access::Write, |x| {
            x.exec_batch(WORKSPACE_SCHEMA)?;
            ensure_version(x, "jj-workspace-schema", WORKSPACE_SCHEMA_VERSION)
        })?;
        Ok(Self {
            conn,
            repo_path: repo_path.to_owned(),
        })
    }
}

impl WorkspaceStore for SqlWorkspaceStore {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn add(&self, workspace_name: &WorkspaceName, path: &Path) -> Result<(), WorkspaceStoreError> {
        let stored = relative_path(&self.repo_path, path);
        let stored = if stored.is_relative() {
            slash_path(&stored).into_owned()
        } else {
            stored
        };
        let bytes = path_to_bytes(&stored).map_err(store_err)?;
        with(self.conn.as_ref(), Access::Write, |x| {
            x.exec(
                "INSERT INTO jj_workspace(name, path) VALUES (?, ?) ON CONFLICT(name) DO UPDATE \
                 SET path = excluded.path",
                &[Param::Text(workspace_name.as_str()), Param::Blob(bytes)],
            )
        })
        .map_err(store_err)?;
        Ok(())
    }

    fn forget(&self, workspace_names: &[&WorkspaceName]) -> Result<(), WorkspaceStoreError> {
        with(self.conn.as_ref(), Access::Write, |x| {
            for name in workspace_names {
                x.exec(
                    "DELETE FROM jj_workspace WHERE name = ?",
                    &[Param::Text(name.as_str())],
                )?;
            }
            Ok::<_, SqlError>(())
        })
        .map_err(store_err)
    }

    fn rename(
        &self,
        old_name: &WorkspaceName,
        new_name: &WorkspaceName,
    ) -> Result<(), WorkspaceStoreError> {
        with(self.conn.as_ref(), Access::Write, |x| {
            x.exec(
                "UPDATE jj_workspace SET name = ? WHERE name = ?",
                &[
                    Param::Text(new_name.as_str()),
                    Param::Text(old_name.as_str()),
                ],
            )
        })
        .map_err(store_err)?;
        Ok(())
    }

    fn get_workspace_path(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<PathBuf>, WorkspaceStoreError> {
        let row = with(self.conn.as_ref(), Access::Read, |x| {
            x.query_row(
                "SELECT path FROM jj_workspace WHERE name = ?",
                &[Param::Text(workspace_name.as_str())],
            )
        })
        .map_err(store_err)?;
        row.map(|row| {
            let bytes = row.blob(0).map_err(store_err)?;
            Ok(path_from_bytes(bytes).map_err(store_err)?.to_path_buf())
        })
        .transpose()
    }
}
