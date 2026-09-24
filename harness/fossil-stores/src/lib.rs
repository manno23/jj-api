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

//! The rest of a jj repo in the Fossil-style SQLite database.
//!
//! Together with [`jj_fossil_backend::FossilBackend`], these stores keep every
//! durable piece of a repo in one `fossil.sqlite` file:
//!
//! | jj store | here |
//! |----------|------|
//! | `Backend` | [`FossilBackend`] |
//! | `OpStore` | [`SqlOpStore`] |
//! | `OpHeadsStore` | [`SqlOpHeadsStore`] |
//! | `WorkspaceStore` | [`SqlWorkspaceStore`] |
//! | `IndexStore` | jj's default on-disk index, a cache rebuilt from the above |
//!
//! All stores of one repo share a single connection
//! ([`RusqliteConn::shared`]), so their scopes are serialized the way they
//! are on a Durable Object.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

mod op_heads_store;
mod op_store;
mod workspace_store;

use std::path::Path;
use std::sync::Arc;

pub use jj_fossil_backend::FossilBackend;
use jj_fossil_cas::sql::RusqliteConn;
use jj_fossil_cas::sql::SqlConn;
use jj_lib::backend::BackendInitError;
use jj_lib::backend::BackendLoadError;
use jj_lib::default_backend_factories::default_backend_factories;
use jj_lib::default_backend_factories::default_working_copy_factory;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::repo::RepoInitError;
use jj_lib::repo::StoreFactories;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::workspace::Workspace;
use jj_lib::workspace::WorkspaceInitError;

pub use crate::op_heads_store::OP_HEADS_SCHEMA;
pub use crate::op_heads_store::SqlOpHeadsStore;
pub use crate::op_store::OP_STORE_SCHEMA;
pub use crate::op_store::SqlOpStore;
pub use crate::workspace_store::SqlWorkspaceStore;
pub use crate::workspace_store::WORKSPACE_SCHEMA;

/// The shared database of the repo that owns the store directory
/// `store_dir` (any of `.jj/repo/{store,op_store,op_heads,…}`).
pub fn repo_conn(store_dir: &Path) -> Result<Arc<dyn SqlConn>, jj_fossil_cas::sql::SqlError> {
    Ok(RusqliteConn::shared(&FossilBackend::db_path(store_dir))?)
}

fn repo_path_of(store_dir: &Path) -> &Path {
    store_dir.parent().unwrap_or(store_dir)
}

fn init_err(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> BackendInitError {
    BackendInitError(err.into())
}

fn load_err(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> BackendLoadError {
    BackendLoadError(err.into())
}

/// Factories for the stores in this crate only, keyed by their type names.
pub fn fossil_store_factories() -> StoreFactories {
    let mut factories = StoreFactories::empty();
    factories.add_backend(
        FossilBackend::NAME,
        Box::new(|_settings, store_path| Ok(Box::new(FossilBackend::load_at(store_path)?))),
    );
    factories.add_op_store(
        SqlOpStore::NAME,
        Box::new(|_settings, store_path, root_data| {
            let conn = repo_conn(store_path).map_err(load_err)?;
            Ok(Box::new(SqlOpStore::load(conn, root_data)?))
        }),
    );
    factories.add_op_heads_store(
        SqlOpHeadsStore::NAME,
        Box::new(|_settings, store_path| {
            let conn = repo_conn(store_path).map_err(load_err)?;
            Ok(Box::new(SqlOpHeadsStore::load(conn)?))
        }),
    );
    factories.add_workspace_store(
        SqlWorkspaceStore::NAME,
        Box::new(|_settings, store_path| {
            let conn = repo_conn(store_path).map_err(load_err)?;
            Ok(Box::new(SqlWorkspaceStore::load(
                conn,
                repo_path_of(store_path),
            )?))
        }),
    );
    factories
}

/// jj's default factories plus [`fossil_store_factories`], for loading any
/// repo, including ones that use this crate's stores.
pub fn store_factories() -> StoreFactories {
    let mut factories = default_backend_factories();
    factories.merge(fossil_store_factories());
    factories
}

/// Initializes a repo at `repo_path` (an empty `.jj/repo` directory) whose
/// durable state lives entirely in `repo_path/fossil.sqlite`.
pub async fn init_repo(
    settings: &UserSettings,
    repo_path: &Path,
    signer: Signer,
) -> Result<Arc<ReadonlyRepo>, RepoInitError> {
    ReadonlyRepo::init(
        settings,
        repo_path,
        &|_settings, store_path| Ok(Box::new(FossilBackend::init_at(store_path)?)),
        signer,
        &|_settings, store_path| {
            let conn = repo_conn(store_path).map_err(init_err)?;
            Ok(Box::new(SqlWorkspaceStore::init(
                conn,
                repo_path_of(store_path),
            )?))
        },
        &|_settings, store_path, root_data| {
            let conn = repo_conn(store_path).map_err(init_err)?;
            Ok(Box::new(SqlOpStore::init(conn, root_data)?))
        },
        &|_settings, store_path, root_op_id| {
            let conn = repo_conn(store_path).map_err(init_err)?;
            Ok(Box::new(SqlOpHeadsStore::init(conn, root_op_id)?))
        },
        ReadonlyRepo::default_index_store_initializer(),
        ReadonlyRepo::default_submodule_store_initializer(),
    )
    .await
}

/// Initializes a workspace at `workspace_root` with a repo created as by
/// [`init_repo`] and jj's local working copy.
pub async fn init_workspace(
    settings: &UserSettings,
    workspace_root: &Path,
    signer: Signer,
) -> Result<(Workspace, Arc<ReadonlyRepo>), WorkspaceInitError> {
    Workspace::init_with_factories(
        settings,
        workspace_root,
        &|_settings, store_path| Ok(Box::new(FossilBackend::init_at(store_path)?)),
        signer,
        &|_settings, store_path| {
            let conn = repo_conn(store_path).map_err(init_err)?;
            Ok(Box::new(SqlWorkspaceStore::init(
                conn,
                repo_path_of(store_path),
            )?))
        },
        &|_settings, store_path, root_data| {
            let conn = repo_conn(store_path).map_err(init_err)?;
            Ok(Box::new(SqlOpStore::init(conn, root_data)?))
        },
        &|_settings, store_path, root_op_id| {
            let conn = repo_conn(store_path).map_err(init_err)?;
            Ok(Box::new(SqlOpHeadsStore::init(conn, root_op_id)?))
        },
        ReadonlyRepo::default_index_store_initializer(),
        ReadonlyRepo::default_submodule_store_initializer(),
        &*default_working_copy_factory(),
        WorkspaceName::DEFAULT.to_owned(),
    )
    .await
}
