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

//! A `wasm32-unknown-unknown` stand-in for the unix/windows `FileLock`.
//!
//! There is no filesystem to lock, and `wasm32-unknown-unknown` (built
//! without the `atomics` target feature, as this workspace does) has no real
//! threads: everything in one wasm instance runs on a single execution
//! context. So no other thread can ever be racing to take this lock; the
//! only way to observe it "already held" is a bug that re-enters it from the
//! same call chain.
//!
//! The stores that use `FileLock` on disk (`simple_op_heads_store`,
//! `simple_workspace_store`, `stacked_table`, `local_working_copy`) are
//! exactly the ones a SQL-backed deployment replaces with
//! `jj-fossil-stores` and (eventually) `SqlWorkingCopy`, whose own
//! concurrency comes from `SqlConn::scope`, not from this type. This lock
//! exists only so the crate compiles as one unit and so jj-lib's own tests
//! of these code paths still pass.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::OnceLock;

use super::FileLockError;

fn locked_paths() -> &'static Mutex<HashSet<PathBuf>> {
    static LOCKED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    LOCKED.get_or_init(Default::default)
}

/// Holds `path`'s slot in [`locked_paths`] for as long as it's alive. Only a
/// `PathBuf`, so it's trivially `Send`, unlike a held `MutexGuard` would be.
pub struct FileLock {
    path: PathBuf,
}

impl FileLock {
    /// Acquires the lock for `path`.
    pub fn lock(path: PathBuf) -> Result<Self, FileLockError> {
        Self::try_lock(path.clone())?.ok_or_else(|| FileLockError {
            message: "Deadlock: already locked by this execution context",
            path,
            err: std::io::ErrorKind::WouldBlock.into(),
        })
    }

    /// Tries to acquire the lock for `path` without blocking.
    pub fn try_lock(path: PathBuf) -> Result<Option<Self>, FileLockError> {
        let mut locked = locked_paths().lock().unwrap_or_else(|err| err.into_inner());
        if locked.insert(path.clone()) {
            Ok(Some(Self { path }))
        } else {
            Ok(None)
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let mut locked = locked_paths().lock().unwrap_or_else(|err| err.into_inner());
        locked.remove(&self.path);
    }
}
