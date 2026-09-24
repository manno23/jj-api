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
//! *threads*. That does not mean no contention, though: a single-threaded
//! async executor can still interleave several logical tasks, and jj-lib's
//! own callers (`transaction.rs`, `op_heads_store.rs`,
//! `local_working_copy.rs`) hold a `FileLock` across `.await` points. If two
//! such tasks are polled concurrently, the second one really can observe the
//! lock "already held" with no bug involved.
//!
//! [`FileLock::lock`] therefore cannot honestly claim to block the way the
//! unix/windows versions do. A synchronous spin-loop would not be blocking
//! in any useful sense here: with only one execution context and no
//! scheduler to preempt it, spinning would never yield back to let the
//! task that holds the lock make progress and release it, hanging the
//! *whole* wasm instance forever rather than the one task. Reporting
//! contention as an immediate error is the safer of the two bad options.
//! Real waiting would need `lock` to become an `async fn` that registers a
//! waker and is polled again on release, which is a bigger, cross-platform
//! change to a type today's call sites use synchronously everywhere; it
//! isn't done here.
//!
//! In other words, this lock is exactly as strong as the "must not run on
//! wasm yet" rule already documented for its callers: `simple_op_heads_store`,
//! `simple_workspace_store`, `stacked_table` and `local_working_copy` are the
//! stores a SQL-backed deployment replaces with `jj-fossil-stores` and
//! (eventually) `SqlWorkingCopy`, whose own concurrency comes from
//! `SqlConn::scope`, not from this type, and which never hold this lock
//! across an await at all. This lock exists only so the crate compiles as
//! one unit and so jj-lib's own tests of these code paths still pass; it is
//! not a green light to run them concurrently on wasm.

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
    ///
    /// Unlike the unix/windows versions, this does not block: see the
    /// module docs for why a spin-loop would hang the wasm instance
    /// instead. Contention (from a re-entrant bug, or from two async tasks
    /// that both hold this lock across an `.await`) is reported as an
    /// error rather than silently deadlocking.
    pub fn lock(path: PathBuf) -> Result<Self, FileLockError> {
        Self::try_lock(path.clone())?.ok_or_else(|| FileLockError {
            message: "Contended: already locked elsewhere in this wasm instance (blocking is not \
                      supported here; see lock::wasm's module docs)",
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
