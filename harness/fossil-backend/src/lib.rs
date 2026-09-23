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

//! A jj commit backend over the Fossil-style SQLite blob store.
//!
//! This is a *Fossil-compatible CAS*, not Fossil-compatible history: every
//! jj id is the SHA3-256 `uuid` of a blob, and file contents are stored
//! verbatim like Fossil file artifacts, but trees, commits and copies use
//! jj's own canonical encoding ([`codec`]). The crate depends only on
//! `jj-core`, so it builds for `wasm32-unknown-unknown` without the
//! `rusqlite` feature.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

mod backend;
pub mod codec;

pub use crate::backend::BACKEND_SCHEMA;
pub use crate::backend::BACKEND_SCHEMA_VERSION;
pub use crate::backend::CHANGE_ID_LENGTH;
pub use crate::backend::COMMIT_ID_LENGTH;
pub use crate::backend::FossilBackend;
