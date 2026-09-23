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

//! A Fossil-style content-addressed blob store on SQLite.
//!
//! Blobs live in a single `blob` table keyed by the lowercase-hex SHA3-256 of
//! their raw bytes, as in Fossil. This crate borrows Fossil's storage idea
//! only; it makes no claim of Fossil-compatible history.
//!
//! All SQL goes through [`sql::SqlConn`], the common denominator of rusqlite
//! and a Cloudflare Durable Object's `ctx.storage.sql`, so the same store runs
//! natively and in wasm.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod codec;
pub mod hash;
pub mod schema;
pub mod sql;
pub mod store;

pub use crate::hash::ArtifactHash;
pub use crate::store::BlobStore;
pub use crate::store::CasError;
pub use crate::store::CasResult;
pub use crate::store::Rid;
