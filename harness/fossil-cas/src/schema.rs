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

//! Tables owned by the blob store.
//!
//! `blob` keeps the shape of Fossil's table (`rid`, `uuid`, `size`,
//! `content`) plus an `enc` column; nothing else of Fossil's schema is copied.
//! Layers above add their own `jj_*` tables and record their schema versions
//! in `config`.

use crate::sql::Param;
use crate::sql::SqlExec;
use crate::sql::SqlResult;

/// DDL for the blob store. Idempotent.
pub const CAS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS blob(
  rid INTEGER PRIMARY KEY,
  uuid TEXT UNIQUE NOT NULL,
  size INTEGER NOT NULL,
  enc INTEGER NOT NULL DEFAULT 0,
  content BLOB,
  CHECK(length(uuid) >= 40 AND rid > 0 AND size >= 0)
);
CREATE TABLE IF NOT EXISTS jj_chunk(
  rid INTEGER NOT NULL REFERENCES blob,
  seq INTEGER NOT NULL,
  data BLOB NOT NULL,
  PRIMARY KEY(rid, seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS config(
  name TEXT PRIMARY KEY NOT NULL,
  value CLOB
) WITHOUT ROWID;
";

/// Schema version of the tables in [`CAS_SCHEMA`].
pub const CAS_SCHEMA_VERSION: i64 = 1;

/// Creates the blob-store tables and records their version.
pub fn ensure_cas_schema(x: &dyn SqlExec) -> SqlResult<()> {
    x.exec_batch(CAS_SCHEMA)?;
    ensure_version(x, "jj-cas-schema", CAS_SCHEMA_VERSION)
}

/// Records `version` under `name` in `config`, or checks that an existing
/// record matches.
pub fn ensure_version(x: &dyn SqlExec, name: &str, version: i64) -> SqlResult<()> {
    let version_text = version.to_string();
    match get_config(x, name)? {
        None => set_config(x, name, &version_text),
        Some(found) if found == version_text => Ok(()),
        Some(found) => Err(crate::sql::SqlError(format!(
            "{name}: database has version {found}, this build supports {version}"
        ))),
    }
}

/// Reads a `config` value.
pub fn get_config(x: &dyn SqlExec, name: &str) -> SqlResult<Option<String>> {
    x.query_row(
        "SELECT value FROM config WHERE name = ?",
        &[Param::Text(name)],
    )?
    .map(|row| row.text(0).map(str::to_owned))
    .transpose()
}

/// Writes a `config` value.
pub fn set_config(x: &dyn SqlExec, name: &str, value: &str) -> SqlResult<()> {
    x.exec(
        "INSERT INTO config(name, value) VALUES (?, ?) \
         ON CONFLICT(name) DO UPDATE SET value = excluded.value",
        &[Param::Text(name), Param::Text(value)],
    )?;
    Ok(())
}
