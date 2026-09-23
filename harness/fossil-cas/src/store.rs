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

//! The content-addressed blob store.

use std::sync::Arc;

use thiserror::Error;

use crate::codec;
use crate::codec::Encoding;
use crate::hash::ArtifactHash;
use crate::schema::ensure_cas_schema;
use crate::sql::Access;
use crate::sql::Param;
use crate::sql::SqlConn;
use crate::sql::SqlError;
use crate::sql::SqlExec;
use crate::sql::with;

/// Row id of a blob. Always below 2^53, so it survives a JS double.
pub type Rid = i64;

/// Errors from the blob store.
#[derive(Debug, Error)]
pub enum CasError {
    /// The SQL layer failed.
    #[error(transparent)]
    Sql(#[from] SqlError),
    /// Stored data is inconsistent.
    #[error("corrupt blob {uuid}: {message}")]
    Corrupt {
        /// The blob's uuid.
        uuid: String,
        /// What is wrong.
        message: String,
    },
}

/// Result of blob-store operations.
pub type CasResult<T> = Result<T, CasError>;

/// Room left in each row for the non-content columns.
const ROW_OVERHEAD: usize = 1024;
/// Largest chunk written when a blob exceeds the value limit.
const MAX_CHUNK_LEN: usize = 1 << 20;

/// A content-addressed store of immutable blobs.
///
/// `put` is idempotent: equal bytes always map to the same `uuid` and `rid`.
/// A blob too large for one SQL value (only possible on a Durable Object) is
/// split across `jj_chunk` rows with `blob.content` left `NULL`; this is
/// invisible to callers.
#[derive(Clone, Debug)]
pub struct BlobStore {
    conn: Arc<dyn SqlConn>,
}

impl BlobStore {
    /// Opens the store, creating its tables if needed.
    pub fn open(conn: Arc<dyn SqlConn>) -> CasResult<Self> {
        with(conn.as_ref(), Access::Write, |x| ensure_cas_schema(x))?;
        Ok(Self { conn })
    }

    /// The underlying connection, for layers that keep their own tables in
    /// the same database.
    pub fn conn(&self) -> &Arc<dyn SqlConn> {
        &self.conn
    }

    /// Stores `raw` and returns its row id and hash.
    pub fn put(&self, raw: &[u8]) -> CasResult<(Rid, ArtifactHash)> {
        let hash = ArtifactHash::of(raw);
        let rid = with(self.conn.as_ref(), Access::Write, |x| {
            self.put_in(x, &hash, raw)
        })?;
        Ok((rid, hash))
    }

    /// Stores `raw`, whose hash the caller has already computed, inside the
    /// caller's write scope.
    pub fn put_in(&self, x: &dyn SqlExec, hash: &ArtifactHash, raw: &[u8]) -> CasResult<Rid> {
        debug_assert_eq!(*hash, ArtifactHash::of(raw));
        let uuid = hash.to_uuid();
        if let Some(rid) = rid_in(x, &uuid)? {
            return Ok(rid);
        }
        let (encoding, stored) = codec::encode(raw);
        let size = i64::try_from(raw.len()).expect("blob size fits in i64");
        let max_inline = self.conn.max_value_len().saturating_sub(ROW_OVERHEAD);
        let inline = stored.len() <= max_inline;
        let content = if inline {
            Param::Blob(&stored)
        } else {
            Param::Null
        };
        let row = x
            .query_row(
                "INSERT INTO blob(uuid, size, enc, content) VALUES (?, ?, ?, ?) RETURNING rid",
                &[
                    Param::Text(&uuid),
                    Param::Int(size),
                    Param::Int(encoding as i64),
                    content,
                ],
            )?
            .ok_or_else(|| SqlError("INSERT … RETURNING returned no row".to_owned()))?;
        let rid = row.int(0)?;
        if !inline {
            let chunk_len = max_inline.clamp(1, MAX_CHUNK_LEN);
            for (seq, chunk) in stored.chunks(chunk_len).enumerate() {
                x.exec(
                    "INSERT INTO jj_chunk(rid, seq, data) VALUES (?, ?, ?)",
                    &[
                        Param::Int(rid),
                        Param::Int(i64::try_from(seq).unwrap()),
                        Param::Blob(chunk),
                    ],
                )?;
            }
        }
        Ok(rid)
    }

    /// Reads a blob's raw bytes, or `None` if it isn't stored.
    pub fn get(&self, hash: &ArtifactHash) -> CasResult<Option<Vec<u8>>> {
        with(self.conn.as_ref(), Access::Read, |x| self.get_in(x, hash))
    }

    /// Like [`Self::get`], inside the caller's scope.
    pub fn get_in(&self, x: &dyn SqlExec, hash: &ArtifactHash) -> CasResult<Option<Vec<u8>>> {
        let uuid = hash.to_uuid();
        let Some(row) = x.query_row(
            "SELECT rid, size, enc, content FROM blob WHERE uuid = ?",
            &[Param::Text(&uuid)],
        )?
        else {
            return Ok(None);
        };
        let corrupt = |message: String| CasError::Corrupt {
            uuid: uuid.clone(),
            message,
        };
        let rid = row.int(0)?;
        let size = row.int(1)?;
        let encoding = Encoding::from_i64(row.int(2)?)
            .ok_or_else(|| corrupt(format!("unknown encoding {:?}", row.0[2])))?;
        let stored = if row.is_null(3)? {
            let chunks = x.query(
                "SELECT data FROM jj_chunk WHERE rid = ? ORDER BY seq",
                &[Param::Int(rid)],
            )?;
            if chunks.is_empty() {
                return Err(corrupt(
                    "content is NULL and there are no chunks".to_owned(),
                ));
            }
            let mut stored = Vec::new();
            for chunk in &chunks {
                stored.extend_from_slice(chunk.blob(0)?);
            }
            stored
        } else {
            row.blob(3)?.to_vec()
        };
        let raw = codec::decode(encoding, stored).map_err(corrupt)?;
        if i64::try_from(raw.len()).ok() != Some(size) {
            return Err(corrupt(format!(
                "decoded {} bytes, size column says {size}",
                raw.len()
            )));
        }
        Ok(Some(raw))
    }

    /// Returns the row id of a stored blob.
    pub fn rid(&self, hash: &ArtifactHash) -> CasResult<Option<Rid>> {
        with(self.conn.as_ref(), Access::Read, |x| {
            rid_in(x, &hash.to_uuid())
        })
    }

    /// Whether a blob is stored.
    pub fn contains(&self, hash: &ArtifactHash) -> CasResult<bool> {
        Ok(self.rid(hash)?.is_some())
    }
}

/// Looks up the row id for `uuid`.
pub fn rid_in(x: &dyn SqlExec, uuid: &str) -> CasResult<Option<Rid>> {
    Ok(
        x.query_row("SELECT rid FROM blob WHERE uuid = ?", &[Param::Text(uuid)])?
            .map(|row| row.int(0))
            .transpose()?,
    )
}

#[cfg(all(test, feature = "rusqlite"))]
mod tests {
    use super::*;
    use crate::sql::RusqliteConn;

    fn store_with(conn: RusqliteConn) -> BlobStore {
        BlobStore::open(Arc::new(conn)).unwrap()
    }

    fn store() -> BlobStore {
        store_with(RusqliteConn::open_in_memory().unwrap())
    }

    #[test]
    fn put_get_round_trip() {
        let s = store();
        let (rid, hash) = s.put(b"hello").unwrap();
        assert_eq!(hash, ArtifactHash::of(b"hello"));
        assert_eq!(s.get(&hash).unwrap().as_deref(), Some(&b"hello"[..]));
        assert_eq!(s.rid(&hash).unwrap(), Some(rid));
        assert!(s.contains(&hash).unwrap());
        assert_eq!(s.get(&ArtifactHash::of(b"nope")).unwrap(), None);
    }

    #[test]
    fn put_is_idempotent() {
        let s = store();
        let a = s.put(b"same").unwrap();
        let b = s.put(b"same").unwrap();
        assert_eq!(a, b);
        let n = with(s.conn().as_ref(), Access::Read, |x| {
            x.query_row("SELECT count(*) FROM blob", &[])?
                .unwrap()
                .int(0)
        })
        .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn empty_blob() {
        let s = store();
        let (_, hash) = s.put(b"").unwrap();
        assert_eq!(s.get(&hash).unwrap(), Some(vec![]));
    }

    #[test]
    fn uuid_is_sha3_of_raw_bytes_even_when_compressed() {
        let s = store();
        let raw = "compressible ".repeat(1000);
        let (_, hash) = s.put(raw.as_bytes()).unwrap();
        let (uuid, enc) = with(s.conn().as_ref(), Access::Read, |x| {
            let row = x.query_row("SELECT uuid, enc FROM blob", &[])?.unwrap();
            Ok::<_, CasError>((row.text(0)?.to_owned(), row.int(1)?))
        })
        .unwrap();
        assert_eq!(uuid, ArtifactHash::of(raw.as_bytes()).to_uuid());
        assert_eq!(uuid, hash.to_uuid());
        assert_eq!(enc, Encoding::Zlib as i64);
        assert_eq!(s.get(&hash).unwrap().unwrap(), raw.as_bytes());
    }

    #[test]
    fn chunks_blobs_over_the_value_limit() {
        let s = store_with(
            RusqliteConn::open_in_memory()
                .unwrap()
                .with_max_value_len(ROW_OVERHEAD + 100),
        );
        // Incompressible, so it is stored raw and needs several chunks.
        let mut state: u32 = 0x9e37_79b9;
        let raw: Vec<u8> = (0..1000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect();
        let (rid, hash) = s.put(&raw).unwrap();
        let (null_content, nchunks) = with(s.conn().as_ref(), Access::Read, |x| {
            let row = x
                .query_row("SELECT content FROM blob WHERE rid = ?", &[Param::Int(rid)])?
                .unwrap();
            let n = x
                .query_row(
                    "SELECT count(*) FROM jj_chunk WHERE rid = ?",
                    &[Param::Int(rid)],
                )?
                .unwrap()
                .int(0)?;
            Ok::<_, CasError>((row.is_null(0)?, n))
        })
        .unwrap();
        assert!(null_content);
        assert_eq!(nchunks, 10);
        assert_eq!(s.get(&hash).unwrap().unwrap(), raw);
        // Small blobs stay inline.
        let (_, small) = s.put(b"small").unwrap();
        assert_eq!(s.get(&small).unwrap().unwrap(), b"small");
    }

    #[test]
    fn reopen_is_idempotent_and_keeps_data() {
        let dir = std::env::temp_dir().join(format!("jj-fossil-cas-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("reopen.sqlite");
        std::fs::remove_file(&path).ok();
        let hash = {
            let s = store_with(RusqliteConn::open(&path).unwrap());
            s.put(b"persisted").unwrap().1
        };
        let s = store_with(RusqliteConn::open(&path).unwrap());
        assert_eq!(s.get(&hash).unwrap().unwrap(), b"persisted");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
