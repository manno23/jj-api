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

//! Streaming access to blobs with bounded memory.
//!
//! [`BlobReader`] pages chunk rows in one at a time and decompresses as it
//! goes; [`BlobWriter`] hashes incrementally and, once the content outgrows a
//! single SQL value, writes it out chunk by chunk. Either way the memory held
//! per blob is bounded by the connection's value limit (about 2 MB on a
//! Durable Object) rather than by the size of the blob.
//!
//! Locally the value limit is SQLite's (about 1 GB), so blobs are kept
//! inline and the database never contains chunk rows.

use std::io;
use std::io::Read;

use flate2::read::ZlibDecoder;
use sha3::Digest as _;
use sha3::Sha3_256;

use crate::codec::Encoding;
use crate::hash::ArtifactHash;
use crate::sql::Access;
use crate::sql::Param;
use crate::sql::SqlError;
use crate::sql::with;
use crate::store::BlobStore;
use crate::store::CasError;
use crate::store::CasResult;
use crate::store::Rid;
use crate::store::rid_in;

/// Reads one blob's raw bytes, fetching chunk rows lazily.
pub struct BlobReader {
    inner: Box<dyn Read + Send>,
    uuid: String,
    expected: u64,
    seen: u64,
}

/// Yields a chunked blob's stored bytes, one `jj_chunk` row at a time.
struct ChunkPager {
    store: BlobStore,
    rid: Rid,
    next_seq: i64,
    buf: Vec<u8>,
    pos: usize,
}

impl Read for ChunkPager {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos == self.buf.len() {
            let row = with(self.store.conn().as_ref(), Access::Read, |x| {
                x.query_row(
                    "SELECT data FROM jj_chunk WHERE rid = ? AND seq = ?",
                    &[Param::Int(self.rid), Param::Int(self.next_seq)],
                )
            })
            .map_err(io::Error::other)?;
            let Some(row) = row else {
                return Ok(0);
            };
            self.buf = row.blob(0).map_err(io::Error::other)?.to_vec();
            self.pos = 0;
            self.next_seq += 1;
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl BlobReader {
    pub(crate) fn open(store: &BlobStore, hash: &ArtifactHash) -> CasResult<Option<Self>> {
        let uuid = hash.to_uuid();
        let row = with(store.conn().as_ref(), Access::Read, |x| {
            x.query_row(
                "SELECT rid, size, enc, content FROM blob WHERE uuid = ?",
                &[Param::Text(&uuid)],
            )
        })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let corrupt = |message: String| CasError::Corrupt {
            uuid: uuid.clone(),
            message,
        };
        let rid = row.int(0)?;
        let size = u64::try_from(row.int(1)?).map_err(|_| corrupt("negative size".to_owned()))?;
        let encoding = Encoding::from_i64(row.int(2)?)
            .ok_or_else(|| corrupt(format!("unknown encoding {:?}", row.0[2])))?;
        let mut stored: Box<dyn Read + Send> = if row.is_null(3)? {
            Box::new(ChunkPager {
                store: store.clone(),
                rid,
                next_seq: 0,
                buf: vec![],
                pos: 0,
            })
        } else {
            Box::new(io::Cursor::new(row.blob(3)?.to_vec()))
        };
        let inner: Box<dyn Read + Send> = match encoding {
            Encoding::Raw => stored,
            Encoding::Zlib => {
                let mut header = [0; 4];
                stored
                    .read_exact(&mut header)
                    .map_err(|err| corrupt(format!("compressed header: {err}")))?;
                if u64::from(u32::from_be_bytes(header)) != size {
                    return Err(corrupt("compressed header disagrees with size".to_owned()));
                }
                Box::new(ZlibDecoder::new(stored))
            }
        };
        Ok(Some(Self {
            inner,
            uuid,
            expected: size,
            seen: 0,
        }))
    }

    /// The blob's size in bytes.
    pub fn len(&self) -> u64 {
        self.expected
    }

    /// Whether the blob is empty.
    pub fn is_empty(&self) -> bool {
        self.expected == 0
    }
}

impl Read for BlobReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(out)?;
        self.seen += n as u64;
        let short = n == 0 && !out.is_empty() && self.seen != self.expected;
        if self.seen > self.expected || short {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "blob {}: read {} bytes, size column says {}",
                    self.uuid, self.seen, self.expected
                ),
            ));
        }
        Ok(n)
    }
}

/// Writes one blob incrementally. Call [`Self::finish`] to store it; dropping
/// the writer instead may leave an unnamed partial blob (`uuid` starting with
/// `~`), which is never returned by lookups.
pub struct BlobWriter {
    store: BlobStore,
    hasher: Sha3_256,
    buf: Vec<u8>,
    spill: Option<Spill>,
    inline_max: usize,
    chunk_len: usize,
}

/// A blob being written chunk by chunk under a provisional name.
struct Spill {
    rid: Rid,
    next_seq: i64,
    len: i64,
}

impl BlobWriter {
    pub(crate) fn new(store: &BlobStore, inline_max: usize, chunk_len: usize) -> Self {
        Self {
            store: store.clone(),
            hasher: Sha3_256::new(),
            buf: vec![],
            spill: None,
            inline_max,
            chunk_len,
        }
    }

    /// Appends bytes to the blob.
    pub fn write(&mut self, data: &[u8]) -> CasResult<()> {
        self.hasher.update(data);
        self.buf.extend_from_slice(data);
        if self.spill.is_none() && self.buf.len() > self.inline_max {
            // Too big for one value: stream the rest out as raw chunks under
            // a provisional name, renamed once the hash is known.
            let row = with(self.store.conn().as_ref(), Access::Write, |x| {
                x.query_row(
                    "INSERT INTO blob(uuid, size, enc, content) \
                     VALUES ('~' || lower(hex(randomblob(32))), 0, ?, NULL) RETURNING rid",
                    &[Param::Int(Encoding::Raw as i64)],
                )
            })?
            .ok_or_else(|| SqlError("INSERT … RETURNING returned no row".to_owned()))?;
            self.spill = Some(Spill {
                rid: row.int(0)?,
                next_seq: 0,
                len: 0,
            });
        }
        if let Some(spill) = &mut self.spill {
            while self.buf.len() >= self.chunk_len {
                write_chunk(&self.store, spill, &self.buf[..self.chunk_len])?;
                self.buf.drain(..self.chunk_len);
            }
        }
        Ok(())
    }

    /// Stores the blob and returns its row id and hash. Idempotent like
    /// [`BlobStore::put`].
    pub fn finish(mut self) -> CasResult<(Rid, ArtifactHash)> {
        let hash = ArtifactHash(self.hasher.finalize().into());
        let Some(mut spill) = self.spill.take() else {
            let rid = with(self.store.conn().as_ref(), Access::Write, |x| {
                self.store.put_in(x, &hash, &self.buf)
            })?;
            return Ok((rid, hash));
        };
        if !self.buf.is_empty() {
            write_chunk(&self.store, &mut spill, &self.buf)?;
        }
        let uuid = hash.to_uuid();
        let rid = with(self.store.conn().as_ref(), Access::Write, |x| {
            if let Some(existing) = rid_in(x, &uuid)? {
                // Already stored: drop the provisional copy.
                x.exec(
                    "DELETE FROM jj_chunk WHERE rid = ?",
                    &[Param::Int(spill.rid)],
                )?;
                x.exec("DELETE FROM blob WHERE rid = ?", &[Param::Int(spill.rid)])?;
                return Ok::<_, CasError>(existing);
            }
            x.exec(
                "UPDATE blob SET uuid = ?, size = ? WHERE rid = ?",
                &[
                    Param::Text(&uuid),
                    Param::Int(spill.len),
                    Param::Int(spill.rid),
                ],
            )?;
            Ok(spill.rid)
        })?;
        Ok((rid, hash))
    }
}

fn write_chunk(store: &BlobStore, spill: &mut Spill, data: &[u8]) -> CasResult<()> {
    with(store.conn().as_ref(), Access::Write, |x| {
        x.exec(
            "INSERT INTO jj_chunk(rid, seq, data) VALUES (?, ?, ?)",
            &[
                Param::Int(spill.rid),
                Param::Int(spill.next_seq),
                Param::Blob(data),
            ],
        )
    })?;
    spill.next_seq += 1;
    spill.len += i64::try_from(data.len()).expect("chunk size fits in i64");
    Ok(())
}

#[cfg(all(test, feature = "rusqlite"))]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::sql::RusqliteConn;
    use crate::sql::SqlConn;
    use crate::store::ROW_OVERHEAD;

    fn store(max_value_len: usize) -> BlobStore {
        let conn = RusqliteConn::open_in_memory()
            .unwrap()
            .with_max_value_len(max_value_len);
        BlobStore::open(Arc::new(conn)).unwrap()
    }

    fn noise(len: usize) -> Vec<u8> {
        let mut state: u32 = 0x1234_5678;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect()
    }

    fn read_all(store: &BlobStore, hash: &ArtifactHash) -> Vec<u8> {
        let mut reader = store.reader(hash).unwrap().unwrap();
        let mut out = vec![];
        // Small reads, to cross chunk boundaries at odd offsets.
        let mut buf = [0; 7];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        out
    }

    fn count(store: &BlobStore, sql: &str) -> i64 {
        with(store.conn().as_ref() as &dyn SqlConn, Access::Read, |x| {
            x.query_row(sql, &[])?.unwrap().int(0)
        })
        .unwrap()
    }

    #[test]
    fn writer_spills_large_blobs_into_chunks() {
        let s = store(ROW_OVERHEAD + 100);
        let data = noise(1000);
        let mut writer = s.writer();
        for piece in data.chunks(33) {
            writer.write(piece).unwrap();
        }
        let (rid, hash) = writer.finish().unwrap();
        assert_eq!(hash, ArtifactHash::of(&data));
        assert_eq!(s.rid(&hash).unwrap(), Some(rid));
        assert_eq!(count(&s, "SELECT count(*) FROM jj_chunk"), 10);
        assert_eq!(count(&s, "SELECT size FROM blob"), 1000);
        assert_eq!(read_all(&s, &hash), data);
        assert_eq!(s.get(&hash).unwrap().unwrap(), data);
    }

    #[test]
    fn writer_dedupes_spilled_blobs() {
        let s = store(ROW_OVERHEAD + 100);
        let data = noise(500);
        let first = s.put(&data).unwrap();
        let mut writer = s.writer();
        writer.write(&data).unwrap();
        assert_eq!(writer.finish().unwrap(), first);
        assert_eq!(count(&s, "SELECT count(*) FROM blob"), 1);
        assert_eq!(
            count(&s, "SELECT count(*) FROM blob WHERE uuid LIKE '~%'"),
            0
        );
    }

    #[test]
    fn writer_keeps_small_blobs_inline_and_compressed() {
        let s = store(1_000_000);
        let data = "line\n".repeat(1000);
        let mut writer = s.writer();
        writer.write(data.as_bytes()).unwrap();
        let (_, hash) = writer.finish().unwrap();
        assert_eq!(count(&s, "SELECT count(*) FROM jj_chunk"), 0);
        assert_eq!(count(&s, "SELECT enc FROM blob"), Encoding::Zlib as i64);
        assert_eq!(read_all(&s, &hash), data.as_bytes());
    }

    #[test]
    fn reader_streams_compressed_chunked_blobs() {
        // `put` of in-memory bytes may chunk the compressed form.
        let s = store(ROW_OVERHEAD + 64);
        let data = "abcdefgh".repeat(4000);
        let (_, hash) = s.put(data.as_bytes()).unwrap();
        assert!(count(&s, "SELECT count(*) FROM jj_chunk") > 1);
        assert_eq!(read_all(&s, &hash), data.as_bytes());
    }

    #[test]
    fn reader_detects_missing_chunks() {
        let s = store(ROW_OVERHEAD + 100);
        let data = noise(1000);
        let (_, hash) = s.put(&data).unwrap();
        with(s.conn().as_ref(), Access::Write, |x| {
            x.exec("DELETE FROM jj_chunk WHERE seq = 9", &[])
        })
        .unwrap();
        let mut out = vec![];
        let err = s
            .reader(&hash)
            .unwrap()
            .unwrap()
            .read_to_end(&mut out)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
