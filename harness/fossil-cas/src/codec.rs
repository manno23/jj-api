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

//! At-rest encoding of blob content.
//!
//! SQLite does not compress pages, and a Durable Object caps both total size
//! and row size, so larger blobs are zlib-compressed when that pays off. The
//! compressed form is Fossil's `blob_compress` layout: a 4-byte big-endian
//! uncompressed length followed by a zlib stream. The artifact hash is always
//! over the raw bytes, never over this encoding.

use std::io::Read as _;
use std::io::Write as _;

use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;

/// How a blob's `content` is encoded (`blob.enc`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Encoding {
    /// Stored as-is.
    Raw = 0,
    /// `be32(len) ‖ zlib(raw)`.
    Zlib = 1,
}

impl Encoding {
    /// Parses the `blob.enc` column.
    pub fn from_i64(n: i64) -> Option<Self> {
        match n {
            0 => Some(Self::Raw),
            1 => Some(Self::Zlib),
            _ => None,
        }
    }
}

/// Blobs smaller than this are never compressed.
pub const COMPRESS_MIN_LEN: usize = 512;

/// Chooses an encoding for `raw` and returns the bytes to store. Compression
/// is kept only if it saves at least 10%.
pub fn encode(raw: &[u8]) -> (Encoding, Vec<u8>) {
    if raw.len() >= COMPRESS_MIN_LEN {
        let packed = compress(raw);
        if packed.len() <= raw.len() / 10 * 9 {
            return (Encoding::Zlib, packed);
        }
    }
    (Encoding::Raw, raw.to_vec())
}

/// Decodes stored bytes back to the raw content.
pub fn decode(encoding: Encoding, stored: Vec<u8>) -> Result<Vec<u8>, String> {
    match encoding {
        Encoding::Raw => Ok(stored),
        Encoding::Zlib => decompress(&stored),
    }
}

/// Fossil's `blob_compress`: `be32(len) ‖ zlib(raw)`.
pub fn compress(raw: &[u8]) -> Vec<u8> {
    let len = u32::try_from(raw.len()).expect("blob larger than 4 GiB");
    let mut out = len.to_be_bytes().to_vec();
    let mut encoder = ZlibEncoder::new(&mut out, Compression::default());
    encoder
        .write_all(raw)
        .expect("writing to a Vec cannot fail");
    encoder.finish().expect("writing to a Vec cannot fail");
    out
}

/// Inverse of [`compress`]. Checks the recorded length.
pub fn decompress(stored: &[u8]) -> Result<Vec<u8>, String> {
    let (header, body) = stored
        .split_first_chunk::<4>()
        .ok_or("compressed blob shorter than its header")?;
    let len = u32::from_be_bytes(*header) as usize;
    let mut out = Vec::with_capacity(len);
    ZlibDecoder::new(body)
        .read_to_end(&mut out)
        .map_err(|err| format!("zlib: {err}"))?;
    if out.len() != len {
        return Err(format!(
            "decompressed {} bytes, header says {len}",
            out.len()
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_layout_is_fossil_blob_compress() {
        let raw = b"hello hello hello hello";
        let packed = compress(raw);
        assert_eq!(&packed[..4], &(raw.len() as u32).to_be_bytes());
        // zlib header (CMF=0x78) follows the length.
        assert_eq!(packed[4], 0x78);
        assert_eq!(decompress(&packed).unwrap(), raw);
    }

    #[test]
    fn encode_picks_raw_for_small_or_incompressible() {
        assert_eq!(encode(b"tiny").0, Encoding::Raw);
        // A pseudo-random buffer doesn't compress.
        let mut x: u32 = 1;
        let noise: Vec<u8> = (0..4096)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                x as u8
            })
            .collect();
        let (enc, stored) = encode(&noise);
        assert_eq!(enc, Encoding::Raw);
        assert_eq!(stored, noise);
    }

    #[test]
    fn encode_compresses_repetitive_content() {
        let raw = "fn main() {}\n".repeat(200);
        let (enc, stored) = encode(raw.as_bytes());
        assert_eq!(enc, Encoding::Zlib);
        assert!(stored.len() < raw.len() / 4);
        assert_eq!(decode(enc, stored).unwrap(), raw.as_bytes());
    }

    #[test]
    fn decompress_rejects_bad_length() {
        let mut packed = compress(b"abcdef");
        packed[3] += 1;
        assert!(decompress(&packed).is_err());
        assert!(decompress(&[0, 0]).is_err());
    }
}
