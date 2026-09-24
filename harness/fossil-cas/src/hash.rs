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

//! Artifact names: SHA3-256 of the raw bytes, as Fossil names artifacts.

use std::fmt;

use sha3::Digest as _;
use sha3::Sha3_256;

/// The SHA3-256 of a blob's raw bytes. Its lowercase hex form is the blob's
/// `uuid`, exactly as Fossil names an artifact.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArtifactHash(pub [u8; 32]);

impl ArtifactHash {
    /// Length of the hash in bytes.
    pub const LEN: usize = 32;

    /// Hashes `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha3_256::digest(bytes).into())
    }

    /// Wraps a 32-byte slice, or returns `None` for any other length.
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok().map(Self)
    }

    /// The raw hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The lowercase-hex `uuid` of the blob.
    pub fn to_uuid(&self) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(64);
        for b in self.0 {
            out.push(DIGITS[usize::from(b >> 4)].into());
            out.push(DIGITS[usize::from(b & 0xf)].into());
        }
        out
    }

    /// Parses a 64-digit lowercase-hex `uuid`.
    pub fn from_uuid(uuid: &str) -> Option<Self> {
        fn nibble(c: u8) -> Option<u8> {
            match c {
                b'0'..=b'9' => Some(c - b'0'),
                b'a'..=b'f' => Some(c - b'a' + 10),
                _ => None,
            }
        }
        let bytes = uuid.as_bytes();
        if bytes.len() != 64 {
            return None;
        }
        let mut out = [0; 32];
        for (i, pair) in bytes.as_chunks::<2>().0.iter().enumerate() {
            out[i] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
        }
        Some(Self(out))
    }
}

impl fmt::Debug for ArtifactHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArtifactHash({})", self.to_uuid())
    }
}

impl fmt::Display for ArtifactHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_uuid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha3_known_answers() {
        // FIPS 202 test vectors.
        assert_eq!(
            ArtifactHash::of(b"").to_uuid(),
            "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a"
        );
        assert_eq!(
            ArtifactHash::of(b"abc").to_uuid(),
            "3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532"
        );
    }

    #[test]
    fn uuid_round_trip() {
        let h = ArtifactHash::of(b"hello");
        assert_eq!(ArtifactHash::from_uuid(&h.to_uuid()), Some(h));
        assert_eq!(ArtifactHash::from_slice(h.as_bytes()), Some(h));
        assert_eq!(ArtifactHash::from_uuid("ABCD"), None);
        assert_eq!(ArtifactHash::from_uuid(&"G".repeat(64)), None);
        assert_eq!(ArtifactHash::from_slice(&[0; 20]), None);
    }
}
