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

//! Canonical binary encoding of jj trees, commits and copy histories.
//!
//! Object ids are hashes of these bytes, so the encoding is a bijection:
//! `decode(b) == Ok(x)` implies `encode(x) == b`. The decoder rejects anything
//! the encoder would not produce (non-minimal varints, unsorted tree entries,
//! even-length merges, unknown tags, trailing bytes).
//!
//! # Format (version 1)
//!
//! Every object starts with the header `00 6a 6a <kind> 01` (`\0jj`, kind,
//! version). The leading NUL means Fossil can never mistake an object for a
//! control artifact, which must start with an upper-case card letter.
//!
//! Primitives: `uv` is an unsigned LEB128 varint (minimal); `sv` a zigzag
//! signed varint; `bytes` is `uv(len) ‖ data`; `str` is UTF-8 `bytes`; `bool`
//! is one byte `00`/`01`. Every id is `bytes`, since ids have varying lengths
//! (placeholder copy ids are empty, git submodule ids are 20 bytes).
//!
//! | Kind | Body, in order |
//! |------|----------------|
//! | `T` tree | `uv(n)`, then per entry: `str name`, `u8 tag`, payload. Tags: `0` file (`bytes id`, `bool executable`, `bytes copy_id`), `1` symlink (`bytes id`), `2` tree (`bytes id`), `3` git submodule (`bytes id`). Names strictly increasing. |
//! | `C` commit | `uv(n) bytes*` parents; `uv(n) bytes*` predecessors; `uv(n) bytes*` root tree terms (n odd); `uv(n) str*` conflict labels (n = 0 or odd; normalised with [`ConflictLabels::from_merge`]); `bytes` change id; `str` description; author and committer, each `str name`, `str email`, `sv millis`, `sv tz_offset`. |
//! | `Y` copy | `str current_path`; `uv(n) bytes*` parents; `bytes salt`. |
//!
//! A signed commit is the unsigned encoding followed by `53 ('S') ‖ bytes
//! sig`. The signed data is exactly the unsigned prefix, which is never
//! re-encoded.

use jj_core::backend::ChangeId;
use jj_core::backend::Commit;
use jj_core::backend::CommitId;
use jj_core::backend::CopyHistory;
use jj_core::backend::CopyId;
use jj_core::backend::FileId;
use jj_core::backend::MillisSinceEpoch;
use jj_core::backend::SecureSig;
use jj_core::backend::Signature;
use jj_core::backend::SymlinkId;
use jj_core::backend::Timestamp;
use jj_core::backend::Tree;
use jj_core::backend::TreeId;
use jj_core::backend::TreeValue;
use jj_core::conflict_labels::ConflictLabels;
use jj_core::merge::Merge;
use jj_core::object_id::ObjectId as _;
use jj_core::repo_path::RepoPathBuf;
use jj_core::repo_path::RepoPathComponentBuf;
use thiserror::Error;

/// Encoding version written by this build.
pub const VERSION: u8 = 1;
const MAGIC: [u8; 3] = *b"\0jj";
const SIG_TAG: u8 = b'S';

/// The kind of an encoded object, stored in its header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    /// A [`Tree`].
    Tree,
    /// A [`Commit`].
    Commit,
    /// A [`CopyHistory`].
    Copy,
}

impl Kind {
    fn byte(self) -> u8 {
        match self {
            Self::Tree => b'T',
            Self::Commit => b'C',
            Self::Copy => b'Y',
        }
    }

    /// The value recorded in the `jj_object.kind` column.
    pub fn index_code(self) -> i64 {
        match self {
            Self::Tree => 1,
            Self::Commit => 2,
            Self::Copy => 3,
        }
    }
}

/// A byte string that is not a valid encoding.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    /// The header is missing or wrong.
    #[error("not a jj object (bad magic)")]
    BadMagic,
    /// The object is of another kind.
    #[error("expected a {expected:?} object, found kind byte {found:#04x}")]
    WrongKind {
        /// The kind the caller asked for.
        expected: Kind,
        /// The kind byte found.
        found: u8,
    },
    /// Written by a newer encoder.
    #[error("unsupported encoding version {0}")]
    Version(u8),
    /// The input ended early.
    #[error("truncated object")]
    Truncated,
    /// A varint had redundant bytes or overflowed.
    #[error("non-canonical varint")]
    Varint,
    /// A boolean byte other than 0 or 1.
    #[error("invalid boolean byte {0:#04x}")]
    Bool(u8),
    /// A text field was not UTF-8.
    #[error("invalid UTF-8")]
    Utf8,
    /// An unknown tree-value tag.
    #[error("unknown tree value tag {0}")]
    Tag(u8),
    /// A path component or path was invalid.
    #[error("invalid path: {0}")]
    Path(String),
    /// Tree entries were not strictly increasing.
    #[error("tree entries not strictly sorted")]
    Unsorted,
    /// A merge had an even number of terms.
    #[error("merge with an even number of terms ({0})")]
    EvenMerge(usize),
    /// Conflict labels that encoding would have normalised away (labels on
    /// a resolved merge, or all-empty labels).
    #[error("non-canonical conflict labels")]
    Labels,
    /// Bytes followed the encoded object.
    #[error("trailing bytes")]
    Trailing,
}

// ---- writer ----------------------------------------------------------------

struct Writer(Vec<u8>);

impl Writer {
    fn new(kind: Kind) -> Self {
        let mut out = MAGIC.to_vec();
        out.push(kind.byte());
        out.push(VERSION);
        Self(out)
    }

    fn uv(&mut self, mut n: u64) {
        loop {
            let low = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                self.0.push(low);
                return;
            }
            self.0.push(low | 0x80);
        }
    }

    fn len(&mut self, n: usize) {
        self.uv(n as u64);
    }

    fn sv(&mut self, n: i64) {
        self.uv(((n << 1) ^ (n >> 63)) as u64);
    }

    fn bytes(&mut self, data: &[u8]) {
        self.len(data.len());
        self.0.extend_from_slice(data);
    }

    fn str(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }

    fn bool(&mut self, b: bool) {
        self.0.push(u8::from(b));
    }

    fn ids<'a>(&mut self, ids: impl ExactSizeIterator<Item = &'a [u8]>) {
        self.len(ids.len());
        for id in ids {
            self.bytes(id);
        }
    }

    fn signature(&mut self, sig: &Signature) {
        self.str(&sig.name);
        self.str(&sig.email);
        self.sv(sig.timestamp.timestamp.0);
        self.sv(i64::from(sig.timestamp.tz_offset));
    }
}

// ---- reader ----------------------------------------------------------------

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8], kind: Kind) -> Result<Self, DecodeError> {
        let [m0, m1, m2, k, v, ..] = *buf else {
            return Err(DecodeError::BadMagic);
        };
        if [m0, m1, m2] != MAGIC {
            return Err(DecodeError::BadMagic);
        }
        if k != kind.byte() {
            return Err(DecodeError::WrongKind {
                expected: kind,
                found: k,
            });
        }
        if v != VERSION {
            return Err(DecodeError::Version(v));
        }
        Ok(Self { buf, pos: 5 })
    }

    fn at_end(&self) -> bool {
        self.pos == self.buf.len()
    }

    fn finish(&self) -> Result<(), DecodeError> {
        if self.at_end() {
            Ok(())
        } else {
            Err(DecodeError::Trailing)
        }
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        let b = *self.buf.get(self.pos).ok_or(DecodeError::Truncated)?;
        self.pos += 1;
        Ok(b)
    }

    fn uv(&mut self) -> Result<u64, DecodeError> {
        let mut n: u64 = 0;
        for i in 0..10 {
            let b = self.u8()?;
            let payload = u64::from(b & 0x7f);
            if i == 9 && payload > 1 {
                return Err(DecodeError::Varint);
            }
            n |= payload << (7 * i);
            if b & 0x80 == 0 {
                // A zero final byte after the first is redundant.
                if i > 0 && b == 0 {
                    return Err(DecodeError::Varint);
                }
                return Ok(n);
            }
        }
        Err(DecodeError::Varint)
    }

    fn len(&mut self) -> Result<usize, DecodeError> {
        let n = usize::try_from(self.uv()?).map_err(|_| DecodeError::Truncated)?;
        // Every element takes at least one byte, so a count larger than the
        // rest of the input is truncated; this also bounds allocations.
        if n > self.buf.len() - self.pos {
            return Err(DecodeError::Truncated);
        }
        Ok(n)
    }

    fn sv(&mut self) -> Result<i64, DecodeError> {
        let n = self.uv()?;
        Ok(((n >> 1) as i64) ^ -((n & 1) as i64))
    }

    fn bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.len()?;
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn string(&mut self) -> Result<String, DecodeError> {
        str::from_utf8(self.bytes()?)
            .map(str::to_owned)
            .map_err(|_| DecodeError::Utf8)
    }

    fn bool(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(DecodeError::Bool(b)),
        }
    }

    fn ids<T>(&mut self, make: impl Fn(&[u8]) -> T) -> Result<Vec<T>, DecodeError> {
        let n = self.len()?;
        (0..n).map(|_| self.bytes().map(&make)).collect()
    }

    fn signature(&mut self) -> Result<Signature, DecodeError> {
        let name = self.string()?;
        let email = self.string()?;
        let millis = self.sv()?;
        let tz_offset = i32::try_from(self.sv()?).map_err(|_| DecodeError::Varint)?;
        Ok(Signature {
            name,
            email,
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(millis),
                tz_offset,
            },
        })
    }
}

fn odd_merge<T>(values: Vec<T>) -> Result<Merge<T>, DecodeError> {
    if values.len().is_multiple_of(2) {
        return Err(DecodeError::EvenMerge(values.len()));
    }
    Ok(Merge::from_vec(values))
}

// ---- trees -----------------------------------------------------------------

/// Encodes a tree.
pub fn encode_tree(tree: &Tree) -> Vec<u8> {
    let mut w = Writer::new(Kind::Tree);
    let entries: Vec<_> = tree.entries().collect();
    w.len(entries.len());
    for entry in entries {
        w.str(entry.name().as_internal_str());
        match entry.value() {
            TreeValue::File {
                id,
                executable,
                copy_id,
            } => {
                w.0.push(0);
                w.bytes(id.as_bytes());
                w.bool(*executable);
                w.bytes(copy_id.as_bytes());
            }
            TreeValue::Symlink(id) => {
                w.0.push(1);
                w.bytes(id.as_bytes());
            }
            TreeValue::Tree(id) => {
                w.0.push(2);
                w.bytes(id.as_bytes());
            }
            TreeValue::GitSubmodule(id) => {
                w.0.push(3);
                w.bytes(id.as_bytes());
            }
        }
    }
    w.0
}

/// Decodes a tree.
pub fn decode_tree(buf: &[u8]) -> Result<Tree, DecodeError> {
    let mut r = Reader::new(buf, Kind::Tree)?;
    let n = r.len()?;
    let mut entries: Vec<(RepoPathComponentBuf, TreeValue)> = Vec::with_capacity(n);
    for _ in 0..n {
        let name = RepoPathComponentBuf::new(r.string()?)
            .map_err(|err| DecodeError::Path(err.to_string()))?;
        if let Some((prev, _)) = entries.last()
            && *prev >= name
        {
            return Err(DecodeError::Unsorted);
        }
        let value = match r.u8()? {
            0 => TreeValue::File {
                id: FileId::from_bytes(r.bytes()?),
                executable: r.bool()?,
                copy_id: CopyId::from_bytes(r.bytes()?),
            },
            1 => TreeValue::Symlink(SymlinkId::from_bytes(r.bytes()?)),
            2 => TreeValue::Tree(TreeId::from_bytes(r.bytes()?)),
            3 => TreeValue::GitSubmodule(CommitId::from_bytes(r.bytes()?)),
            tag => return Err(DecodeError::Tag(tag)),
        };
        entries.push((name, value));
    }
    r.finish()?;
    Ok(Tree::from_sorted_entries(entries))
}

/// The encoding of the empty tree. Its hash is the backend's empty tree id.
pub fn empty_tree_bytes() -> Vec<u8> {
    encode_tree(&Tree::default())
}

// ---- commits ---------------------------------------------------------------

/// Encodes a commit without its signature. `commit.secure_sig` is ignored.
/// These bytes are what gets signed.
pub fn encode_commit_unsigned(commit: &Commit) -> Vec<u8> {
    let mut w = Writer::new(Kind::Commit);
    w.ids(commit.parents.iter().map(|id| id.as_bytes()));
    w.ids(commit.predecessors.iter().map(|id| id.as_bytes()));
    w.ids(commit.root_tree.iter().map(|id| id.as_bytes()));
    let labels = ConflictLabels::from_merge(commit.conflict_labels.clone());
    let labels = labels.as_slice();
    w.len(labels.len());
    for label in labels {
        w.str(label);
    }
    w.bytes(commit.change_id.as_bytes());
    w.str(&commit.description);
    w.signature(&commit.author);
    w.signature(&commit.committer);
    w.0
}

/// Appends a signature to an unsigned commit encoding.
pub fn attach_sig(mut unsigned: Vec<u8>, sig: &[u8]) -> Vec<u8> {
    unsigned.push(SIG_TAG);
    let mut w = Writer(unsigned);
    w.bytes(sig);
    w.0
}

/// Decodes a commit. For a signed commit, `secure_sig.data` is the exact
/// unsigned prefix of `buf`.
pub fn decode_commit(buf: &[u8]) -> Result<Commit, DecodeError> {
    let mut r = Reader::new(buf, Kind::Commit)?;
    let parents = r.ids(CommitId::from_bytes)?;
    let predecessors = r.ids(CommitId::from_bytes)?;
    let root_tree = odd_merge(r.ids(TreeId::from_bytes)?)?;
    let n_labels = r.len()?;
    let labels = (0..n_labels)
        .map(|_| r.string())
        .collect::<Result<Vec<_>, _>>()?;
    let conflict_labels = if labels.is_empty() {
        Merge::resolved(String::new())
    } else {
        let merge = odd_merge(labels)?;
        let normalized = ConflictLabels::from_merge(merge.clone()).into_merge();
        // Normalisation must be a no-op on canonical input.
        if normalized != merge {
            return Err(DecodeError::Labels);
        }
        merge
    };
    let change_id = ChangeId::from_bytes(r.bytes()?);
    let description = r.string()?;
    let author = r.signature()?;
    let committer = r.signature()?;
    let unsigned_len = r.pos;
    let secure_sig = if r.at_end() {
        None
    } else {
        match r.u8()? {
            SIG_TAG => Some(SecureSig {
                data: buf[..unsigned_len].to_vec(),
                sig: r.bytes()?.to_vec(),
            }),
            _ => return Err(DecodeError::Trailing),
        }
    };
    r.finish()?;
    Ok(Commit {
        parents,
        predecessors,
        root_tree,
        conflict_labels,
        change_id,
        description,
        author,
        committer,
        secure_sig,
    })
}

// ---- copies ----------------------------------------------------------------

/// Encodes a copy history.
pub fn encode_copy(copy: &CopyHistory) -> Vec<u8> {
    let mut w = Writer::new(Kind::Copy);
    w.str(copy.current_path.as_internal_file_string());
    w.ids(copy.parents.iter().map(|id| id.as_bytes()));
    w.bytes(&copy.salt);
    w.0
}

/// Decodes a copy history.
pub fn decode_copy(buf: &[u8]) -> Result<CopyHistory, DecodeError> {
    let mut r = Reader::new(buf, Kind::Copy)?;
    let current_path = RepoPathBuf::from_internal_string(r.string()?)
        .map_err(|err| DecodeError::Path(err.to_string()))?;
    let parents = r.ids(CopyId::from_bytes)?;
    let salt = r.bytes()?.to_vec();
    r.finish()?;
    Ok(CopyHistory {
        current_path,
        parents,
        salt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(name: &str, millis: i64, tz: i32) -> Signature {
        Signature {
            name: name.to_owned(),
            email: format!("{name}@example.com"),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(millis),
                tz_offset: tz,
            },
        }
    }

    fn name(s: &str) -> RepoPathComponentBuf {
        RepoPathComponentBuf::new(s).unwrap()
    }

    fn sample_tree() -> Tree {
        Tree::from_sorted_entries(vec![
            (
                name("a.txt"),
                TreeValue::File {
                    id: FileId::new(vec![1; 32]),
                    executable: true,
                    copy_id: CopyId::placeholder(),
                },
            ),
            (name("dir"), TreeValue::Tree(TreeId::new(vec![2; 32]))),
            (
                name("link"),
                TreeValue::Symlink(SymlinkId::new(vec![3; 32])),
            ),
            (
                name("sub"),
                TreeValue::GitSubmodule(CommitId::new(vec![4; 20])),
            ),
        ])
    }

    fn sample_commit() -> Commit {
        Commit {
            parents: vec![CommitId::new(vec![9; 32]), CommitId::new(vec![8; 32])],
            predecessors: vec![CommitId::new(vec![7; 32])],
            root_tree: Merge::from_vec(vec![
                TreeId::new(vec![1; 32]),
                TreeId::new(vec![2; 32]),
                TreeId::new(vec![3; 32]),
            ]),
            conflict_labels: Merge::from_vec(vec![
                "left".to_owned(),
                "base".to_owned(),
                "right".to_owned(),
            ]),
            change_id: ChangeId::new(vec![5; 16]),
            description: "multi\nline ✓\n".to_owned(),
            author: sig("alice", -1_234_567, -480),
            committer: sig("bob", 1_700_000_000_000, 330),
            secure_sig: None,
        }
    }

    #[test]
    fn header_starts_with_nul() {
        for bytes in [
            empty_tree_bytes(),
            encode_commit_unsigned(&sample_commit()),
            encode_copy(&CopyHistory {
                current_path: RepoPathBuf::from_internal_string("x/y").unwrap(),
                parents: vec![],
                salt: vec![],
            }),
        ] {
            assert_eq!(&bytes[..3], b"\0jj");
            assert_eq!(bytes[4], VERSION);
        }
    }

    #[test]
    fn tree_round_trip_is_bijective() {
        for tree in [Tree::default(), sample_tree()] {
            let bytes = encode_tree(&tree);
            let decoded = decode_tree(&bytes).unwrap();
            assert_eq!(decoded, tree);
            assert_eq!(encode_tree(&decoded), bytes);
        }
    }

    #[test]
    fn commit_round_trip_is_bijective() {
        let mut resolved = sample_commit();
        resolved.root_tree = Merge::resolved(TreeId::new(vec![1; 32]));
        resolved.conflict_labels = Merge::resolved(String::new());
        for commit in [sample_commit(), resolved] {
            let bytes = encode_commit_unsigned(&commit);
            let decoded = decode_commit(&bytes).unwrap();
            assert_eq!(decoded, commit);
            assert_eq!(encode_commit_unsigned(&decoded), bytes);
        }
    }

    #[test]
    fn labels_are_normalized() {
        let mut commit = sample_commit();
        commit.conflict_labels = Merge::from_vec(vec![String::new(), String::new(), String::new()]);
        let decoded = decode_commit(&encode_commit_unsigned(&commit)).unwrap();
        assert_eq!(decoded.conflict_labels, Merge::resolved(String::new()));
    }

    #[test]
    fn signed_commit_keeps_exact_unsigned_prefix() {
        let unsigned = encode_commit_unsigned(&sample_commit());
        let signed = attach_sig(unsigned.clone(), b"signature bytes");
        let decoded = decode_commit(&signed).unwrap();
        let secure_sig = decoded.secure_sig.clone().unwrap();
        assert_eq!(secure_sig.data, unsigned);
        assert_eq!(secure_sig.sig, b"signature bytes");
        // Re-encoding the decoded commit without its signature gives the
        // signed data back, byte for byte.
        assert_eq!(encode_commit_unsigned(&decoded), unsigned);
    }

    #[test]
    fn copy_round_trip() {
        let copy = CopyHistory {
            current_path: RepoPathBuf::from_internal_string("dir/file.rs").unwrap(),
            parents: vec![CopyId::new(vec![1; 32])],
            salt: vec![0xde, 0xad],
        };
        let bytes = encode_copy(&copy);
        assert_eq!(decode_copy(&bytes).unwrap(), copy);
    }

    #[test]
    fn rejects_non_canonical_input() {
        let tree = encode_tree(&sample_tree());
        // Trailing byte.
        let mut long = tree.clone();
        long.push(0);
        assert_eq!(decode_tree(&long), Err(DecodeError::Trailing));
        // Truncation.
        assert_eq!(
            decode_tree(&tree[..tree.len() - 1]),
            Err(DecodeError::Truncated)
        );
        // Wrong kind and magic.
        assert!(matches!(
            decode_commit(&tree),
            Err(DecodeError::WrongKind { .. })
        ));
        assert_eq!(decode_tree(b"F some-card\n"), Err(DecodeError::BadMagic));
        // Non-minimal varint for the entry count: 0x84 0x00 instead of 0x04.
        let mut padded = tree[..5].to_vec();
        padded.extend([0x84, 0x00]);
        padded.extend(&tree[6..]);
        assert_eq!(decode_tree(&padded), Err(DecodeError::Varint));
    }

    #[test]
    fn rejects_unsorted_tree_and_even_merge() {
        // Swap the order of two single-entry encodings by hand.
        let mut w = Writer::new(Kind::Tree);
        w.len(2);
        for n in ["b", "a"] {
            w.str(n);
            w.0.push(2);
            w.bytes(&[0; 32]);
        }
        assert_eq!(decode_tree(&w.0), Err(DecodeError::Unsorted));

        let mut w = Writer::new(Kind::Commit);
        w.len(0);
        w.len(0);
        w.ids([&[1u8; 32][..], &[2u8; 32][..]].into_iter());
        assert_eq!(decode_commit(&w.0), Err(DecodeError::EvenMerge(2)));
    }

    #[test]
    fn rejects_non_canonical_labels() {
        let encode_with_labels = |root_trees: usize, labels: &[&str]| {
            let mut w = Writer::new(Kind::Commit);
            w.len(0); // parents
            w.len(0); // predecessors
            w.len(root_trees);
            for i in 0..root_trees {
                w.bytes(&[u8::try_from(i).unwrap(); 32]);
            }
            w.len(labels.len());
            for label in labels {
                w.str(label);
            }
            w.bytes(&[0; 16]);
            w.str("");
            for _ in 0..2 {
                w.signature(&sig("x", 0, 0));
            }
            w.0
        };
        // Canonical: no labels, or a full set on a conflict.
        assert!(decode_commit(&encode_with_labels(1, &[])).is_ok());
        assert!(decode_commit(&encode_with_labels(3, &["a", "b", "c"])).is_ok());
        // A label on a resolved tree.
        assert_eq!(
            decode_commit(&encode_with_labels(1, &["stray"])),
            Err(DecodeError::Labels)
        );
        // All-empty labels on a conflict.
        assert_eq!(
            decode_commit(&encode_with_labels(3, &["", "", ""])),
            Err(DecodeError::Labels)
        );
        // An even number of labels.
        assert_eq!(
            decode_commit(&encode_with_labels(3, &["a", "b"])),
            Err(DecodeError::EvenMerge(2))
        );
    }

    #[test]
    fn rejects_bad_bools_tags_and_utf8() {
        let tree = encode_tree(&sample_tree());
        // The first entry is `a.txt`, a file: find its executable flag (1).
        let exec_pos = 5 + 1 + 1 + 5 + 1 + 1 + 32;
        assert_eq!(tree[exec_pos], 1);
        let mut bad_bool = tree.clone();
        bad_bool[exec_pos] = 2;
        assert_eq!(decode_tree(&bad_bool), Err(DecodeError::Bool(2)));
        let tag_pos = 5 + 1 + 1 + 5;
        let mut bad_tag = tree.clone();
        bad_tag[tag_pos] = 9;
        assert_eq!(decode_tree(&bad_tag), Err(DecodeError::Tag(9)));
        let mut bad_utf8 = tree;
        bad_utf8[5 + 1 + 1] = 0xff;
        assert_eq!(decode_tree(&bad_utf8), Err(DecodeError::Utf8));
        // An unknown version.
        let mut future = empty_tree_bytes();
        future[4] = 2;
        assert_eq!(decode_tree(&future), Err(DecodeError::Version(2)));
    }

    #[test]
    fn varints_round_trip() {
        for n in [0, 1, 127, 128, 300, i64::MAX as u64, u64::MAX] {
            let mut w = Writer(vec![]);
            w.uv(n);
            let mut r = Reader { buf: &w.0, pos: 0 };
            assert_eq!(r.uv().unwrap(), n);
            assert!(r.at_end());
        }
        for n in [0, -1, 1, i64::MIN, i64::MAX, -480] {
            let mut w = Writer(vec![]);
            w.sv(n);
            let mut r = Reader { buf: &w.0, pos: 0 };
            assert_eq!(r.sv().unwrap(), n);
        }
    }
}
