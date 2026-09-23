# jj-harness — design (rev 3 draft, under discussion)

> Status: design only. No implementation until the open decisions in §5 are settled.

## 1. Goal

A remote agent harness built around one durable writer, `jjd`. `jjd` owns:

- a jj repository;
- its working copies;
- a filesystem view over them.

It runs in a Cloudflare Durable Object (SQLite-backed) or locally. Agents drive it
through capabilities exposed over capnweb or capnp-rpc. Storage borrows Fossil's
one good idea: a single SQLite file of content-addressed blobs.

## 2. Compatibility verdict

- **Using a Fossil-style CAS for jj objects: yes** (about 90% confident). jj's
  `Backend` (`core/src/backend.rs`) is a typed CAS in which the backend chooses
  the hash and the ID width. Fossil's `blob` table is an untyped CAS keyed by a
  hash of the raw bytes. Mapping `FileId`, `TreeId`, `CommitId` and `CopyId`
  onto `blob.uuid` (SHA3-256) fits, and file blobs come out byte-identical to
  Fossil file artifacts.
- **Treating a Fossil repo as a native, lossless jj store: no.** Fossil
  manifests cannot encode any of these:
  - per-directory trees;
  - `Merge<TreeId>` conflicts and conflict labels;
  - change IDs and predecessors;
  - a separate author and committer;
  - time-zone offsets;
  - copy history.

We use the positioning **"a Fossil-compatible CAS, not Fossil-compatible
history"**. A real Fossil view would be a later export: manifests in their own
artifact namespace, the full Schema1, and CI against a `fossil` binary. It is
never the live format.

## 3. Layers

```
 agent ─capnweb / capnp─► Harness ─► Repo | Worktree | Fs           L3 capabilities
                            │
                       jjd  (jj-lib: Store, Transaction, rewrite, revset)    L2
                            │  Backend · OpStore · OpHeadsStore · IndexStore
                            │  WorkspaceStore · WorkingCopy
                       jj-fossil-* stores over BlobStore + jj_* tables        L1
                            │
                       SqlConn::scope(Read | Write) — rusqlite | DO sql.exec  L0
```

## 4. Decisions accepted so far

### L0: SQL surface

This is the lowest common denominator of rusqlite and DO `ctx.storage.sql`.

```rust
pub enum Param<'a> { Null, Int(i64), Text(&'a str), Blob(&'a [u8]) }
pub enum Value { Null, Int(i64), Real(f64), Text(String), Blob(Vec<u8>) }
pub enum Access { Read, Write }
pub trait SqlExec {
    fn exec(&self, sql: &str, p: &[Param<'_>]) -> SqlResult<u64>;
    fn query(&self, sql: &str, p: &[Param<'_>]) -> SqlResult<Vec<Row>>;
}
pub trait SqlConn: Send + Sync + Debug {
    /// Write = one atomic transaction (BEGIN IMMEDIATE / transactionSync).
    /// The executor handle has no `scope`, so nesting is impossible.
    fn scope(&self, a: Access, f: &mut dyn FnMut(&dyn SqlExec) -> SqlResult<()>) -> SqlResult<()>;
    fn max_value_len(&self) -> usize;   // DO: 2 MB; rusqlite: effectively unlimited
}
```

- Row IDs come from `INSERT … ON CONFLICT DO NOTHING RETURNING rid`, with a
  `SELECT` fallback. `last_insert_rowid` is not used.
- The only SQL integers are row IDs and sizes, both below 2^53, so DO's
  JS-double integers are safe.

### L1: blob store

- Schema:
  - `blob(rid INTEGER PRIMARY KEY, uuid TEXT UNIQUE NOT NULL, size INTEGER, enc INTEGER, content BLOB)`
  - the `jj_*` tables
  - `config(name, value)` holding `jj-schema`

  The other Schema1 tables, `rcvfrom`, `application_id` and `project-code` are
  dropped.
- `uuid` is the lowercase-hex SHA3-256 of the **raw** bytes. It is never
  computed over the compressed or chunked form.
- `enc`: 0 means raw, 1 means zlib. zlib is applied only above a size threshold,
  and only when it saves about 10% or more. SQLite does not compress pages, and a
  DO has a 10 GB cap and a 2 MB row limit.
- Chunking is a property of the connection. When a stored value exceeds
  `max_value_len`, `content` is NULL and the bytes live in
  `jj_chunk(rid, seq, data)`. Only `BlobStore::get` reassembles them. This never
  happens locally.
- There is no SHA1, no delta encoding and no streaming hasher.

### L1: `jj-fossil-backend` (`impl Backend`)

- Commit IDs are 32 bytes (SHA3-256) and change IDs are 16 bytes.
- The root commit `[0; 32]` is virtual. The empty tree is virtual on read but
  written at init, and its ID is pinned in a test.
- Lookups return `InvalidHashLength` unless the ID is 32 bytes.
- **Codec:** hand-rolled and strict, so `decode(b) = x ⇒ encode(x) = b`.
  - Header: `\0 j j kind version`, where kind is `T` (tree), `C` (commit) or
    `Y` (copy). Fossil can never parse this as a control artifact.
  - Every ID is length-prefixed. Placeholder `CopyId`s are 0 bytes and submodule
    IDs are 20.
  - Decoding rejects: an even merge-term count, tree names that are not strictly
    sorted, non-minimal varints, and trailing bytes.
  - Labels are normalised with `ConflictLabels::from_merge` before encoding.
  - A signed commit is stored as `unsigned ‖ 'S' ‖ varint(len) ‖ sig`, and
    `secure_sig.data` is exactly that unsigned prefix. It is never re-encoded.
  - `write_commit` rejects a commit whose `secure_sig` is already set, and
    rejects a commit with no parents. It returns `decode(stored bytes)`.
  - The frozen field-order table is to be written here once the design settles.
- Files and symlinks are untyped (identical bytes are one blob).
  `jj_object(rid, kind)` is a non-authoritative index for trees, commits and
  copies. Reads never consult it.
- **Copies:**
  - `write_copy` checks that each parent blob exists and passes `decode_copy`.
    It writes the blob and the `jj_copy_edge` rows in one `scope(Write)`.
  - `get_related_copies` collects the related set with a recursive CTE, then
    orders it with `dag_walk::topo_order_reverse`.
  - A missing ID returns `ObjectNotFound` and a cycle returns `Other`. Neither
    ever panics.
  - `get_copy_records` returns an empty stream.

### Tests

- Add `TestRepoBackend::Fossil` to the shared cases only: `test_commit_builder`,
  `test_init`, the locking tests, concurrent commits, and the Simple-like signing
  tests.
- Git-only signing tests such as keep-on-rewrite are excluded.
- Pin these values: the empty-tree ID, an unsigned commit hash, a signed commit
  hash that differs from the unsigned one, and file/symlink dedupe. Also test that
  an unknown copy parent is rejected.

### The real Durable Object risk

jj-lib as a whole has to run in wasm inside the DO. Today:

- `lock/` has no non-unix/non-windows fallback;
- the default IndexStore, op stores and workspace store are all on-disk;
- `ReadonlyRepo::init` writes to the filesystem.

So L1 must provide **every** store on SQL: Backend, OpStore, OpHeadsStore,
IndexStore, WorkspaceStore and WorkingCopy. They are assembled with
`RepoLoader::new` (`lib/src/repo.rs`), not through the filesystem paths.

`jj-core` itself needs only a `file_util` platform fallback to build for
`wasm32-unknown-unknown`. It fails today with 9 errors, all from that module.

### Working copy on the DO: `SqlWorkingCopy`

- `wc_file(workspace, path, rid, exec, symlink, mtime)` rows point at blob
  row IDs.
- Snapshot costs O(dirty set).
- Checkout rewrites rows and copies no bytes.

## 5. Open decisions

1. **Where does jj-lib execute on Cloudflare?** Either wasm inside the DO
   (recommended: one writer, no hops, but it needs the jj-lib wasm port), or a
   Container sidecar with the DO as a remote SQLite (chatty).
2. **Phase 1 scope.** Either one SQLite file that a full jj repo reloads from
   (backend, op store, op heads, workspace store, first-cut index) plus the
   `.capnp` schema for `Repo`, `Worktree` and `Fs` (recommended), or the backend
   only.
3. **Where agent code execution lives.** Either the DO's SQL working copy is the
   source of truth and executors (Container, Sandbox, local) are clients that
   materialise a checkout through `Fs` (recommended), or the executor's disk is
   the working copy and the DO holds only history.
