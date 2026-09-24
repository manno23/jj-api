# jj-harness: design (rev 4)

> Status: the design decisions are settled (§5). Phase 1 is implemented (§6).

## 1. Goal

A remote agent harness built around one durable writer, `jjd`. `jjd` owns a jj
repository, its working copies, and a filesystem view over them. It runs in a
Cloudflare Durable Object (DO), backed by SQLite, or it runs locally. Agents
drive it through capabilities over capnweb or capnp-rpc.

Storage borrows Fossil's one good idea: a single SQLite file of
content-addressed blobs.

## 2. Compatibility verdict

- **Using a Fossil-style content-addressed store (CAS) for jj objects: yes**
  (about 90% confident).
  - jj's `Backend` (`core/src/backend.rs`) is a typed CAS. The backend chooses
    the hash and the ID width.
  - Fossil's `blob` table is an untyped CAS keyed by a hash of the raw bytes.
  - Mapping `FileId`, `TreeId`, `CommitId` and `CopyId` onto `blob.uuid`
    (SHA3-256) fits. File blobs are byte-identical to Fossil file artifacts.
- **Treating a Fossil repo as a native, lossless jj store: no.** Fossil
  manifests cannot encode any of these:
  - per-directory trees;
  - `Merge<TreeId>` conflicts and conflict labels;
  - change ids and predecessors;
  - separate author and committer;
  - time-zone offsets;
  - copy history.

The positioning is a *Fossil-compatible CAS, not Fossil-compatible history*.
A real Fossil view would be a later export: manifests in their own artifact
namespace, full Schema1, and CI against a `fossil` binary. It is never the
live format.

## 3. Layers

```
 agent ─capnweb / capnp─► Harness ─► Repo | Worktree | Fs           L3 capabilities
                            │                                            (schema/jjd.capnp)
                       jjd  (jj-lib: Store, Transaction, rewrite, revset)    L2
                            │  Backend · OpStore · OpHeadsStore · IndexStore
                            │  WorkspaceStore · WorkingCopy
                       jj-fossil-backend, jj-fossil-stores                   L1
                            │
                       jj-fossil-cas: BlobStore over SqlConn::scope          L0
                            └── rusqlite (local) | DO ctx.storage.sql (wasm)
```

## 4. Crates and interfaces

### `jj-fossil-cas` (L0)

This crate has no jj dependencies and builds for wasm32 without the
`rusqlite` feature.

```rust
// sql: the lowest common denominator of rusqlite and a DO's ctx.storage.sql
pub enum Param<'a> { Null, Int(i64), Text(&'a str), Blob(&'a [u8]) }
pub enum Value { Null, Int(i64), Real(f64), Text(String), Blob(Vec<u8>) }
pub enum Access { Read, Write }
pub trait SqlExec { fn exec(..) -> SqlResult<u64>; fn exec_batch(..); fn query(..) -> SqlResult<Vec<Row>>; fn query_row(..) }
pub trait SqlConn: Send + Sync + Debug {
    fn scope(&self, a: Access, f: &mut dyn FnMut(&dyn SqlExec) -> SqlResult<()>) -> SqlResult<()>;
    fn max_value_len(&self) -> usize;
}
pub fn with<T, E: From<SqlError>>(c: &dyn SqlConn, a: Access, f: impl FnOnce(&dyn SqlExec) -> Result<T, E>) -> Result<T, E>;
pub struct RusqliteConn;  // open, open_in_memory, shared(path) (one connection per file per process), with_max_value_len

// store
pub struct BlobStore; // open, put, put_in(x, hash, raw), get, get_in, rid, contains, conn,
                      // reader(hash) -> BlobReader (io::Read), writer() -> BlobWriter
```

- A `Write` scope is a single transaction: `BEGIN IMMEDIATE` locally, or
  `transactionSync` on a DO. The executor passed into the scope cannot open
  another scope, so nested transactions are impossible.
- New row ids come from `INSERT … RETURNING rid`.
- **Integer range:** every integer that crosses the SQL boundary, in
  parameters and in results, must lie within ±(2^53 − 1)
  (`sql::MAX_SAFE_INT`), because a DO passes numbers through JS doubles. This
  covers every integer column: `rid`, `size`, `enc`, `jj_chunk.seq`,
  `jj_object.kind`, and the future `wc_file` `exec`, `symlink` and `mtime`
  fields. Hashes and ids are always text or blobs. `sql::check_params` and
  `Row::int` reject violations, and every `SqlConn` must call
  `check_params`.
- `blob(rid, uuid UNIQUE, size, enc, content)`:
  - `uuid` is the lowercase-hex SHA3-256 of the **raw** bytes.
  - `enc`: `0` means raw, `1` means Fossil's `be32(len) ‖ zlib` layout. zlib
    is used only for blobs of at least 512 bytes, and only when it saves at
    least 10%.
- Blobs larger than `max_value_len` are stored as `jj_chunk(rid, seq, data)`
  rows with `content` set to NULL. Only a DO triggers this, because of its
  2 MB limit.
- **Bounded memory.** File content never has to fit in memory at once:
  - `BlobReader` pages in one chunk row at a time and decompresses as a
    stream. It checks the size column at the end.
  - `BlobWriter` hashes incrementally. Once the content exceeds one value, it
    writes raw chunks under a provisional name (`uuid` starting with `~`,
    which never matches a lookup). When it finishes, it renames the blob to
    its hash, or drops it if that hash is already stored.
  - Memory per blob operation is therefore O(`max_value_len`): about 2 MB on
    a DO. Locally the limit is SQLite's, so blobs stay inline.
  - The backend's `read_file` and `write_file` use these streams. `get` and
    `put` load the whole value, so they are kept for small structured
    objects.
  - An interrupted `BlobWriter` can leave a `~` blob behind; GC (§7) will
    sweep these.
- `config(name, value)` holds one schema-version row per layer.

### `jj-fossil-backend` (L1)

This crate implements `jj_core::backend::Backend` and depends only on
`jj-core`. It builds for wasm32.

- IDs are SHA3-256 of the stored bytes. Commit IDs are 32 bytes; change IDs
  are 16.
- The root commit `[0; 32]` and the empty tree are virtual. The empty tree ID
  is pinned in tests: `9a2d81e1…f9ee`.
- **Codec** (`codec.rs`, format table in its module docs):
  - Header is `\0 j j <kind> 01`, where kind is `T`, `C` or `Y`.
  - The codec is strict and bijective. Every ID is length-prefixed.
  - Decoding rejects an even number of merge terms, unsorted tree entries,
    non-minimal varints and trailing bytes.
  - Labels are normalized with `ConflictLabels::from_merge` on encode.
    Decoding rejects anything that normalization would change (a label on a
    resolved tree, or all-empty labels): `DecodeError::Labels`.
  - A signed commit is stored as `unsigned ‖ 'S' ‖ bytes(sig)`.
    `secure_sig.data` is exactly the unsigned prefix.
- `write_commit`:
  - rejects commits with no parents;
  - rejects commits whose `secure_sig` is already set;
  - returns `decode(stored)`.
- Files and symlinks are untyped. `jj_object(rid, kind)` indexes trees,
  commits and copies but is never read on lookup.
- Copies:
  - `jj_copy_edge(child, parent)`.
  - `write_copy` checks each parent: the blob must exist and decode as a copy.
  - `get_related_copies` runs a recursive CTE, then `topo_order_reverse`.
    The CTE uses `UNION`, so it terminates even on corrupt, cyclic edges.
  - The order is deterministic whatever order SQL returns rows in:
    1. results go into a map;
    2. the walk starts from the **sorted** ids;
    3. neighbors are each copy's stored `parents` list, which is part of its
       content.
  - A missing ID or a cycle is an error, never a panic.
- **Tests cover every decoder rule with a negative fixture:** bad magic,
  wrong kind, unknown version, truncation, non-minimal varint, bad bool,
  unknown tag, invalid UTF-8, unsorted names, even merges, non-canonical
  labels, and trailing bytes. Copy-graph tests cover a chain, a diamond (the
  same order from every start), an unknown or non-copy parent, a missing ID,
  and cyclic edges.

### `jj-fossil-stores` (L1, native for now)

This crate depends on `jj-lib`.

| jj store | Implementation | Tables |
|---|---|---|
| `OpStore` | `SqlOpStore` (`fossil_op_store`) | blobs `\0jjV\x01`/`\0jjO\x01` + simple-op-store protobuf; `jj_view(id, rid)`, `jj_operation(id, hex, rid)` |
| `OpHeadsStore` | `SqlOpHeadsStore` (`fossil_op_heads_store`) | `jj_op_head(id)`, swapped in one `scope(Write)` |
| `WorkspaceStore` | `SqlWorkspaceStore` (`fossil_workspace_store`) | `jj_workspace(name, path)` |
| `IndexStore` | jj's `DefaultIndexStore` | none; see below |

- Operation and view IDs keep jj's convention: BLAKE2b-512 of the struct's
  `ContentHash`. Prefix lookup does a range scan on `hex`.
- The protobuf converters are exposed as
  `jj_lib::simple_op_store::{encode,decode}_{view,operation}`.
- **Index (deviation from rev 3).** jj-lib's default index writes segment
  files and has no in-memory mode. Phase 1 therefore keeps it as an on-disk
  **cache** next to the database. It rebuilds from SQLite when deleted, and
  the acceptance test proves this. An SQL- or memory-backed `IndexStore` is
  part of the wasm port (§7).
- `gc` does nothing for now: unreachable operations are kept.
- Helpers:
  - `init_repo`, `init_workspace`;
  - `fossil_store_factories()`, `store_factories()`;
  - `repo_conn(store_dir)`.

### Capability surface (L3)

`schema/jjd.capnp` defines `Harness`, `Repo`, `Worktree` and `Fs`. The capnweb
`RpcTarget` classes mirror it one to one.

- Every mutation returns `OpInfo`.
- Authority is exactly the set of stubs an agent holds.

## 5. Decisions (settled)

1. **Where `jjd` runs on Cloudflare:** inside the DO, as wasm (in a dynamic
   worker). This means porting jj-lib to wasm. The native Container sidecar is
   not the primary target.
2. **Phase 1 scope:** backend, op store, op heads, workspace store and a
   first-cut index. A whole repo reloads from one SQLite file. The
   `.capnp` schema is included.
3. **Where the working copy's source of truth lives:** in the DO's SQL
   working copy. Lifecycle:
   - A repo snapshot is kept as a single **R2 object**, holding the SQLite
     database image or an export of its blob and `jj_*` rows.
   - On activation, the dynamic worker and the DO fetch that object whole and
     **rehydrate** SQL: repo tables plus the working-copy rows (`wc_file`).
   - All updates then happen in the DO's SQL. Snapshots are written back to
     R2 on a schedule or when the DO goes idle.
   - Executors that need a real disk are clients: they materialise a checkout
     through `Fs`.

## 6. Phase 1 — done

- `core/src/file_util.rs` gains a `platform` fallback for targets that are
  neither unix nor windows. `jj-core` now builds for `wasm32-unknown-unknown`.
- The crates `harness/fossil-cas`, `harness/fossil-backend` and
  `harness/fossil-stores` are in the workspace.
- `testutils::TestRepoBackend::Fossil` runs jj-lib's shared backend tests:
  - `test_commit_builder`, `test_init`, `test_commit_concurrent`,
    `test_signing::manual`, `test_bad_locking_interrupted`.
  - `test_bad_locking_children` is excluded because it merges repo
    directories file by file, which a single database can't support.
- The acceptance test is `fossil-stores/tests/reload.rs`. It:
  1. initializes a repo, commits, and rebases into a conflict;
  2. checks that nothing but `type` markers and `fossil.sqlite` exists
     outside the index cache;
  3. deletes the index;
  4. reloads through `RepoLoader`;
  5. checks that the heads, the op log, the conflicted commit and ancestry
     all match.
- `fossil rebuild` interoperability is **not** claimed, and no `fossil`
  binary is used in tests.

## 7. Next phases

1. **jj-lib wasm port:**
   - ~~a `lock/` fallback~~ — done: `lib/src/lock/wasm.rs`, plus `rand`'s
     `getrandom` needing the `wasm_js` backend. `jj-lib --no-default-features`
     now builds clean for `wasm32-unknown-unknown`; its native test suite is
     unaffected. `rayon` and `gix` were never a *build*-time problem (`gix` is
     already optional and off; `rayon` compiles for wasm32) — and `rayon`'s
     thread pool, which would panic if actually invoked on a target with no
     real threads, turns out not to be a *runtime* risk for `jjd` either:
     `rayon` appears in exactly two places, both opt-in rather than on any
     path `RepoLoader`/`Transaction`/commit-writing touch. `fix.rs`'s
     `ParallelFileFixer` is a `FileFixer` impl a caller must choose to
     construct (the `fix` capability isn't in `schema/jjd.capnp`), and
     `local_working_copy.rs`'s `rayon::scope` calls are internal to
     `LocalWorkingCopy`, which `SqlWorkingCopy` replaces. So the constraint
     for `jjd` is simply: never construct `ParallelFileFixer` or
     `LocalWorkingCopy` from wasm. Worth a `ParallelFileFixer` sequential
     fallback on wasm32 someday, as a courtesy to other consumers of jj-lib
     as a library, but not before `jjd` needs it;
   - an `IndexStore` backed by SQL or memory (`DefaultIndexStore` still writes
     segment files to a real filesystem);
   - repo assembly through `RepoLoader::new` instead of filesystem paths
     (`ReadonlyRepo::init`/`Workspace::init_with_factories` still call
     `std::fs` directly, regardless of which stores are passed in).
2. **`SqlWorkingCopy`:**
   - `wc_file(workspace, path, rid, exec, symlink, mtime)`;
   - snapshot in O(dirty set);
   - checkout by rewriting rows.
3. **`DoSqlConn`:** `sql.exec` plus `transactionSync` through wasm-bindgen.
   Also a `jjd-worker` DO in TypeScript with capnweb targets, and R2
   hydrate/snapshot.
4. **`jjd` native:** capnp-rpc over a Unix socket or TCP, implementing
   `schema/jjd.capnp`.
5. **Op-store `gc`, and a persisted index.**
