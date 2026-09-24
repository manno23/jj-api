# Copyright 2026 The Jujutsu Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# jjd: the capability surface agents use to drive a jj repository.
#
# The same interfaces are exposed twice:
#   * locally, over capnp-rpc (this file);
#   * on Cloudflare, as capnweb `RpcTarget` classes with identical method
#     names and shapes (`Data` <-> `Uint8Array`, lists <-> arrays).
#
# Design rules:
#   * Capabilities, not paths: you can only reach a Worktree or Fs you were
#     handed, so an agent's authority is exactly the stubs it holds.
#   * Every mutation returns the `OpInfo` of the jj operation it recorded, so
#     callers can pipeline on it and undo precisely.
#   * Ids travel as hex text: commit ids (64 hex digits), change ids in jj's
#     reverse-hex form, operation ids (128 hex digits).
#
# Status: phase-1 design artifact. Nothing implements it yet.

@0xf83581a2c7d33c8c;

# ---- values -----------------------------------------------------------------

struct Signature {
  name @0 :Text;
  email @1 :Text;
  millisSinceEpoch @2 :Int64;
  tzOffsetMinutes @3 :Int32;
}

struct CommitInfo {
  commitId @0 :Text;
  changeId @1 :Text;
  parents @2 :List(Text);        # commit ids
  description @3 :Text;
  author @4 :Signature;
  committer @5 :Signature;
  hasConflict @6 :Bool;
  isEmpty @7 :Bool;
}

struct OpInfo {
  opId @0 :Text;
  parents @1 :List(Text);        # operation ids
  description @2 :Text;
  timeMillis @3 :Int64;
}

enum EntryKind {
  file @0;
  executable @1;
  symlink @2;
  directory @3;
  conflict @4;
}

struct DirEntry {
  name @0 :Text;
  kind @1 :EntryKind;
  size @2 :UInt64;               # 0 for directories and conflicts
}

struct Stat {
  kind @0 :EntryKind;
  size @1 :UInt64;
  contentId @2 :Text;            # the blob uuid (SHA3-256); empty for directories
}

enum ChangeKind {
  added @0;
  modified @1;
  removed @2;
}

struct DiffEntry {
  path @0 :Text;                 # repo-relative, '/'-separated
  kind @1 :ChangeKind;
}

struct Status {
  workingCopy @0 :CommitInfo;
  changes @1 :List(DiffEntry);   # working copy vs. its parent(s)
  conflicts @2 :List(Text);      # paths
}

# ---- capabilities -----------------------------------------------------------

# Entry point returned by the daemon's bootstrap.
interface Harness {
  repo @0 () -> (repo :Repo);
  worktree @1 (name :Text) -> (worktree :Worktree);   # "default" exists
  worktrees @2 () -> (names :List(Text));
}

# Read-mostly view of history.
interface Repo {
  head @0 () -> (op :OpInfo);
  log @1 (revset :Text, limit :UInt32) -> (commits :List(CommitInfo));
  show @2 (rev :Text) -> (commit :CommitInfo);
  diff @3 (from :Text, to :Text, paths :List(Text)) -> (entries :List(DiffEntry));
  opLog @4 (limit :UInt32) -> (ops :List(OpInfo));
  undo @5 () -> (op :OpInfo);
}

# One jj workspace: a working-copy commit and the files in it.
interface Worktree {
  fs @0 () -> (fs :Fs);
  status @1 () -> (status :Status);

  # Records pending Fs writes as the working-copy commit.
  snapshot @2 () -> (commit :CommitInfo, op :OpInfo);

  describe @3 (message :Text) -> (commit :CommitInfo, op :OpInfo);
  # `jj new`: start a new change on top of `parents` (default: the working copy).
  new @4 (parents :List(Text), message :Text) -> (commit :CommitInfo, op :OpInfo);
  # `jj commit`: describe the working copy and start a new change on top.
  commit @5 (message :Text) -> (commit :CommitInfo, op :OpInfo);
  edit @6 (rev :Text) -> (op :OpInfo);
  squash @7 (from :Text, into :Text) -> (op :OpInfo);
}

# The working copy's files. On a Durable Object these are rows pointing at
# blobs, so reads and writes never touch a disk.
interface Fs {
  read @0 (path :Text) -> (data :Data);
  write @1 (path :Text, data :Data, executable :Bool) -> ();
  symlink @2 (path :Text, target :Text) -> ();
  remove @3 (path :Text) -> ();
  rename @4 (from :Text, to :Text) -> ();
  list @5 (dir :Text) -> (entries :List(DirEntry));
  stat @6 (path :Text) -> (stat :Stat);
  glob @7 (pattern :Text) -> (paths :List(Text));
}
