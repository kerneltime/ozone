---
title: Leader-Side Execution — Concurrency and Locking Design
summary: Fine-grained, objectID-keyed locking for OBS and FSO buckets under leader-side execution, with a linearizability correctness model
date: 2026-06-14
jira: HDDS-11898
status: draft (working — expected to evolve)
author: Ritesh Shukla
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Leader-Side Execution — Concurrency and Locking Design

## 0. Status, scope, and relationship to the parent design

This document specifies the **concurrency control** for Ozone Manager (OM) write
operations once request execution moves to the leader (HDDS-11898, parent design
`leader-planned-execution.md`). It is deliberately a **standalone** locking spec
(per review feedback on PR #7583 requesting locking be split out) and covers both
bucket layouts under a single model:

- **OBS** (OBJECT_STORE) — flat namespace.
- **FSO** (FILE_SYSTEM_OPTIMIZED) — hierarchical namespace.

It does **not** re-derive the parent design. It depends on these decisions made in
the parent thread, restated here only as context:

- **D-PARENT-1** The leader executes a request once and replicates a **DB patch**
  (operations over raw bytes) via Ratis. Followers apply the patch with no business
  logic. (Ethan Rose's replicated-DB module: `Put / Delete / Merge / Checkpoint`.)
- **D-PARENT-2** Bucket **quota usage** (`usedBytes`/`usedNamespace`) is a commutative
  RocksDB **merge operator** in the replicated patch — not in-memory reserved state.
- **D-PARENT-3** OM-level table caches are **removed** for migrated commands; reads go
  to RocksDB (OM runs on NVMe; RocksDB block cache suffices). A future caching layer,
  if justified, is a separate post-refactor effort.
- **D-PARENT-4** A new lean lock subsystem is built for this design. The existing
  `OzoneManagerLock` (eight leveled resource types, per-type striped maps, trackers,
  reentrancy) is **not reused** — it is heavier than this design needs and, critically,
  is thread-affine (see I-9).

The locking design below is the contract the parent design's executor must honor.

---

## 1. Motivation and the one hard problem

Today every FSO and OBS write serializes on a single **bucket write lock**
(`BUCKET_LOCK`, striped on `(volume, bucket)`). Two creates into disjoint subtrees of
the same bucket still contend. Leader-side execution lets us replace this with locks
that protect only the **planning** phase, so independent operations run in parallel.

The structural facts that make fine-grained locking possible (all verified against
master):

- **F-1** Every namespace entry is stored under the key
  `/<volumeId>/<bucketId>/<parentObjectID>/<name>` — addressed by its **parent's
  objectID**, never by path. (`OmMetadataManagerImpl.getOzonePathKey`.)
- **F-2** Directory rename is an **O(1) re-parent**: the directory keeps its objectID;
  only its own entry (old key deleted, new key put) and the two parent entries'
  mtimes change. **Children are never touched** because they key on the parent's
  (unchanged) objectID. (`OMKeyRenameRequestWithFSO.renameKey`.)
- **F-3** objectIDs are strictly monotonic and **never reused** (derived from a
  monotonic index). A deleted node's objectID can never alias a later node.
- **F-4** Recursive directory delete is **not** atomic today: it tombstones the root
  and reclaims the subtree asynchronously (`deletedDirTable` + `DirectoryDeletingService`).

**Consequence (the key narrowing):** because of F-2/F-1, an ancestor *rename* leaves
every descendant key valid and correctly parented. Therefore rename is **not** a
cross-cutting conflict, and multiple-granularity (ancestor) locking is over-strict
for Ozone FSO. The **only** prefix-node operation that can invalidate a descendant is
**delete** (orphaning). The entire design reduces to: handle leaf operations with
parent+child locks, and handle exactly one cross-cutting class — *ancestor-delete vs.
descendant-operation*.

---

## 2. The lock model: containers and slots

A node plays two roles, each with its own lock namespace.

- **Container lock** — keyed by a directory's **objectID**. Governs *"what children
  exist under me."* Modes: **S** (shared) = "I am adding/touching one child"; **X**
  (exclusive) = "I am emptying/removing this directory."
- **Slot lock** — keyed by **(parentObjectID, name)** (the DB key minus the
  vol/bucket prefix). Governs *"the existence/identity of this one entry."* Taken
  **X** by whoever creates/commits/deletes/renames that specific entry.

The **bucket** is the root container; it additionally carries bucket-level properties
(quota *limits*, default ACLs/replication/encryption) that every key op reads, so the
bucket lock is taken **S** by every op and **X** by `SetBucketProperty`/`DeleteBucket`.

Lock identity:
- container → `objectID` (immutable; rename-stable per F-2; ABA-free per F-3).
- slot → `(parentObjectID, name)`.

Path-keyed locks are rejected: a rename would invalidate a held path-keyed lock,
whereas an objectID-keyed lock survives rename.

### 2.1 Per-operation lock matrix

`P` = immediate parent directory; `D` = the directory being deleted; `P1/P2` = source/
dest parent of a rename. All locks listed are acquired in the total order of §5.

| Operation | Bucket | Container | Slot | Notes |
|---|---|---|---|---|
| createKey (OBS) | S | — | — | open key carries `clientID`; creates never collide |
| createFile (FSO) | S | S(P) | — | open file carries `clientID`; same as OBS |
| createDir (FSO) | S | S(P) | X(P, d) | dir name has no `clientID` → uniqueness needed |
| commit (OBS) | S | — | X(bucket, key) | two commits of same key serialize |
| commit (FSO) | S | S(P) | X(P, file) | |
| deleteFile | S | S(P) | X(P, file) | |
| deleteDir (empty) | S | S(P) | X(P, d) + **X(D)** | X(D) blocks new children during the empty-check |
| rename | S | S(P1) + S(P2) | X(P1,a) + X(P2,b) | **no X on the moved node** (children untouched, F-2) |
| SetBucketProperty / DeleteBucket | X | — | — | exclusive bucket |

Rationale highlights:
- **createKey/createFile take no slot lock** (only `S(parent)`): the open(key|file)
  table key includes `clientID`, so concurrent creates of the same name by different
  clients write distinct rows and cannot collide. Same-name resolution is deferred to
  commit (`X(parent, name)`). This preserves "creates never block creates."
- **rename takes no container lock on the moved node.** Renaming `D` changes `D`'s own
  entry and the two parents' mtimes; `D`'s children key on `D`'s unchanged objectID and
  are untouched, so a concurrent create *under* `D` (which holds `S(D)`) need not
  conflict with the rename. The slot `X(P1, a)` is the rendezvous for rename-vs-delete
  of `D`; the container `X(D)` (held only by delete-empty) is the rendezvous for
  create-under-`D`-vs-delete-`D`.

---

## 3. Invariants

- **I-1 (lock identity).** Container locks are keyed by objectID; slot locks by
  `(parentObjectID, name)`. Never by path.
- **I-2 (hold span).** A lock is held on the **leader** from before the Ratis submit
  until after **quorum commit and local apply** of that step. Locking happens only on
  the leader; followers apply the replicated patch in leader-dictated order and take no
  OM locks.
- **I-3 (no lock across the gate).** In a multi-step operation (§4), locks are taken and
  released **per step**. The inter-step gap is intentional; correctness across the gap is
  provided by I-5/I-6/I-7, not by holding a lock.
- **I-4 (creates don't serialize).** create(Key|File) takes no slot lock; concurrent
  creates of the same name (distinct clients) both succeed at create time and are
  resolved at commit.
- **I-5 (FSO-RESOLVE-FAIL).** Once a directory is tombstoned, any path resolution that
  traverses it fails (`DIRECTORY_NOT_FOUND`). New descendant operations therefore cannot
  *begin* after the tombstone; only a bounded set of already-resolved in-flight
  operations remain.
- **I-6 (FSO-REVAL).** After acquiring its locks, every operation re-reads each locked
  node by `(parentObjectID, name)` (using parent objectIDs captured during resolution)
  and confirms it still exists with the expected objectID before mutating. This closes
  the window between path resolution and lock acquisition. ABA-safe by F-3.
- **I-7 (FSO-PURGE).** To remove a directory node, the purge takes `X(node)` + its slot,
  enumerates current children **under that lock**, and removes the node only when
  childless; a late child (from an in-flight create that resolved before the root
  tombstone) is processed before the node is removed. Guarantees **no orphan**.
- **I-8 (no holder lease — correctness-critical).** A held lock is **never** revoked from
  an in-flight holder. Lock release happens only in the holder's own completion path
  (commit or failure). A lease that expired a held lock while its Ratis op was still in
  flight would let a waiter and the original holder both mutate the protected state — a
  mutual-exclusion violation. Therefore **the lock has no timeout of its own** (see B-3).
- **I-9 (non-thread-affine).** Because orchestration is asynchronous (the worker thread
  is released during the Ratis await) and I-2 holds the lock across that await, the lock
  primitive must be releasable on a different thread than acquired. `ReentrantReadWriteLock`
  is therefore **disqualified**; the primitive is owned by the request handle, not the
  thread.
- **I-10 (leader-local).** Locks live only in the leader's memory. On leader change the
  lock table is discarded; the new leader applies the committed Ratis log as plain
  deterministic DB writes (no OM locks) and then serves new requests under fresh locks.
  No lock survives failover; nothing leaks.
- **I-11 (deadlock-free by total order).** Every operation acquires its full lock set in
  one fixed total order and releases in reverse. The order is uniform across all
  operations, so no cycle can form. The order does **not** rely on tree structure
  (which rename can invert per F-2); it relies only on a stable comparator over lock
  identities (§5).
- **I-12 (cache-free correctness).** Read-your-writes is provided by I-2 (a same-key
  successor blocks until the predecessor's bytes are in RocksDB) — not by any in-memory
  cache (D-PARENT-3). No correctness property may depend on a cache.

---

## 4. Multi-step operations (orchestration)

Two operations are not single transitions; the leader orchestrates them as an **ordered
chain of dependent Ratis transitions**, each with its own per-step lock acquisition
(I-3). This generalizes the parent design's request contract from "one request → one
transition" to "one request → an ordered chain."

### 4.1 Implicit directory creation (createFile with missing parents)

`createFile /a/b/c/file` with `/a/b/c` missing is decomposed, **OM-internally**, into:
`create b` → await commit → `create c` → await commit → `create open-file`. The client
sends one request with the full path; the OM gates the sub-creates (each needs the
previous one's committed objectID to resolve the next parent).

- **Non-atomic** (B-2): a failure after creating `b`,`c` leaves empty directories. This
  matches `mkdir -p` semantics, is idempotent on retry (`create dir` on an existing dir
  is a no-op), and is low-harm. Accepted for V1.
- **objectID-window retirement:** because each created object is now its own transition,
  each gets exactly one managed index → one objectID. The 256-wide recursive-dir objectID
  window (`(epoch<<62)|(txId<<8)|offset`) is **retired** for migrated commands.
- **Asynchronous** (I-9 driver): the leader registers a continuation on each sub-op's
  commit future and frees the execution thread during the await; it holds the *client RPC*
  open, not a worker thread.

### 4.2 Recursive directory delete

`rm -rf /a/b` is: **synchronous** tombstone of the root `b` (so I-5 makes new descending
resolutions fail immediately, fast UX), plus a **decomposed, per-node-locked background
purge** honoring I-7. Each node removal takes that node's `X(container)` + slot, so a
descendant operation meets the purge at exactly the node they contend over — no prefix
locking. Quota release is **lazy** (subtree bytes/namespace are freed as the purge
drains) — see §8.

### 4.3 Recovery

Recovery is **client-retry-driven**, with no persisted orchestration/saga state
(decision A). A mid-orchestration leader crash leaves committed sub-steps durable on the
quorum and the client's RPC unanswered; the failover retry re-runs the whole request,
idempotently skipping already-created dirs and completing. The client retry-cache entry
(`clientId#callId → response`) is written by the **terminal** step only; intermediate
sub-steps are idempotent by structure and need no retry-cache entry. This is
semantically correct for the filesystem protocol; durable fire-and-forget can be added
later if needed.

---

## 5. Deadlock avoidance (acquisition order)

All locks for an operation are sorted by a single total order and acquired in that order;
released in reverse (I-11). The comparator over lock identities:

1. by the lock's primary id — `objectID` for a container, `parentObjectID` for a slot
   (note the bucket, as the lowest objectID, sorts first);
2. tiebreak container-before-slot when ids coincide;
3. tiebreak slots by `name`.

Multi-slot operations (`DeleteKeys`, `RenameKeys` batch) sort all target slots by this
comparator before acquiring (the lean equivalent of Guava `Striped.bulkGet` ordering).
Because the lock manager is striped (§6), the *effective* order is by **stripe index**,
and two distinct keys colliding on one stripe dedup to a single acquisition at the
**strongest** mode required.

This is deadlock-free purely by uniform total ordering — explicitly **not** by an
ancestor-first rule, which F-2 (rename) can invert.

---

## 6. The lock manager (lean implementation)

A **fixed striped array** of non-thread-affine RW primitives. No resource hierarchy, no
per-type maps, no tracker, no refcount, no timer.

- **Striped, not per-key.** A key hashes to a stripe index. With ~hundreds of locks held
  at steady state and a generously-sized array (B-1), false-contention collisions are a
  handful at peak and cost only latency, never correctness. Striping avoids the per-op
  `ConcurrentHashMap.compute()` + allocation + refcount overhead of a dynamic map — which
  would be pure cost on a hot parent directory (where real contention exists anyway and
  the map can't help).
- **Non-thread-affine primitive** (I-9): a fair semaphore used as an RW lock — **S** =
  acquire 1 permit, **X** = acquire N permits (N = the array's per-stripe permit ceiling).
  Releasable on any thread; fairness prevents writer starvation.
- **Handle-owned release.** `acquire(sortedReqs)` returns a handle; the op releases it in
  its completion path (commit or failure), possibly on the continuation thread.
- **No lock timeout** (I-8). The timeout that matters lives on the **Ratis request**
  (existing). When the Ratis op times out or errors, the holder fails and releases the
  lock in its `finally`. A waiter simply waits — Ratis guarantees the holder commits-or-
  fails, so the wait is bounded by real contention; an OM that is genuinely hung is
  handled by failover (I-10) + the client RPC timeout, not by a lock timer. (This
  relocates Sumit's `obs-locking.md` "timeout required" to the correct layer.)

Sketch:
```
acquire(sortedReqs) -> Handle:                 // reqs pre-sorted (§5), deduped by stripe to strongest mode
  for (stripeIdx, mode) in sortedReqs:
     sem = stripes[stripeIdx]
     sem.acquire(mode == SHARED ? 1 : N)       // blocks until available; no timeout
  return Handle(acquired)
release(Handle):                               // any thread
  for (stripeIdx, mode) in reverse(acquired):
     stripes[stripeIdx].release(mode == SHARED ? 1 : N)
```

---

## 7. Correctness criterion and test plan

The design **allows** many interleavings and treats "an error under one ordering" as a
legal outcome, so example-based "op X always succeeds" tests are invalid. The bar is
**linearizability**: every observed concurrent history is equivalent to *some* legal
sequential order consistent with real-time precedence.

Test architecture:

1. **Sequential reference model** — a single-threaded in-memory filesystem (path→node
   with objectIDs) implementing identical op semantics and error conditions. The oracle
   of "what is legal."
2. **Concurrent harness** — randomized concurrent op mixes against the real lock +
   execution path, recording each op's invocation/response interval and the final DB
   state. Generation is weighted toward the adversarial pairs (T-1..T-6).
3. **Linearizability checker** — a Wing-Gong / Lincheck-style search verifying the
   recorded history is linearizable against the reference model. Accepts any legal order,
   including orders in which an op legitimately errors.
4. **Invariant assertions** layered on top, each mapped to §3 (traceability §9).

Scope of the bar (§8): namespace operations are **strictly linearizable**; quota usage
and subtree reclamation are **eventually consistent** and the checker treats them as
"converges after background work drains," not "correct at every linearization point."

### Test scenarios

- **T-1 create-under-ancestor-being-rm-rf'd** — `create /a/b/c/file` || `rm -rf /a/b`.
  Asserts: no orphan (I-7); outcome is linearizable (create-before-delete, or create
  fails via I-6); no leak.
- **T-2 already-purged-parent** — create whose immediate parent is purged between
  resolution and lock acquisition. Asserts I-6 fails the op cleanly.
- **T-3 two crossing renames** — `rename /a/x→/b/y` || `rename /b/p→/a/q`. Asserts no
  deadlock (I-11; bounded completion) and linearizable result.
- **T-4 delete-empty vs create-child** — `rmdir /a/d` || `create /a/d/f`. Asserts the
  `X(d)`/`S(d)` rendezvous yields a linearizable order (either rmdir-then-create-fails or
  create-then-rmdir-fails-NOT_EMPTY).
- **T-5 deep mkdirs vs ancestor rename** — `createFile /a/b/c/d/file` (missing chain) ||
  `rename /a/b→/a/z`. Asserts children follow the renamed objectID (F-2) and the chain
  either completes under the new location or fails via I-6, linearizably.
- **T-6 rename vs delete of the same node** — `rename /a/d→/a/e` || `delete /a/d`.
  Asserts the `X(P,d)` slot rendezvous yields exactly one winner.
- **T-7 hot-parent throughput** — N concurrent creates under one directory. Asserts
  `S(parent)` allows them to proceed concurrently (no false serialization) and commits
  serialize per-key only.
- **T-8 leader failover mid-orchestration** — crash after committing some sub-dirs of a
  `createFile`. Asserts client retry completes idempotently; no orphan; no double-apply
  (retry-cache on terminal step).

---

## 8. Bounds and explicit exceptions

- **B-1 (stripe array size).** ~2^20 stripes (tunable). At ~64 B per primitive ≈ 64 MB;
  expected false-contention collisions in the low tens at peak in-flight, each costing
  one extra hold-time wait. Sized for burst, not steady state; revisit if a profile shows
  collision tail latency.
- **B-2 (createFile non-atomicity).** A failed multi-step `createFile` may leave empty
  intermediate directories. Bounded, idempotent on retry, reusable. Not cleaned eagerly.
- **B-3 (no lock timeout).** Locks have no timer (I-8). The Ratis request timeout +
  release-on-completion + failover (I-10) bound any wait.
- **EXC-1 (lazy quota release).** `rm -rf` releases quota as the background purge drains,
  not synchronously. Eventually consistent. Synchronous release would require a maintained
  subtree-size aggregate on every create/commit/delete up the chain — filed as a separate
  enhancement.
- **EXC-2 (eventual subtree purge).** Recursive-delete subtree reclamation is async; the
  namespace root disappears synchronously (I-5) but descendants are reclaimed over time.
- **EXC-3 (soft quota / bucket quota over-commit) — KNOWN, ACCEPTED LIMITATION.** Because
  key commits take only a **shared** bucket lock (so commits to different keys run in
  parallel — the core throughput goal), and bucket `usedBytes` is updated by a commutative
  merge operator (D-PARENT-2) rather than read-modify-written under an exclusive lock, the
  quota *limit* is enforced **best-effort, not exactly**: two (or N) commits in flight can
  each pass the quota check against the same pre-increment `usedBytes` and then both apply,
  transiently over-committing the limit by up to the in-flight commit count. This is a
  deliberate trade vs. today's exact (bucket-write-lock) quota, which serialized all commits.
  - **What is still guaranteed:** the `usedBytes` counter never loses an update — it always
    equals the true committed size (no double-count, no lost decrement). Only the *limit
    gate* is soft. (Formally: invariant `UsedConsistent` holds; only refinement against an
    *exact*-quota oracle fails.)
  - **Evidence:** mechanically reproducible — `QuotaOvercommit.cfg` checks the model against
    `ObsAbstractExact` and TLC returns the counterexample (two commits plan at `used=0`, both
    apply, `used=2 > limit=1`). The accepted oracle `ObsAbstract` models quota as soft and
    the model refines it (green).
  - **Mitigation (separate effort, out of scope for this design):** exact enforcement is
    delegated to (a) the existing background `QuotaRepair` reconcile, and/or (b) a future
    leader-local atomic reservation (atomic check-and-reserve in memory, DB merge remains the
    durable truth, decrement-on-abort, rebuild-from-DB on failover). Tracked separately.

These exceptions are stated so a reviewer reads them as deliberate, not as gaps.

---

## 9. Traceability (invariant/bound → test)

| Item | Validated by |
|---|---|
| I-2 hold span | T-4, T-6 (same-node exclusion holds to commit) |
| I-4 creates don't serialize | T-7 |
| I-5 resolve-fail | T-1, T-5 |
| I-6 reval | T-2, T-5 |
| I-7 no orphan | T-1 |
| I-8 no holder lease | (design constraint; checked by absence of any lease path + T-8 no double-apply) |
| I-9 non-thread-affine | (unit test: acquire on thread A, release on thread B) |
| I-10 leader-local | T-8 |
| I-11 deadlock-free | T-3, T-5 (bounded completion = no deadlock; no lock timeout means a hang *is* the signal) |
| I-12 cache-free RYW | T-7 (successor reads predecessor's committed bytes) |
| B-1 stripe sizing | T-7 (throughput under hot parent) |
| EXC-1/EXC-2 | linearizability checker treats quota/purge as eventually-consistent |
| EXC-3 soft quota | `UsedConsistent` holds (counter exact); `QuotaOvercommit.cfg` reproduces the over-commit vs the exact oracle (TLA+ model `ObsImpl`) |

---

## 10. Open items (to refine as we proceed)

- Multipart upload (FSO/OBS) lock placement and the large-value (HDDS-8238) concern.
- hsync / lease-recovery interaction with `X(parent, file)` on commit.
- Exact merge-operator interaction for FSO directory mtime updates on cross-parent rename.
- Snapshot (`Checkpoint` op) ordering vs. in-flight fine-grained ops.
- `SetAcl`/`SetTimes`/`AllocateBlock` placement (expected: `S(bucket)+S(P)+X(P,name)`).
- Volume-lock escape hatch: none today (no volume rename); builder-extensible if a future
  volume op can invalidate an in-flight key op.
- **Retry / idempotency mechanism — DEFERRED (parent-level decision; gates the framework's
  terminal-step retry-cache contract).** The choice between (a) a durable, replicated
  `(clientId, callId) → response` table written **atomically with the data batch** plus a
  leader-local in-flight registry, versus (b) in-memory-only retry state, is **deferred
  pending a thorough per-operation audit that classifies every OM write operation as
  idempotent or non-idempotent under client retry/replay.** The non-idempotent set — any op
  using a commutative `Merge` (quota), SCM block allocation, table moves, or soft-delete —
  is what dictates which operations require the atomic durable retry entry: a re-planned
  non-idempotent op double-applies (e.g. double-counts quota). Naturally-idempotent pure
  `Put`/`Delete` ops may tolerate weaker handling. No retry mechanism is fixed until this
  operation-by-operation idempotency audit exists. (This note lives here for now; it belongs
  in the parent `leader-planned-execution.md` once that design doc is reorganized.)
