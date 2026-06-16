---
title: Leader-Side Execution — Component Designs
summary: The twelve buildable units of the leader-side execution model — replicated-DB module, managed index, lean lock manager, orchestrator, planned-request base, dual-path state machine, layout gate, quota merge operator, retry (deferred), per-command subclasses, linearizability harness, and late legacy removal — each with its C-n block, the exact seam to existing code, and traceability to the master spec invariants, tests, and phases.
date: 2026-06-15
jira: HDDS-11898
status: draft
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

# Leader-Side Execution — Component Designs

## 0. Scope and how to read this companion

This file is the **implementation-facing** companion to the master spec
`leader-planned-execution.md` (referenced from master **§11. Component designs**). The
master holds the *why* (the D-n rationale spine, the I-n correctness contract, the P-n
delivery plan); the locking companion `leader-execution-locking.md` holds the *concurrency
model* (container/slot locks, the linearizability bar, I-1..I-12). This file holds the
*twelve buildable units*: for each one, a `C-n` structured block, dense prose, and — the
load-bearing part — the **exact file and method to add or modify** in the worktree, cited to
`file:line`.

Three reading rules carried from §A of the master:

1. **Every load-bearing claim cites evidence** and is tagged `verified` (a `file:line` / PR# /
   reviewer resolved against this worktree) or `inferred` (a design choice not yet in code).
   Where a component is net-new, the cited anchor is the *seam* it attaches to — the existing
   method whose body or call site changes — not the new code (which does not exist yet).
2. **The `C-n` block is the contract**; the prose explains it. The block's `implements`,
   `tests`, `depends_on`, `phase`, and `anti_patterns` are consumed by the CI `lint-spec`
   projection (master §C). If an `I-n` is listed in no `C-n.implements`, that is a spec defect.
3. **Locked decisions are honoured, never re-litigated.** This file consumes
   D-1..D-16, D-SPEC-1..3 and the two OPEN decisions (D-OPEN-quota-enforcement,
   D-OPEN-retry) exactly as recorded in the master. Where a component touches an open
   decision, it states the open shape and marks it open — it does not silently settle it.

### 0.1 The one-paragraph mental model the twelve components assemble

The leader takes a client write, runs the business logic **once** under fine-grained locks
(C-lock-manager), and emits a deterministic **DB patch** — a `Batch` of `Put / Delete /
Merge / Checkpoint` operations over raw bytes (C-replicated-db-module). That patch is
wrapped in an OM envelope carrying a **managed index** (C-managed-index), the client
retry-info, and the legacy `OMResponse`, then replicated through Ratis. On **every** node
(leader included, for symmetry) the state machine (C-state-machine-dualpath) applies the
inner `Batch` byte-for-byte with **zero business logic** — followers never re-run
`validateAndUpdateCache`. A request that needs more than one transition (createFile with
missing parents, recursive delete) is driven as an **ordered chain** by the orchestrator
(C-orchestrator), with the chain state owned by the request (C-planned-request,
C-command-subclass). Quota is the one mutable counter that two parallel commits can race on;
it is expressed as a commutative **Merge** resolved at apply time (C-merge-operator). The
whole feature is inert until **finalization** flips an OM layout feature (C-layout-feature)
and a per-command runtime flag opts the path in (D-14). Correctness is proved by a
**linearizability harness** against a sequential reference model (C-test-harness). Only after
every command has migrated does the **double buffer and the table cache get removed**
(C-legacy-removal) — late, deliberately, because that removal is irreversible and the cache
is load-bearing for the legacy path that coexists during the long mixed-mode window.

### 0.2 Component → phase → decision quick index

| C-n | Component | Phase | Anchors / settles |
|---|---|---|---|
| C-replicated-db-module | `Batch`/`Operation` proto + apply engine in `hadoop-hdds/framework` | P-0 | D-1, D-2 |
| C-managed-index | `ManagedIndexService` + `#MANAGED_INDEX` persistence | P-0 | D-8, D-12, D-11 |
| C-lock-manager | Lean striped semaphore-RW lock manager | P-0 | D-4, D-5, D-15 (model in locking companion) |
| C-orchestrator | `LeaderPlanner` + dynamic step-iterator driver | P-0 / P-2 | D-6, D-16 |
| C-planned-request | `PlannedRequest` base + change recorder | P-0 | D-6, D-1 |
| C-state-machine-dualpath | dual-path apply in `OzoneManagerStateMachine` | P-0 | D-10, D-14, D-11 |
| C-layout-feature | `OMLayoutFeature.LEADER_SIDE_EXECUTION` | P-0 | D-11 |
| C-merge-operator | Option-B quota Merge operator | P-1 | D-7, D-OPEN-quota-enforcement |
| C-retry | idempotency / retry-cache (DEFERRED) | P-1+ | D-OPEN-retry |
| C-command-subclass | per-command `PlannedRequest` (CreateKey/CommitKey worked) | P-1 | D-6, D-7 |
| C-test-harness | linearizability harness + reference model | P-0..P-2 | D-10, D-16 |
| C-legacy-removal | late removal of double buffer + table cache | P-7 | D-3 |

---

## C-replicated-db-module — domain-agnostic `Batch`/`Operation` proto and apply engine

```yaml
id: C-replicated-db-module
target_files:
  - hadoop-hdds/framework/src/main/proto/  # NEW: ReplicatedDbProtocol.proto (Batch, Operation{Put,Delete,Merge,Checkpoint})
  - hadoop-hdds/framework/src/main/java/org/apache/hadoop/hdds/utils/db/replicated/  # NEW package: PersistDbBatch, OperationApplier, MergeOperatorRegistry
  - hadoop-hdds/framework/src/main/java/org/apache/hadoop/hdds/utils/db/BatchOperation.java  # existing target the applier writes through
interface: |
  // proto (inner, domain-agnostic — bytes only):
  message Operation { enum Kind { PUT=1; DELETE=2; MERGE=3; CHECKPOINT=4; }
    Kind kind = 1; bytes column_family = 2; bytes key = 3; bytes value = 4; bytes merge_operator_id = 5; }
  message Batch { repeated Operation ops = 1; }
  // apply engine (runs on every node; NO Ozone types imported):
  final class OperationApplier {
    void apply(Batch batch, DBStore store, BatchOperation rocksBatch, MergeOperatorRegistry registry);
  }
depends_on: [D-1, D-2, F-9]
implements: [I-inner-domain-agnostic, I-txninfo-atomic-with-patch]
tests: [T-proto-roundtrip, T-determinism-follower-byte-identical]
anti_patterns:
  - "MUST NOT import any org.apache.hadoop.ozone.om.* type into the inner proto or the applier — that re-creates the per-command divergence this design exists to kill (ALT-journal-not-dbchanges)."
  - "MUST NOT deserialize a domain object (OmKeyInfo, OmBucketInfo) inside apply — apply sees only (cf, key, value) bytes."
  - "MUST NOT extract this to its own Maven module yet (ALT-module-extraction-now killed by D-2); lock the API surface in framework first."
  - "MUST NOT add a Kind beyond {Put,Delete,Merge,Checkpoint}; raw-put/delete-only (ALT-raw-putdelete-only) cannot express quota or the snapshot barrier and is rejected."
phase: P-0
provenance: inferred
evidence:
  - "D-1/D-2 locked (master §20); PR#7583 review (errose28 'Put/Delete/Merge/Checkpoint module'), nandakumar131 +1"
  - "seam: OzoneManagerDoubleBuffer.flushBatch writes via BatchOperation today — hadoop-ozone/.../ratis/OzoneManagerDoubleBuffer.java:354,364-381"
  - "atomic #TRANSACTIONINFO commit pattern to mirror — OzoneManagerDoubleBuffer.java:373-376"
```

This is the substrate every other component sits on. It defines the **wire format of a write**
and the **engine that applies it**, both deliberately ignorant of Ozone.

**Two-layer proto (D-2).** The *inner* `Batch` is domain-agnostic: a `repeated Operation`
where each `Operation` is one of `PUT / DELETE / MERGE / CHECKPOINT` over `(column_family,
key, value)` byte fields, plus a `merge_operator_id` selecting the resolver for `MERGE`. The
*outer* OM envelope (defined alongside the OM protocol, not here) wraps the inner batch with
the managed index (C-managed-index), the `ClientRequestInfo[]` for retry (C-retry), and the
legacy `OMResponse`. The hard invariant — **I-inner-domain-agnostic** — is that the inner
layer never deserializes a domain object. `apply` sees bytes, writes bytes. This is what makes
followers business-logic-free and what lets Recon-as-listener, follower-reads, and an eventual
SCM reuse "fall out for free" (D-2 consequences). Per D-2 the module **stays in
`hadoop-hdds/framework`** — locking the API surface there now, with extraction to its own Maven
module deferred to a later no-behavior-change PR (ALT-module-extraction-now is killed, not
forgotten).

**The apply engine and its seam.** Today the sole RocksDB writer is the double buffer:
`OzoneManagerDoubleBuffer.flushBatch` opens a `BatchOperation`, calls each response's
`checkAndUpdateDB` to stage Puts/Deletes
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerDoubleBuffer.java:354,364-381`),
appends `#TRANSACTIONINFO` to the **same** batch
(`OzoneManagerDoubleBuffer.java:373-376`), and commits atomically. The new `OperationApplier`
mirrors exactly that shape — it iterates `Batch.ops`, translates each `Operation` into a
`BatchOperation` mutation (`PUT`/`DELETE` direct; `MERGE` via the registered resolver, see
C-merge-operator; `CHECKPOINT` triggers the snapshot barrier that
`splitReadyBufferAtCreateSnapshot` provides today,
`OzoneManagerDoubleBuffer.java:340`), and writes the managed index into the same
`BatchOperation` so that **the patch and the index advance atomically** (I-txninfo-atomic-with-patch).
The applier reuses the existing `BatchOperation` abstraction
(`hadoop-hdds/framework/src/main/java/org/apache/hadoop/hdds/utils/db/BatchOperation.java`) so
the durability and atomicity guarantees the double buffer relies on transfer unchanged.

**Why DB-changes and not a journal (ALT-journal-not-dbchanges, ALT-raw-putdelete-only).**
xichen01 asked whether replicating a *journal of commands* would preserve more future
flexibility (FSO inode trees). It was rejected (D-1): abstract-command apply is precisely the
per-command-unique step that causes today's silent divergence — every follower re-deriving the
same bytes is the bug surface. Replicating the *bytes themselves* is how consensus systems
normally work and is what makes byte-identity (T-determinism-follower-byte-identical) checkable.
Raw-put/delete-only was also rejected because it cannot express the commutative quota counter or
the snapshot barrier — `#10503` regressed to that shape and lost both.

**Failure atomicity at this seam.** The applier holds no lock and performs no network I/O; its
only failure mode is a RocksDB write error, which is exactly today's `flushBatch` failure mode
and follows the same fail-stop discipline (`terminate` on a write that cannot be applied,
`OzoneManagerStateMachine.java:498-505`). Because the index is in the same atomic batch, there
is no "patch applied, index not advanced" partial state — either both land or neither does.

---

## C-managed-index — `ManagedIndexService`, `#MANAGED_INDEX` persistence, finalize-seed, switchover max()

```yaml
id: C-managed-index
target_files:
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/  # NEW: ManagedIndexService.java
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OzoneManager.java  # modify getObjectIdFromTxId(...) source (line 2380)
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OmMetadataManagerImpl.java  # NEW table accessor for #MANAGED_INDEX (alongside getTransactionInfoTable line 1698, getOmEpoch line 730)
  - hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/OmUtils.java  # objectID encoding reused unchanged (getObjectIdFromTxId line 766)
interface: |
  final class ManagedIndexService {
    long next();                 // AtomicLong.getAndIncrement; the per-object index
    long current();              // peek without advancing
    void seedFromFinalization(long ratisIndexAtFinalize);  // managed = max(ratisIndex)+1 at first finalize
    void onBecomeLeader(long lastAppliedRatisIndex);        // switchover: managed = max(managed, ratisIndex)+1
    void persist(BatchOperation batch);                     // #MANAGED_INDEX into the same atomic batch as the patch
  }
depends_on: [D-8, D-12, D-11, C-replicated-db-module]
implements: [I-managed-index-monotonic, I-objectid-disjoint]
tests: [T-objectid-disjoint, T-mixed-mode-no-collision, T-managed-index-monotonic]
anti_patterns:
  - "MUST NOT derive objectID from the Ratis log index on the new path while the legacy path still does (mixed-mode collision — D-12); BOTH paths must source from this counter during mixed mode."
  - "MUST NOT change the objectID bit layout (epoch<<62 | index<<8) — every tool that decodes objectIDs depends on it (D-8); the low 8 bits go dead-zero, the format is preserved."
  - "MUST NOT advance the counter for a Merge-only or pure-property op that allocates no new object (no wasted index; but monotonicity, not density, is the invariant)."
  - "MUST NOT seed the counter below max(committed Ratis index) at finalize/switchover — an index <= a previously-used Ratis-derived objectID would re-mint a live id."
phase: P-0
provenance: inferred
evidence:
  - "objectID encoding to reuse — OmUtils.getObjectIdFromTxId / addEpochToTxId, hadoop-ozone/common/.../OmUtils.java:766-783; shifts TRANSACTION_ID_SHIFT=8, EPOCH_ID_SHIFT=62 at OmUtils.java:95,101,103"
  - "current source of objectID from txn index — OzoneManager.getObjectIdFromTxId, hadoop-ozone/ozone-manager/.../OzoneManager.java:2380-2383 (calls metadataManager.getOmEpoch())"
  - "epoch accessor — OmMetadataManagerImpl.getOmEpoch, hadoop-ozone/ozone-manager/.../OmMetadataManagerImpl.java:730; persistence-table exemplar getTransactionInfoTable line 1698"
  - "D-8/D-12 locked (master §20); PR#7583 'use managed index in both flows'; grill catch 2026-06-15"
```

The managed index is the **single monotonic counter that mints objectIDs and updateIDs** once
execution moves to the leader. It exists to sever the welding of `objectID` to the Ratis log
index — today `OzoneManager.getObjectIdFromTxId(trxnId)` takes the Ratis transaction index and
runs it through the encoding
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OzoneManager.java:2380-2383`),
and the recursive-directory case carves a 256-wide window out of that index via the low 8 bits
(`OmUtils.java:95` `TRANSACTION_ID_SHIFT = 8`). In the new model each created object is its own
transition, so each gets **exactly one** managed index and the 256 window is retired (D-8;
locking companion §4.1).

**The encoding is reused verbatim (D-8).** `OmUtils.getObjectIdFromTxId(epoch, index)` and its
helper `addEpochToTxId` (`OmUtils.java:766-783`) are untouched: `objectID = (epoch << 62) |
(index << 8)`. The only change is the *source* of `index` — `ManagedIndexService.next()` (an
`AtomicLong.getAndIncrement`) instead of the Ratis log index. The low 8 bits go dead-zero
(format continuity preserved for every diagnostic tool that decodes objectIDs). The bound
**B-managed-index-max** = `MAX_TRXN_ID = (1L<<54)-2` (`OmUtils.java:103`) is inherited
unchanged: the 54-bit index space is identical to today's, just consumed one-per-object instead
of one-per-Ratis-entry.

**`#MANAGED_INDEX` persistence (D-11, additive).** The counter's durable value is a new
metadata-table key, written **into the same `BatchOperation`** as the data patch and the
`#TRANSACTIONINFO` advance (the atomic triple — patch + transaction info + managed index land
together or not at all; mirrors `OzoneManagerDoubleBuffer.java:373-376`). The accessor sits
alongside `OmMetadataManagerImpl.getTransactionInfoTable()`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OmMetadataManagerImpl.java:1698`).
This entry is **additive and inert** until finalization (D-11): an old binary that has not
finalized never reads or writes it, so a new-leader → old-follower patch carries it harmlessly.

**Finalize-seed and the mixed-mode disjointness proof (D-12 → D-8).** This is the subtle part
and the reason D-12 is a Phase-0 prerequisite **before any command migrates**. During the long
rolling-upgrade window, the legacy path mints objectIDs from the Ratis index and the new path
would mint from the managed counter; if the two ranges overlapped, two distinct objects could
collide on one objectID. The fix (D-12): **both** paths draw from the managed counter during
mixed mode — the legacy `validateAndUpdateCache` is retrofitted so its `getObjectIdFromTxId`
call routes through `ManagedIndexService.next()` rather than the raw txn index. At first
finalization the counter is **seeded to `max(committed Ratis index) + 1`**
(`seedFromFinalization`), which makes the new-path range start strictly above every
Ratis-derived objectID ever issued — **disjoint by construction** (I-objectid-disjoint,
T-objectid-disjoint). The seed is read from the finalize hook
(`OMLayoutVersionManager.finalized`,
`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/upgrade/OMLayoutVersionManager.java:140`).

**Switchover max() (I-managed-index-monotonic).** On leader change the in-memory counter is
rebuilt: `onBecomeLeader` sets it to `max(persisted #MANAGED_INDEX, lastAppliedRatisIndex) + 1`.
The `max` is defensive — even if a follower applied entries past the last persisted counter
value, the new leader never re-mints an index at or below anything already committed. No managed
index survives as live in-memory state across failover except via this rebuild-from-DB; the
counter is leader-authoritative and durably anchored.

---

## C-lock-manager — lean striped semaphore-RW lock manager (model in the locking companion)

```yaml
id: C-lock-manager
target_files:
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/execution/lock/  # NEW package: StripedSemaphoreLockManager, LockHandle, LockRequest
interface: |
  final class StripedSemaphoreLockManager {
    LockHandle acquire(List<LockRequest> sortedDedupedReqs);  // §5 total order; blocks; NO timeout (I-8)
    // LockRequest = (stripeIndex, Mode{SHARED,EXCLUSIVE}); SHARED=1 permit, EXCLUSIVE=N permits (D-15)
  }
  interface LockHandle extends AutoCloseable {
    void release();   // releasable on ANY thread (I-9); reverse order; idempotent
  }
depends_on: [D-4, D-5, D-15, F-12]
implements: [I-9, I-11, I-8, I-2, I-mixed-mode-lock-gate]
tests: [T-cross-thread-release, T-3, T-7, T-hot-stripe, T-holder-lease-negative]
anti_patterns:
  - "MUST NOT let a migrated command skip the existing OzoneManagerLock bucket lock during mixed mode (D-17 I-mixed-mode-lock-gate) — without the shared gate it races legacy commands on the same key."
  - "MUST NOT use ReentrantReadWriteLock (thread-affine; cannot release on the continuation thread — I-9). OzoneManagerLock is built on it and is therefore disqualified for reuse (ALT-reuse-ozonemanagerlock)."
  - "MUST NOT reuse OzoneManagerLock's 8 leveled resources / per-type striped EnumMaps / trackers / reentrancy — heavier than this design needs (D-4)."
  - "MUST NOT add a per-lock acquisition timeout / holder lease (ALT-lock-timeout killed by D-5; I-8). The Ratis request timeout + release-on-completion + failover are the only bounds (B-3)."
  - "MUST NOT acquire ancestor (prefix) locks — rename is O(1) re-parent (locking F-2) so ancestor-first ordering can invert (ALT-mgl-ancestor-locking). Order is by stripe index only (locking §5)."
  - "MUST NOT do any DNS/RPC/disk I/O while holding a stripe permit (no I/O under lock)."
phase: P-0
provenance: inferred
evidence:
  - "FULL MODEL: leader-execution-locking.md §6 (lean implementation), §2 (container/slot), §3 (I-1..I-12), §5 (acquisition order), §8 (B-1 stripe size, B-3 no timeout)"
  - "OzoneManagerLock IS thread-affine (built on ReentrantReadWriteLock) — hadoop-ozone/ozone-manager/.../om/lock/OzoneManagerLock.java:39 (import), :158-162 (getLock returns ReentrantReadWriteLock); 8-resource leveled model EnumMap<LeveledResource>/EnumMap<DAGLeveledResource> at :117,:126"
  - "today key ops serialize on BUCKET_LOCK write lock — OMKeyCommitRequest.java:191-194 (acquireWriteLock BUCKET_LOCK)"
```

**This component is the *implementation*; the *model* is the locking companion's contract — do
not re-derive it here.** `leader-execution-locking.md` defines what is locked (container locks
keyed by directory objectID, slot locks keyed by `(parentObjectID, name)`, §2), the per-op lock
matrix (§2.1), the twelve invariants I-1..I-12 (§3), the deadlock-free total order (§5), and the
bounds (§8). This section specifies only the **primitive and its API**.

**Why not reuse `OzoneManagerLock` (D-4, verified).** Two disqualifiers, both confirmed in
source. (1) **Thread-affinity.** `OzoneManagerLock` is built on `ReentrantReadWriteLock`
(import at
`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/lock/OzoneManagerLock.java:39`;
`getLock` returns one at `OzoneManagerLock.java:158-162`). A reentrant lock can only be released
by the thread that acquired it. But this design holds a lock **across the Ratis await** and
releases it on the **continuation thread** (I-9 in the locking companion), so a thread-affine
primitive cannot work. (2) **Weight.** `OzoneManagerLock` carries eight leveled resource types
in per-type striped `EnumMap`s (`EnumMap<LeveledResource>` at `OzoneManagerLock.java:117`,
`EnumMap<DAGLeveledResource>` at `:126`) plus trackers and reentrancy — machinery this design
does not need. The replacement is a **single fixed striped array** with no resource hierarchy.

**The primitive (locking §6, D-15).** A **fair counting `Semaphore` per stripe** used as an RW
lock: **shared** = acquire 1 permit, **exclusive** = drain all `N` permits (`N` = the per-stripe
permit ceiling). This is the non-thread-affine RW primitive I-9 demands — a semaphore release is
not tied to the acquiring thread, so the continuation thread can release. Fairness prevents
writer starvation behind a stream of readers on a hot parent. Per D-15 the permit pool `N` is a
**large fixed constant** (writer-drains-all): correctness is decoupled from any in-flight
estimate, and write-admission backpressure is a *separate* memory concern, not the lock ceiling.

**No timeout, ever (I-8, B-3, ALT-lock-timeout).** `acquire` blocks until permits are available;
there is **no per-lock timer**. A holder lease that expired a held lock while its Ratis op was
still in flight would let a waiter and the original holder both mutate the protected state — a
mutual-exclusion violation (this is the flaw the grill caught in the lease proposal). The only
bounds on a wait are the **Ratis request timeout** (existing), **release-on-completion** (the
holder's `finally`), and **failover** (I-10): a genuinely hung OM is handled by a new leader
discarding the lock table, not by a lock timer.

**Striping, not per-key (locking §6, B-1).** Keys hash to a stripe index; with ~hundreds of
locks held at steady state and a generously sized array (B-1 ≈ 2^20 stripes ≈ 64 MB),
false-contention collisions are a handful at peak and cost **latency only, never correctness**.
Striping avoids the per-op `ConcurrentHashMap.compute()` + allocation + refcount churn of a
dynamic map — pure overhead on a hot parent directory where real contention exists anyway and a
map cannot help. Acquisition order is therefore by **stripe index** (locking §5), and two
distinct keys colliding on one stripe dedup to a **single acquisition at the strongest mode
required** — the caller passes a pre-sorted, pre-deduped `List<LockRequest>`.

**Handle-owned release across the gap (I-2, I-3).** `acquire` returns a `LockHandle`; the
request releases it in its completion path — possibly on the continuation thread, per step. In a
multi-step op locks are taken and released **per step** (I-3); correctness across the inter-step
gap is provided by reval/resolve-fail/purge (I-5/I-6/I-7), not by holding a lock. The hold span
(I-2) runs from before the Ratis submit until after quorum-commit-and-local-apply of that step;
this is what gives read-your-writes with **no cache** (I-12).

---

## C-orchestrator — `LeaderPlanner` driving the dynamic step-iterator

```yaml
id: C-orchestrator
target_files:
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/execution/  # NEW: LeaderPlanner.java, StepResult.java
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerStateMachine.java  # leader submit path it drives (startTransaction line ~?; applyTransaction line 446)
interface: |
  final class LeaderPlanner {
    CompletableFuture<OMResponse> execute(PlannedRequest req);  // drives req's step-iterator to completion
    // loop: while req.hasNextStep(): step=req.nextStep(); acquire(step.locks);
    //       reval(step); Batch=step.plan(); submitToRatis(Batch).thenCompose(commit -> { release; req.advance(commit); })
    // continuation-driven: the worker thread is freed during each Ratis await (I-9 driver)
  }
depends_on: [D-6, D-16, C-planned-request, C-lock-manager, C-replicated-db-module]
implements: [I-3, I-2, I-9]
tests: [T-1, T-5, T-8]
anti_patterns:
  - "MUST NOT pre-compute the full multi-step chain up front (ALT-static-step-decomposition killed by D-6) — a concurrent delete can invalidate a statically-planned chain; reval re-resolves per step."
  - "MUST NOT hold chain/resolution state in the orchestrator (ALT-stateless-request-orchestrator killed by D-6) — the request owns the iterator (locality of the decomposition logic)."
  - "MUST NOT hold a lock across the inter-step gap (I-3) — release per step; the gap is intentional."
  - "MUST NOT block the worker thread on the Ratis await — register a continuation and free the thread (I-9 driver); hold the client RPC, not a thread."
  - "MUST NOT write a retry-cache entry on a non-terminal step — only the terminal step records (clientId#callId -> response) (D-6, locking §4.3)."
phase: P-0
provenance: inferred
evidence:
  - "D-6 (dynamic step-iterator owned by request) + D-16 (createFile = iterative mkdir -p) locked — master §20"
  - "orchestration model — leader-execution-locking.md §4 (4.1 implicit dir create, 4.2 recursive delete, 4.3 recovery)"
  - "leader submit/apply seam — OzoneManagerStateMachine.applyTransaction line 446, runCommand line 668 (hadoop-ozone/.../ratis/OzoneManagerStateMachine.java)"
```

The orchestrator is the **leader-only driver** that turns one client request into an ordered
chain of dependent Ratis transitions. It generalizes the parent design's contract from "one
request → one transition" to "one request → an ordered chain" (D-6); a single-step op is the
degenerate **N = 1** case, so the same driver runs both.

**The dynamic step-iterator (D-6, ALT-static-step-decomposition rejected).** The planner does
**not** pre-compute the whole chain. It pulls **one step at a time** from the request's iterator,
acquires that step's locks (C-lock-manager, sorted/deduped per locking §5), **revalidates** the
step's nodes by `(parentObjectID, name)` under the lock (I-6 closes the resolve→lock window),
plans that step into a `Batch` (C-replicated-db-module), submits it to Ratis, and on commit
releases the locks and **advances** the iterator — which may now plan a *different* next step
because the world changed. This is mandatory because a concurrent `rm -rf` can invalidate a
statically planned chain: `createFile /a/b/c/file` with `/a/b` being deleted must re-resolve
after each commit, not march a stale plan (T-1, T-5). The chain state lives on the **request**,
not the orchestrator (ALT-stateless-request-orchestrator rejected) — locality of the
decomposition logic with the command that owns it (C-planned-request).

**createFile with missing parents = iterative mkdir -p (D-16, locking §4.1).** `createFile
/a/b/c/file` with `/a/b/c` missing decomposes, OM-internally, into `create b` → await commit →
`create c` → await commit → `create open-file`. Each sub-create needs the *previous* one's
committed objectID to resolve the next parent — which is exactly why the chain is dynamic and
gated on commit. Per D-16 this **non-atomic** multi-dir create is the **defined, contract-correct
semantics** (it matches `mkdir -p` and HDFS), not a compromise; atomicity is unobservable
without cross-op isolation, which is what justifies I-3 (no lock across the gap). A failure after
creating `b`,`c` leaves empty directories (B-2) — idempotent on retry (`create dir` on an
existing dir is a no-op), low-harm, accepted.

**Recursive delete (locking §4.2).** `rm -rf /a/b` is a **synchronous** tombstone of the root
(so I-5 makes new descending resolutions fail immediately — fast UX) plus a **decomposed,
per-node-locked background purge** honouring I-7 (each node removal takes that node's
`X(container)` + slot, enumerates children under the lock, removes only when childless — no
orphan). The orchestrator drives the synchronous tombstone; the redesigned
`DirectoryDeletingService` drives the background purge (the redesign is P-2 scope, master §19
blast radius).

**Asynchronous, continuation-driven (I-9 driver, D-6).** On each step's Ratis await the planner
**registers a continuation and frees the execution thread** — it holds the *client RPC* open, not
a worker thread. This is the reason the lock primitive must be non-thread-affine (C-lock-manager,
I-9): the lock acquired on the submit thread is released on the continuation thread. The leader
submit/apply seam it drives is `OzoneManagerStateMachine.applyTransaction`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerStateMachine.java:446`)
→ `runCommand` (`:668`).

**Recovery is retry-driven, no saga state (locking §4.3, D-6).** A mid-orchestration leader crash
leaves committed sub-steps durable on the quorum and the client's RPC unanswered; the failover
retry re-runs the whole request, idempotently skipping already-created dirs (T-8). The
retry-cache entry (`clientId#callId → response`, C-retry) is written by the **terminal step
only**; intermediate sub-steps are idempotent by structure and need no entry. No persisted
orchestration state exists — this is correct for the filesystem protocol and avoids a saga log.

---

## C-planned-request — `PlannedRequest` base and change recorder

```yaml
id: C-planned-request
target_files:
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/execution/request/  # NEW: PlannedRequest.java (base), ChangeRecorder.java
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/OMClientRequest.java  # the legacy abstract base it parallels (validateAndUpdateCache line 145, preExecute line 112)
interface: |
  abstract class PlannedRequest {
    OMRequest preProcess(OzoneManager om);            // ~ today's preExecute (normalize/validate/auth-prep)
    List<LockRequest> stepLocks(StepContext ctx);     // which stripes this step needs (fed to C-lock-manager)
    boolean hasNextStep(); StepPlan nextStep();        // the dynamic iterator (D-6); single-step => one step
    void advance(CommitResult committed);              // consume a step's commit, possibly re-plan
    // StepPlan.plan(ChangeRecorder rec): rec.put(cf,k,v)/delete(cf,k)/merge(cf,k,opId,operand)/checkpoint(idx)
  }
  final class ChangeRecorder {  // accumulates a Batch (C-replicated-db-module) instead of staging into table cache
    void put(...); void delete(...); void merge(...); void checkpoint(...); Batch toBatch();
  }
depends_on: [D-6, D-1, C-replicated-db-module]
implements: [I-inner-domain-agnostic, I-cache-free-ryw]
tests: [T-proto-roundtrip, T-determinism-follower-byte-identical, T-ryw-from-db]
anti_patterns:
  - "MUST NOT stage changes into the OM table cache (addCacheEntry) — the recorder emits a Batch; reads go to RocksDB (D-3, I-cache-free-ryw). This is the structural break from today's validateAndUpdateCache."
  - "MUST NOT run business logic on the follower — preProcess/nextStep/plan run on the LEADER only; followers apply the recorded Batch blind (D-10)."
  - "MUST NOT let plan() depend on wall-clock, RNG, or iteration order — the leader resolves all non-determinism so follower bytes are identical (D-10, I-determinism-followers-pure)."
phase: P-0
provenance: inferred
evidence:
  - "legacy base it parallels — OMClientRequest abstract: validateAndUpdateCache line 145, preExecute line 112 (hadoop-ozone/.../om/request/OMClientRequest.java)"
  - "today changes are staged into table cache then drained — OMKeyCommitRequest addCacheEntry lines 407-408; OMKeyCreateRequest objectID alloc line 306"
  - "apply seam followers reuse — OMClientResponse.checkAndUpdateDB line 59 -> addToDBBatch line 70 (hadoop-ozone/.../om/response/OMClientResponse.java)"
```

`PlannedRequest` is the **leader-side analogue of `OMClientRequest`** — the base every migrated
command extends. It mirrors the legacy contract just enough to be familiar while making the two
structural breaks the design requires.

**What it parallels.** The legacy abstract base is `OMClientRequest`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/OMClientRequest.java`):
`preExecute` for normalize/validate/auth-prep (`:112`) and the abstract `validateAndUpdateCache`
that does the work (`:145`). `PlannedRequest` keeps a `preProcess` (the same role) but replaces
`validateAndUpdateCache` with the **dynamic step-iterator** (`hasNextStep` / `nextStep` /
`advance`, D-6) so a multi-step request is first-class rather than smuggled into one
`validateAndUpdateCache` call.

**The change recorder is the structural break (D-3, I-cache-free-ryw).** Today
`validateAndUpdateCache` mutates the **table cache** — e.g. `OMKeyCommitRequest` calls
`omMetadataManager.getKeyTable(...).addCacheEntry(dbOzoneKey, omKeyInfo, trxnLogIndex)`
(`OMKeyCommitRequest.java:407-408`) — and the double buffer later drains the cache to RocksDB.
The `ChangeRecorder` **does not touch the cache at all**: `plan()` records `put / delete / merge
/ checkpoint` into a `Batch` (C-replicated-db-module), and reads go straight to RocksDB (OM on
NVMe + block cache, D-3). Read-your-writes is provided by the lock hold span (I-2) — a same-key
successor blocks until the predecessor's bytes are in RocksDB (I-12) — **not** by a cache. This
is the change that removes the cache-epoch↔Ratis-index coupling (a known OM bug class, D-3
consequences).

**Determinism boundary (D-10, I-determinism-followers-pure).** `preProcess`, `nextStep`, and `plan` run on the
**leader only**. The leader resolves every source of non-determinism (objectIDs from
C-managed-index, timestamps captured once, any RNG) so that the recorded `Batch` is a fixed
sequence of bytes. Followers never call these methods — they apply the recorded `Batch` blind via
the existing `OMClientResponse.checkAndUpdateDB` → `addToDBBatch` seam
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/response/OMClientResponse.java:59,70`),
re-expressed through the `OperationApplier`. The contract `plan()` must satisfy is **byte
identity** across nodes (T-determinism-follower-byte-identical).

---

## C-state-machine-dualpath — `registerPlannedCommand` + `isAllowed` gate + apply routing in `OzoneManagerStateMachine`

```yaml
id: C-state-machine-dualpath
target_files:
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerStateMachine.java  # applyTransaction line 446, runCommand line 668, processResponse line 482, terminate line 498
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/protocolPB/OzoneManagerRequestHandler.java  # handleWriteRequestImpl line 418, createClientRequest dispatch line 420-426
interface: |
  // in OzoneManagerStateMachine:
  void registerPlannedCommand(Type cmdType, Supplier<PlannedRequest> factory);  // command opts into new path
  boolean isPlannedPath(OMRequest req);   // finalized(LEADER_SIDE_EXECUTION) && perCommandFlag(cmdType) (D-11,D-14)
  // applyTransaction routes: isPlannedPath ? applyBatch(envelope.batch) : runCommand(request, termIndex) [legacy]
  // applyBatch: OperationApplier.apply(batch, store, rocksBatch, mergeRegistry) — NO business logic
depends_on: [D-10, D-14, D-11, C-replicated-db-module, C-layout-feature, C-orchestrator]
implements: [I-determinism-followers-pure, I-apply-failure-resync, I-txninfo-atomic-with-patch, I-mixed-mode-cache-coherent]
tests: [T-flag-routing-both-paths, T-rolling-upgrade-mixed-binary, T-apply-failure-resync, T-determinism-follower-byte-identical]
anti_patterns:
  - "MUST NOT apply a migrated patch without invalidating the written PartialTableCache keys and updating the FullTableCache volume/bucket entries on every node (D-17 I-mixed-mode-cache-coherent) — else legacy/read-op reads go stale."
  - "MUST NOT run validateAndUpdateCache on the new (planned) apply path on ANY node — apply is bytes only (I-determinism-followers-pure); business logic ran once on the leader."
  - "MUST NOT route to the planned path unless BOTH the layout feature is finalized AND the per-command runtime flag is on (D-11 binary-safety gate + D-14 operational revert) — finalization alone is not enough; the flag defaults to legacy."
  - "MUST NOT complete the apply future exceptionally for a critical apply failure — terminate the OM (fail-stop) so a follower that cannot apply a committed patch crashes and re-syncs (D-10, I-apply-failure-resync), matching today's INTERNAL_ERROR/METADATA_ERROR handling."
  - "MUST NOT advance the applied index without the patch in the same atomic batch (I-txninfo-atomic-with-patch)."
phase: P-0
provenance: verified
evidence:
  - "apply path to fork — OzoneManagerStateMachine.applyTransaction line 446 -> runCommand line 668 -> handler.handleWriteRequest; fail-stop terminate on INTERNAL_ERROR/METADATA_ERROR at processResponse lines 482-505 (hadoop-ozone/.../ratis/OzoneManagerStateMachine.java)"
  - "dispatch that builds the legacy request — OzoneManagerRequestHandler.handleWriteRequestImpl line 418, createClientRequest+validateAndUpdateCache lines 420-426 (hadoop-ozone/.../protocolPB/OzoneManagerRequestHandler.java)"
  - "isAllowed(layoutFeature) gate exemplar — OMBucketCreateRequest.java:421,444 (.isAllowed(OMLayoutFeature.*)); D-11/D-14 locked master §20"
```

This is the **fork in the apply path** — the one place where the legacy "every node re-runs
business logic" model and the new "followers apply bytes" model coexist during the long mixed
mode. It is the highest-stakes seam in the design and the most heavily cited.

**The seam, verified.** Today `applyTransaction`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerStateMachine.java:446`)
runs `runCommand` (`:668`) on **every** node, which calls
`handler.handleWriteRequest` → `OzoneManagerRequestHandler.handleWriteRequestImpl`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/protocolPB/OzoneManagerRequestHandler.java:418`)
→ `createClientRequest(...).validateAndUpdateCache(...)`
(`OzoneManagerRequestHandler.java:420-426`). That is the business-logic-on-every-follower path
this design replaces. The dual-path change routes `applyTransaction`: when `isPlannedPath(req)`
is true, it applies the **inner `Batch`** from the OM envelope via `OperationApplier`
(C-replicated-db-module) with **zero business logic**; otherwise it falls through to the
unchanged legacy `runCommand`.

**The two-gate routing predicate (D-11 + D-14 — both required).** `isPlannedPath` is true **iff
both** hold: (1) the layout feature `LEADER_SIDE_EXECUTION` is **finalized** (C-layout-feature)
— the binary-safety gate that guarantees no un-upgraded follower receives a planned patch it
cannot apply (D-11); and (2) the **per-command runtime flag** for this `cmdType` is on (D-14) —
the operational revert that lets an operator drop a command back to legacy without a downgrade.
The flag **defaults to legacy**, so a freshly finalized cluster still runs legacy until an
operator opts a command in (T-flag-routing-both-paths). Finalization alone is **not** sufficient;
this is why the predicate is a conjunction. The `isAllowed(layoutFeature)` style is the existing
gating idiom — `OMBucketCreateRequest` already gates on
`.isAllowed(OMLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)` and
`.isAllowed(OMLayoutFeature.BUCKET_LAYOUT_SUPPORT)`
(`OMBucketCreateRequest.java:421,444`), so the layout-feature half of the gate reuses a proven
pattern.

**`registerPlannedCommand` (D-14, incremental migration).** Each command opts into the new path
by registering a `PlannedRequest` factory keyed by `cmdType`. An unregistered command (or one
whose flag is off) takes the legacy path untouched. This is what makes the migration **per
command**, lands incrementally in master (D-13 hard-first), and keeps master always stable with
long-lived mixed mode as a first-class state.

**Apply-failure → fail-stop → resync (D-10, I-apply-failure-resync — verified pattern).** A
follower that cannot apply a committed patch must **crash and re-sync**, never silently diverge.
The state machine already has exactly this discipline: `processResponse`
(`OzoneManagerStateMachine.java:482`) calls `terminate` on `INTERNAL_ERROR` / `METADATA_ERROR`
(`:487-505`) — "OM must be terminated instead of completing the future exceptionally, otherwise
OM may continue applying transactions which leads to an inconsistent state." The planned-apply
path inherits this: a `Batch` that cannot be applied (a genuinely corrupt or unexpected
operation) terminates the node, which then installs a snapshot / re-syncs from the quorum (D-10
trades split-brain for loud fail-stop — local failure → crash+resync; uniform failure → loud
cluster-wide stop). The index advances **in the same atomic batch** as the patch
(I-txninfo-atomic-with-patch), mirroring `OzoneManagerDoubleBuffer.java:373-376`, so there is no
patch-applied-index-not-advanced partial state to recover from.

---

## C-layout-feature — `OMLayoutFeature.LEADER_SIDE_EXECUTION`

```yaml
id: C-layout-feature
target_files:
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/upgrade/OMLayoutFeature.java  # add LEADER_SIDE_EXECUTION(10, ...) after SNAPSHOT_DEFRAG(9) line 47
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/upgrade/OMLayoutVersionManager.java  # finalize hook seeds managed index (finalized line 140, getMetadataLayoutVersion)
interface: |
  // OMLayoutFeature enum, appended monotonically:
  SNAPSHOT_DEFRAG(9, "Supporting defragmentation of snapshot"),
  LEADER_SIDE_EXECUTION(10, "Leader-side execution: leader computes each write once and replicates a deterministic DB patch");
  // gate usage: om.getVersionManager().isAllowed(OMLayoutFeature.LEADER_SIDE_EXECUTION)
depends_on: [D-11, C-managed-index]
implements: [I-mixed-mode-safe]
tests: [T-rolling-upgrade-mixed-binary, T-objectid-disjoint]
anti_patterns:
  - "MUST NOT reuse or renumber an existing layout version — append the next ordinal (10) after SNAPSHOT_DEFRAG(9); layout versions are monotonic and persisted."
  - "MUST NOT make any planned-path behavior active before finalization — the #MANAGED_INDEX entry and PersistDb envelope are additive and inert until this feature finalizes (D-11)."
  - "MUST NOT couple the per-command runtime flag (D-14) to this feature — finalization is the binary-safety gate; the flag is the separate operational revert."
phase: P-0
provenance: verified
evidence:
  - "OMLayoutFeature enum, last entry SNAPSHOT_DEFRAG(9) — hadoop-ozone/ozone-manager/.../om/upgrade/OMLayoutFeature.java:47; addAction/action pattern lines 75-85"
  - "finalize hook to seed managed index — OMLayoutVersionManager.finalized line 140, getMetadataLayoutVersion line 74/100 (hadoop-ozone/.../om/upgrade/OMLayoutVersionManager.java)"
  - "isAllowed gate idiom in use — OMBucketCreateRequest.java:421,444; D-11 locked master §20"
```

A one-line enum addition with outsized semantics: it is the **binary-safety gate** that makes
the entire feature fully backwards-compatible (D-11). The mechanism is Ozone's standard layout
finalization — one-way, cluster-wide, never auto-flips.

**The addition (verified).** `OMLayoutFeature` is a monotonically numbered enum; the last entry
is `SNAPSHOT_DEFRAG(9, ...)`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/upgrade/OMLayoutFeature.java:47`).
`LEADER_SIDE_EXECUTION` is appended as ordinal **10**. The enum carries the standard
`addAction` / `action` hook pair (`OMLayoutFeature.java:75-85`) if a one-time upgrade action is
ever needed (none is required for V1 — the new state is additive).

**Why finalization gates the feature (D-11, A-3).** During a rolling upgrade the cluster runs
**mixed binaries**. If a new leader emitted a planned `Batch` to an old follower that does not
understand the `OperationApplier` envelope, that follower could not apply it — split-brain. Layout
finalization is the exact tool for this: it is **one-way** (A-3 — "finalization is one-way"),
flips only after every node is on the new binary, and is the condition the state machine's routing
predicate checks (C-state-machine-dualpath). Until finalization, the `#MANAGED_INDEX` key and the
planned envelope are **additive and inert** — written by no path, read by no path — so a
new-leader → old-follower patch carries them harmlessly (D-11 consequences).

**Finalize-seed coupling (D-12, C-managed-index).** The finalize hook is the place
`ManagedIndexService.seedFromFinalization(max(ratisIndex)+1)` runs, establishing objectID
disjointness (I-objectid-disjoint). The hook is `OMLayoutVersionManager.finalized`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/upgrade/OMLayoutVersionManager.java:140`);
the current metadata layout version is read via `getMetadataLayoutVersion`
(`OMLayoutVersionManager.java:74,100`).

**Separation from the runtime flag (D-14).** Finalization is **not** the operational toggle. It is
the binary-safety gate (can the cluster *ever* run the new path safely?); the per-command runtime
flag (D-14, default legacy) is the separate operational revert (should *this command* run the new
path *right now*?). Both must be true to route to the planned path (C-state-machine-dualpath
anti-patterns). Keeping them separate is what gives operators a no-downtime, no-downgrade revert.

---

## C-merge-operator — Option-B quota merge operator applied at apply time

```yaml
id: C-merge-operator
target_files:
  - hadoop-hdds/framework/src/main/java/org/apache/hadoop/hdds/utils/db/replicated/  # NEW: MergeOperatorRegistry, MergeOperator interface
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/execution/merge/  # NEW: QuotaMergeOperator (decodes OmBucketInfo, applies delta, re-encodes whole row)
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/key/OMKeyCommitRequest.java  # the read-modify-write it replaces — incrUsedBytes line 410 under BUCKET_LOCK line 191-194
interface: |
  interface MergeOperator { byte[] apply(byte[] currentValue, byte[] operand); }   // resolved at apply, in Ratis order
  // QuotaMergeOperator: cur=OmBucketInfo.decode(currentValue); cur.incrUsedBytes(delta); return cur.encode();
  // recorded by the planned commit as: rec.merge(BUCKET_TABLE, bucketKey, "quota.usedBytes", encode(delta))
  // Option B: the OPERAND carries the delta; apply reads current row, applies delta, writes WHOLE ROW (no on-disk operand list)
depends_on: [D-7, D-1, D-3, C-replicated-db-module, C-managed-index]
implements: [I-quota-commutative, I-quota-crash-safe]
tests: [T-quota-concurrent, T-quota-failover, T-quota-exact-tlc]
anti_patterns:
  - "MUST NOT take the bucket WRITE lock to update usedBytes (ALT-quota-wholerow-put killed by D-7) — that re-serializes commits and kills the parallelism the feature exists for. Commits take only S(bucket)."
  - "MUST NOT hold reserved-quota in static in-memory AtomicLong maps (ALT-quota-reserved-static killed by D-7) — reserve state outside the DB needs crash-recovery + reset-on-failure machinery."
  - "MUST NOT depend on a RocksDB-native merge operator / on-disk operand list (ALT-quota-rocksdb-native deferred by D-7) — Option B applies the merge in the module at apply time and writes the whole row, so bucketTable physical representation is unchanged (D-11)."
  - "MUST NOT register the operator on only some nodes (A-5) — every node must have it before any node can receive a Merge, else apply diverges."
  - "MUST NOT treat the merge gate as exact quota admission — over-commit is possible (EXC-3); exactness is the OPEN D-OPEN-quota-enforcement decision, not settled here."
phase: P-1
provenance: inferred
evidence:
  - "read-modify-write it replaces — OMKeyCommitRequest.incrUsedBytes line 410, under acquireWriteLock(BUCKET_LOCK) lines 191-194 (hadoop-ozone/.../om/request/key/OMKeyCommitRequest.java)"
  - "D-7 Option B locked; PR#7583 (errose28 merge operator); 'Option B chosen' grill 2026-06-14 — master §20"
  - "soft-quota over-commit is KNOWN/ACCEPTED (EXC-3) and exactness is OPEN — leader-execution-locking.md §8 EXC-3; master D-OPEN-quota-enforcement (TLC counterexample 2026-06-15)"
```

Quota is the **one mutable counter two parallel commits race on**, and it is the linchpin of the
whole throughput story. Everything else can be a whole-object `Put` (idempotent, commutative by
last-writer-wins on a unique objectID). `usedBytes` / `usedNamespace` are different — they are
*read-modify-written*, and if that read-modify-write needs an exclusive lock, commits serialize
and the feature has no point.

**What it replaces (verified).** Today `OMKeyCommitRequest.validateAndUpdateCache` mutates the
bucket counter with `omBucketInfo.incrUsedBytes(correctedSpace)`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/key/OMKeyCommitRequest.java:410`)
**under the bucket write lock** acquired at `OMKeyCommitRequest.java:191-194`
(`acquireWriteLock(BUCKET_LOCK, ...)`). That write lock is exactly what serializes all commits
into one bucket. The new model takes only a **shared** bucket lock (locking §2.1) and expresses
the increment as a commutative **`Merge`** instead of a locked read-modify-write.

**Option B precisely (D-7, the locked choice).** A planned commit **records** the quota change
as `rec.merge(BUCKET_TABLE, bucketKey, quotaOperatorId, encode(delta))` — the **operand carries
the delta**. At **apply time**, on **every** node, the registered `QuotaMergeOperator` runs in
Ratis order: it decodes the *current* `OmBucketInfo` row, applies the delta, and writes the
**whole row back** (`MergeOperator.apply(currentValue, operand) → newWholeRow`). Two crucial
consequences distinguish Option B from the rejected alternatives:

- **No on-disk representation change (vs ALT-quota-rocksdb-native, deferred).** Because the
  operator writes the whole row, the `bucketTable` value is the same `OmBucketInfo` bytes it is
  today — there is **no operand list persisted on disk**, no dependency on a RocksDB-native merge
  operator, and no physical-schema change (this is what keeps D-11's on-disk invariance true).
- **No bucket serialization (vs ALT-quota-wholerow-put, killed).** A whole-row *Put* under the
  bucket write lock would not commute and would force serialization. The `Merge` **commutes**:
  N concurrent commits each record an independent delta, and the deltas are summed in Ratis
  order at apply, on every node, identically (I-quota-commutative).
- **No out-of-DB reserve state (vs ALT-quota-reserved-static, killed).** The counter lives in the
  DB row; there are no static `AtomicLong` maps to crash-recover or reset-on-failure
  (I-quota-crash-safe). The increment is resolved deterministically in the replicated log.

**Registration is a cluster invariant (A-5).** The operator must be **registered on every node
before any node can receive a `Merge`** — otherwise a follower hits an unknown
`merge_operator_id` and apply diverges (fail-stop, C-state-machine-dualpath). This is why the
operator registry lives in C-replicated-db-module and is wired at startup, and why migrating any
quota-bearing command is gated behind finalization (every node on the new binary).

**The OPEN exactness question — present as open, do not settle (D-OPEN-quota-enforcement,
EXC-3).** Because commits take only `S(bucket)` and the merge is commutative-not-serialized, the
quota *limit* is enforced **best-effort, not exactly**: N commits in flight can each pass the
check against the same pre-increment `usedBytes` and then all apply, transiently over-committing
by up to the in-flight count. What **is** guaranteed: the counter never loses or double-counts an
update — it always equals the true committed size (formally `UsedConsistent`; locking §8 EXC-3).
This over-commit is **mechanically reproducible**: the TLA+ fork's `QuotaOvercommit.cfg` checks
the model against the exact oracle `ObsAbstractExact` and TLC returns a counterexample (two
commits plan at `used=0`, both apply, `used=2 > limit=1`) — and the same TLC pass **recommends a
leader-local atomic reservation** (atomic check-and-reserve in memory; DB merge remains the
durable truth; decrement-on-abort; rebuild-from-DB on failover) as the exact-enforcement path.
**D-OPEN-quota-enforcement is OPEN**: the main-chat grill leaned approximate/eventually-consistent
(locking-3), the TLC fork confirmed over-commit and leans exact-via-reservation. This component
ships the **commutative merge unconditionally** (it is the durable truth either way); the *exact
admission gate* is the open decision layered on top later (mitigation also delegable to the
existing background `QuotaRepair` reconcile). This C-n does **not** decide it.

---

## C-retry — idempotency / retry-cache mechanism (DEFERRED)

```yaml
id: C-retry
target_files:
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/execution/retry/  # NEW (shape only; not built in P-0): InFlightRegistry, (optional) ReplicatedResponseTable
interface: |
  // SHAPE ONLY — the choice is OPEN (D-OPEN-retry):
  //  (a) leader-local in-flight registry  (clientId,callId) -> CompletableFuture<response>   [dedupes concurrent retries]
  //   +  durable replicated table (clientId,callId) -> response  WRITTEN ATOMICALLY WITH THE DATA BATCH  [survives failover]
  //  (b) in-memory-only retry state                                                          [weaker; loses on failover]
  // terminal step records exactly one entry (D-6, locking §4.3); intermediate steps are idempotent by structure
depends_on: [D-OPEN-retry, D-7, C-orchestrator, C-merge-operator]
implements: []
tests: []
anti_patterns:
  - "MUST NOT fix a mechanism before the per-operation idempotency audit exists (D-OPEN-retry). The audit classifies every OM write as idempotent or non-idempotent under client retry/replay."
  - "MUST NOT assume the DB batch needs a retry entry to be idempotent — whole-object Puts/Deletes already are; only RE-EXECUTION of a non-idempotent op (SCM block alloc, quota Merge, table move, soft-delete) double-applies."
  - "MUST NOT write a retry-cache entry on a non-terminal step (D-6) — only the terminal step records (clientId#callId -> response)."
  - "MUST NOT name it 'replayCache' — align with Ratis: 'retryCache' (RC-ivandika-terminology)."
phase: P-1+  # deferred; scoped, not built, in P-0
provenance: verified
evidence:
  - "DEFERRED with full rationale — master D-OPEN-retry (status deferred); leader-execution-locking.md §10 (last bullet, the audit gate)"
  - "terminal-step-only retry entry — leader-execution-locking.md §4.3; raised by ivandika3 (RC-ivandika-retry-cache-semantics), terminology RC-ivandika-terminology"
  - "audit scope ~10 non-idempotent ops not 47 — per-command inventory 2026-06-15 (master D-OPEN-retry consequences)"
```

**This component is deferred by decision (D-OPEN-retry), not omitted by oversight.** It is
documented here so the framework reserves the seam and so a reader does not mistake its absence
for a gap — exactly the "state exceptions as deliberate" discipline the locking companion uses.

**Why it is open.** ivandika3 raised the question on [#7583](https://github.com/apache/ozone/pull/7583): a **batched** Ratis transaction can
answer **many** clients, so how do retry/reply caches work
(RC-ivandika-retry-cache-semantics)? The honest answer is that the mechanism depends on a
classification that does not yet exist. The framing (master D-OPEN-retry; locking §10): the **DB
batch is already idempotent** for whole-object `Put`/`Delete` ops (re-applying the same bytes is
a no-op). The danger is **RE-EXECUTION** of a **non-idempotent** op — one that does SCM block
allocation, a commutative quota `Merge` (C-merge-operator — a re-planned commit double-counts),
a table move, or a soft-delete. Only those ops need the durable retry entry. The per-command
inventory (2026-06-15) scopes the non-idempotent set at **~10 ops, not 47** — so the audit is
tractable, but it **gates the choice**.

**The shape, not the choice.** Two candidate shapes are on the table and **neither is fixed**:
(a) a **leader-local in-flight registry** `(clientId, callId) → future` (dedupes concurrent
retries cheaply) **plus** a **durable, replicated `(clientId, callId) → response` table written
atomically with the data batch** (survives failover — the likely invariant for non-idempotent
ops); or (b) **in-memory-only** retry state (weaker; loses dedup on failover, tolerable only for
naturally-idempotent ops). The **atomic-with-data-batch** property is the leading candidate
invariant for the non-idempotent set.

**The one fixed sub-decision (D-6, carried).** Regardless of mechanism, the retry entry is
written by the **terminal step only** of a multi-step request; intermediate sub-steps are
idempotent by structure (locking §4.3). And the name is **`retryCache`**, not `replayCache`
(RC-ivandika-terminology, adopted). Beyond these, **no mechanism is committed until the
per-operation idempotency audit exists**.

---

## C-command-subclass — the per-command `PlannedRequest` pattern (CreateKey / CommitKey worked)

```yaml
id: C-command-subclass
target_files:
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/execution/request/key/  # NEW: PlannedCreateKeyRequest, PlannedCommitKeyRequest (per-command subclasses)
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/key/OMKeyCreateRequest.java  # the legacy worked example (validateAndUpdateCache line 213; objectID alloc line 306)
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/key/OMKeyCommitRequest.java  # the legacy worked example (validateAndUpdateCache line 140; quota incrUsedBytes line 410; bucket write lock 191-194)
interface: |
  // PlannedCreateKeyRequest extends PlannedRequest (single step, N=1; creates take only S(bucket), no slot — locking I-4):
  //   plan(rec): objectId = managedIndex.next() encoded via OmUtils.getObjectIdFromTxId(epoch, idx);
  //              rec.put(OPEN_KEY_TABLE, openKey(vol,buck,key,clientId), encode(omKeyInfo));   // open key carries clientID -> no collision
  // PlannedCommitKeyRequest extends PlannedRequest (single step; S(bucket)+X(bucket,key) on the slot):
  //   plan(rec): rec.put(KEY_TABLE, ozoneKey, encode(committedKeyInfo));
  //              rec.delete(OPEN_KEY_TABLE, openKey);
  //              rec.merge(BUCKET_TABLE, bucketKey, QUOTA_OP, encode(usedBytesDelta));         // commutative quota (C-merge-operator)
  //              [overwrite] rec.put(DELETED_TABLE, ...) for the soft-deleted prior version
depends_on: [D-6, D-7, C-planned-request, C-merge-operator, C-managed-index, C-lock-manager]
implements: [I-quota-commutative, I-cache-free-ryw, I-4]
tests: [T-quota-concurrent, T-ryw-from-db, T-7, T-determinism-follower-byte-identical]
anti_patterns:
  - "MUST NOT take a slot lock on createKey/createFile (locking I-4) — the open(key|file) row carries clientID so concurrent same-name creates by different clients write distinct rows; same-name resolution defers to commit's X(parent,name)."
  - "MUST NOT incrUsedBytes via a locked read-modify-write on commit — record a Merge operand instead (C-merge-operator); commit takes only S(bucket)."
  - "MUST NOT stage into table cache (addCacheEntry) — record into the ChangeRecorder; reads hit RocksDB (D-3)."
  - "MUST NOT derive the objectID from the Ratis index — source it from ManagedIndexService.next() (D-12); both legacy and planned paths share the counter during mixed mode."
phase: P-1
provenance: verified
evidence:
  - "CreateKey worked example — OMKeyCreateRequest.validateAndUpdateCache line 213; objectID alloc getObjectIdFromTxId line 306 (hadoop-ozone/.../om/request/key/OMKeyCreateRequest.java)"
  - "CommitKey worked example — OMKeyCommitRequest.validateAndUpdateCache line 140; quota incrUsedBytes line 410 under acquireWriteLock(BUCKET_LOCK) lines 191-194; overwrite/soft-delete + addCacheEntry lines 407-414 (hadoop-ozone/.../om/request/key/OMKeyCommitRequest.java)"
  - "creates-take-no-slot-lock — leader-execution-locking.md §2.1 (createKey/createFile rows), I-4"
```

This is the **repeatable pattern** every migrated command follows: subclass `PlannedRequest`
(C-planned-request), implement `stepLocks` + the step-iterator + `plan`, register it on the state
machine (C-state-machine-dualpath). Two commands are worked end-to-end against the **real legacy
code** so the pattern is concrete; both are single-step (the `N=1` degenerate case of D-6).

### CreateKey — the simplest case (single step, no slot lock)

The legacy worked example is `OMKeyCreateRequest.validateAndUpdateCache`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/key/OMKeyCreateRequest.java:213`),
which allocates the objectID via `ozoneManager.getObjectIdFromTxId(trxnLogIndex)`
(`OMKeyCreateRequest.java:306`) and writes an open-key row. The planned subclass
`PlannedCreateKeyRequest`:

- **Locks (locking I-4):** takes **only `S(bucket)`** — **no slot lock**. The open-key table key
  includes `clientID`, so two clients creating the same name write **distinct rows** and cannot
  collide; same-name resolution is deferred to commit. This is the "creates never block creates"
  property (I-4, T-7).
- **objectID (D-12):** `idx = managedIndex.next()`, then
  `OmUtils.getObjectIdFromTxId(epoch, idx)` (the encoding reused verbatim, `OmUtils.java:766`) —
  sourced from the managed counter, not the Ratis index, so legacy and planned paths never
  collide during mixed mode.
- **plan:** `rec.put(OPEN_KEY_TABLE, openKey, encode(omKeyInfo))` — a single whole-object Put,
  idempotent, recorded into the `ChangeRecorder` (no `addCacheEntry`, D-3).

### CommitKey — the quota + overwrite case (single step, slot lock, Merge)

The legacy worked example is `OMKeyCommitRequest.validateAndUpdateCache`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/key/OMKeyCommitRequest.java:140`).
It is the canonical demonstration of **why this whole design exists**, because three legacy
behaviors at this exact site are the things the new model changes:

1. **The bucket write lock (the serialization bottleneck).** Legacy acquires
   `acquireWriteLock(BUCKET_LOCK, ...)` (`OMKeyCommitRequest.java:191-194`) and holds it across
   the commit. The planned subclass takes **`S(bucket)` + `X(bucket, key)`** (the slot rendezvous
   so two commits of the *same* key serialize, locking §2.1) — but commits of **different** keys
   in the same bucket now run **in parallel**, which is the entire throughput win.
2. **The quota read-modify-write.** Legacy does `omBucketInfo.incrUsedBytes(correctedSpace)`
   (`OMKeyCommitRequest.java:410`) under that write lock. The planned subclass instead **records a
   Merge operand** `rec.merge(BUCKET_TABLE, bucketKey, QUOTA_OP, encode(usedBytesDelta))` — the
   commutative quota counter (C-merge-operator), resolved at apply in Ratis order on every node.
3. **The overwrite soft-delete + cache staging.** Legacy stages the committed key and the bucket
   into the table cache (`addCacheEntry`, `OMKeyCommitRequest.java:407-408,412-414`) and routes
   the overwritten prior version to the deleted table. The planned subclass records these as
   explicit operations: `rec.put(KEY_TABLE, ozoneKey, ...)`, `rec.delete(OPEN_KEY_TABLE,
   openKey)`, and (on overwrite) `rec.put(DELETED_TABLE, ...)` for the soft-deleted prior version
   — **no cache** (D-3); read-your-writes comes from the `X(bucket,key)` hold span (I-2/I-12,
   T-ryw-from-db).

The same three substitutions — **shared-not-exclusive bucket lock, Merge-not-locked-RMW for
counters, ChangeRecorder-not-table-cache** — are the mechanical recipe for every other command
in P-1..P-6. The harder commands (createFile, recursive delete) add the *multi-step iterator*
(D-6, C-orchestrator) on top of this recipe but follow the same per-step pattern.

---

## C-test-harness — the linearizability harness

```yaml
id: C-test-harness
target_files:
  - hadoop-ozone/ozone-manager/src/test/java/org/apache/hadoop/ozone/om/execution/linearizability/  # NEW: SequentialReferenceModel, ConcurrentHarness, LinearizabilityChecker, InvariantAssertions
interface: |
  // 1. SequentialReferenceModel: single-threaded path->node(objectID) FS; identical op semantics + error conditions; the "what is legal" oracle
  // 2. ConcurrentHarness: randomized concurrent op mix against the REAL lock + execution path; records per-op [invoke,response] interval + final DB state; weighted toward adversarial pairs T-1..T-6
  // 3. LinearizabilityChecker: Wing-Gong / Lincheck-style search; accepts ANY legal sequential order consistent with real-time precedence (including orders where an op legitimately ERRORS)
  // 4. InvariantAssertions: each mapped to a locking I-n (traceability) + the determinism check (leader vs follower byte-identical)
depends_on: [D-10, D-16, C-orchestrator, C-lock-manager, C-planned-request]
implements: [I-determinism-followers-pure, I-3, I-7, I-11, I-12]
tests: [T-1, T-2, T-3, T-4, T-5, T-6, T-7, T-8, T-determinism-follower-byte-identical, T-apply-failure-resync]
anti_patterns:
  - "MUST NOT write example-based 'op X always succeeds' tests — the design ALLOWS many interleavings and an error under one ordering is a LEGAL outcome (locking §7). The bar is linearizability, not a fixed outcome."
  - "MUST NOT treat quota/subtree-reclamation as strictly-linearizable — they are eventually-consistent (EXC-1/EXC-2/EXC-3); the checker treats them as 'converges after background drains', namespace ops as strictly linearizable."
  - "MUST NOT use fixed sleeps or rely on map/iteration order (determinism); generation must be seeded + reproducible."
  - "MUST NOT assert via a negation a wrong value also satisfies (e.g. assertNotEquals) — assert the exact expected DB state."
phase: P-0..P-2  # harness scaffold P-0; T-1..T-8 land with FSO in P-2
provenance: verified
evidence:
  - "FULL test architecture + T-1..T-8 + the linearizability bar — leader-execution-locking.md §7 (criterion + 4-part architecture), §9 (traceability I-n -> T-n)"
  - "determinism (leader vs follower byte-identical) + apply-failure-resync — D-10 (master §20); master §26 references the harness"
  - "scope split: namespace strictly linearizable, quota/purge eventually-consistent — locking §7 (last paragraph), §8 EXC-1/2/3"
```

The harness is the **proof obligation discharger** — it is how the correctness contract (the I-n
invariants) stops being prose and becomes a CI-checkable property. Its design is the locking
companion's §7; this component is its build-out.

**The bar is linearizability, not example outcomes (locking §7 — the load-bearing constraint).**
The design **deliberately allows** many interleavings and treats "an error under one ordering" as
a **legal** outcome. So a test asserting "create X always succeeds" is **invalid** — under a
concurrent `rm -rf` that create *should* sometimes fail (via reval I-6), and a test forbidding
that failure would forbid correct behavior. The bar is: **every observed concurrent history is
equivalent to *some* legal sequential order consistent with real-time precedence.**

**Four parts (locking §7).** (1) A **sequential reference model** — a single-threaded in-memory
filesystem (path → node with objectIDs) implementing **identical** op semantics and error
conditions; the oracle of "what is legal." (2) A **concurrent harness** running randomized op
mixes against the **real** lock + execution path, recording each op's `[invoke, response]`
interval and the final DB state, **weighted toward the adversarial pairs** T-1..T-6. (3) A
**linearizability checker** (Wing-Gong / Lincheck-style search) verifying the recorded history is
linearizable against the reference model — **accepting any legal order, including ones where an
op legitimately errors**. (4) **Invariant assertions** layered on top, each mapped to a locking
I-n (traceability, locking §9), plus the **determinism check** (leader-computed bytes ==
follower-applied bytes, D-10, T-determinism-follower-byte-identical).

**Scope split (locking §7 last paragraph, §8).** Namespace operations are **strictly
linearizable**; quota usage and subtree reclamation are **eventually consistent** — the checker
treats them as "converges after background work drains," not "correct at every linearization
point" (EXC-1 lazy quota release, EXC-2 eventual subtree purge, EXC-3 soft-quota over-commit). The
adversarial scenarios it must pass are exactly the locking companion's catalog: T-1
(create-under-rm-rf, no orphan), T-2 (purged-parent reval), T-3 (crossing renames, no deadlock),
T-4 (rmdir vs create-child rendezvous), T-5 (deep mkdirs vs ancestor rename), T-6 (rename vs
delete same node), T-7 (hot-parent throughput, creates don't serialize), T-8 (failover
mid-orchestration, idempotent retry, no double-apply). The harness scaffold lands in P-0; T-1..T-8
land with FSO in P-2.

---

## C-legacy-removal — late removal of the double buffer and table cache

```yaml
id: C-legacy-removal
target_files:
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerDoubleBuffer.java  # REMOVE after all commands migrate (flushBatch line 354, splitReadyBufferAtCreateSnapshot line 340)
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerStateMachine.java  # remove double-buffer build/use (buildDoubleBufferForRatis line 556, acquireUnFlushedTransactions line 472, runCommand legacy path line 668)
  - hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OmMetadataManagerImpl.java  # remove table-cache usage once no path stages via addCacheEntry
interface: |
  // P-7 ONLY, after every command migrated AND finalized:
  //  - delete OzoneManagerDoubleBuffer; the OperationApplier (C-replicated-db-module) is the sole RocksDB writer
  //  - delete table-cache write-staging (addCacheEntry) and read-cache lookups; reads go to RocksDB (D-3)
  //  - delete the legacy validateAndUpdateCache dispatch in handleWriteRequestImpl
depends_on: [D-3, P-3, P-4, P-5, P-6, C-replicated-db-module, C-state-machine-dualpath]
implements: [I-cache-free-ryw]
tests: [T-no-cache-correctness, T-full-suite-green-after-removal]
anti_patterns:
  - "MUST NOT remove the double buffer or table cache before EVERY command has migrated AND finalized (P-7 only) — the legacy path needs them through the entire long mixed-mode window; early removal breaks every un-migrated command."
  - "MUST NOT bundle this removal into a feature/migration PR (D-13 / repo convention 'cleanup in separate PRs') — it is irreversible; it lands as its own PR after the migration completes."
  - "MUST NOT leave the #TRANSACTIONINFO atomic-commit pattern behind when the double buffer goes — the OperationApplier must preserve patch+index atomicity (I-txninfo-atomic-with-patch), inheriting OzoneManagerDoubleBuffer.java:373-376."
  - "MUST NOT remove the snapshot barrier semantics (splitReadyBufferAtCreateSnapshot) without the Checkpoint op subsuming them (D-1 consequence)."
phase: P-7
provenance: verified
evidence:
  - "double buffer is the sole RocksDB writer today — flushBatch line 354, atomic #TRANSACTIONINFO line 373-376, snapshot barrier splitReadyBufferAtCreateSnapshot line 340 (hadoop-ozone/.../ratis/OzoneManagerDoubleBuffer.java)"
  - "state machine wiring to unwind — buildDoubleBufferForRatis line 556, acquireUnFlushedTransactions line 472, legacy runCommand line 668 (hadoop-ozone/.../ratis/OzoneManagerStateMachine.java)"
  - "table-cache staging to remove — OMKeyCommitRequest addCacheEntry lines 407-408,412-414; D-3 locked master §20; P-7 in master §29"
```

The capstone, and deliberately **last** (P-7, master §29). This component removes the **double
buffer** and the **table cache** — the two pieces of legacy machinery the new model makes
redundant — but only after **every** command has migrated and finalized.

**Why it is irreversible and therefore last.** The double buffer is the **sole RocksDB writer**
today: `OzoneManagerDoubleBuffer.flushBatch`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerDoubleBuffer.java:354`)
drains staged responses, writes `#TRANSACTIONINFO` atomically with the batch (`:373-376`), and
provides the snapshot barrier via `splitReadyBufferAtCreateSnapshot` (`:340`). The table cache is
the write-staging-and-read surface every legacy `validateAndUpdateCache` uses (e.g.
`OMKeyCommitRequest.java:407-408`). During the **entire long mixed-mode window** (D-13 hard-first
means migration spans many releases), un-migrated commands **still run the legacy path** and
**still need both**. Remove them early and every un-migrated command breaks. So removal waits
until P-7 — after P-3 (snapshot), P-4 (MPU), P-5 (batch/background), and P-6 (easy sweep) have all
migrated their commands (master §29 `depends_on_phases: [P-3, P-4, P-5, P-6]`).

**What replaces them.** The `OperationApplier` (C-replicated-db-module) becomes the **sole RocksDB
writer**, inheriting the patch+index atomicity (I-txninfo-atomic-with-patch) — the
`OzoneManagerDoubleBuffer.java:373-376` pattern must be preserved, not dropped. The snapshot
barrier is subsumed by the `Checkpoint` operation (D-1 consequence — "Checkpoint op subsumes the
snapshot barrier"). Reads go to RocksDB on NVMe + block cache (D-3); read-your-writes is the lock
hold span (I-12), not a cache. The legacy `validateAndUpdateCache` dispatch in
`handleWriteRequestImpl`
(`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/protocolPB/OzoneManagerRequestHandler.java:418`)
is deleted along with the dual-path fork (C-state-machine-dualpath collapses to single-path).
The two D-17 mixed-mode coherence layers retire here as well: with the legacy path gone there is
no cross-model race left to gate, so the migrated path no longer needs the shared `OzoneManagerLock`
bucket lock (`I-mixed-mode-lock-gate`), and with the table cache removed there is nothing to keep
coherent, so the migrated apply's PartialTableCache invalidate + FullTableCache update
(`I-mixed-mode-cache-coherent`) goes away with the cache (D-17, both layers scoped P-0→P-7).

**Repo discipline (D-13, AWC/Ozone cleanup-in-separate-PRs convention).** This removal is **its
own PR**, never bundled into a feature or migration branch. It is the definition-of-done capstone
(master §29 P-7 acceptance: "double buffer gone; single execution model"). The post-refactor
dead-code sweep applies: after this lands, grep for any helper that only existed to bridge the two
paths and remove it.

---

## Cross-reference

This file is the implementation-facing companion referenced from the master spec
`leader-planned-execution.md`, **§11. Component designs (C-n) → companion**. The master holds the
rationale spine (Part IV, D-1..D-16 / D-SPEC-1..3 / D-OPEN-quota-enforcement / D-OPEN-retry), the
correctness contract (Part V, I-n / B-n / T-n), and the delivery plan (Part VI, P-0..P-7); the
locking companion `leader-execution-locking.md` holds the concurrency model (I-1..I-12, the
container/slot locks, the linearizability bar, EXC-1..EXC-3) that **C-lock-manager**,
**C-orchestrator**, and **C-test-harness** implement. The `C-n` blocks above are consumed by the
CI `lint-spec` projection (master §C) to regenerate the traceability matrix (§28), the
decision-dependency graph (§22), and the per-phase task list (§29) — they are not hand-maintained
downstream of these blocks.
