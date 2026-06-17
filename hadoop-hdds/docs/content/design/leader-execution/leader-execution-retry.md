---
title: Leader-Side Execution — Retry/Idempotency and the Terminal Write Path
summary: Durable, replicated exactly-once dedup for OM writes under leader-side execution, and the terminal write path it unlocks (double buffer removed, table cache removed, RocksDB WAL disabled with the Ratis log as the sole WAL)
date: 2026-06-16
jira: HDDS-11898
status: draft (working — expected to evolve)
author: Ritesh Shukla
evidence_commit: e1c3357f80c
evidence_branch: HDDS-11898-design-docs
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

# Leader-Side Execution — Retry/Idempotency and the Terminal Write Path

## 0. Status, scope, and relationship to the parent design

This document specifies two coupled things for [HDDS-11898](https://issues.apache.org/jira/browse/HDDS-11898):

1. **The retry/idempotency mechanism** (§1–§5) — how exactly-once client semantics
   are preserved once request execution moves to the Raft leader and the Ratis log
   entry becomes a *DB patch* rather than the original `OMRequest`. This closes the
   `D-OPEN-retry` gap noted in the parent design and consumes the classification in
   the companion `leader-execution-idempotency-audit.md`.
2. **The terminal write path** (§6–§8) — the end state the retry mechanism unlocks:
   the **double buffer is removed**, the **OM table cache is removed** (parent `D-3`),
   and the **RocksDB write-ahead log is disabled** with the **Ratis log as the sole
   WAL**. These three are bundled here because the durable completion record (§3) and
   the `TransactionInfo` ride in the *same atomic batch* as the data patch, so the
   dedup design and the write-path simplification share one apply step and one set of
   atomicity invariants.

Relationship to the rest of the suite:

- Parent design `leader-planned-execution.md`: phase `P-7` ("remove double buffer +
  table cache", `leader-planned-execution.md:5824`), decision `D-3` ("No OM table
  cache on NVMe", `leader-planned-execution.md:335`), and the throughput diagnosis of
  the double buffer (`leader-planned-execution.md:249`). This document **refines** P-7
  and **adds P-8** (WAL disable); it does not re-derive the parent.
- Companion `leader-execution-locking.md`: the concurrency model (objectID-keyed
  container/slot locks). Dedup admission (§3.2) runs *before* lock acquisition.
- Companion `leader-execution-idempotency-audit.md`: the per-operation N1–N4/I
  classification. §2 (`R-4`) explains why the mechanism protects writes *uniformly*
  rather than only the non-idempotent subset that audit enumerates.

**Lint status.** This file is in `lint_spec.py`'s `DOCS` list and participates in the
mechanical gates: its nine new invariants carry §28 (master) and §6.2 (test-plan)
traceability rows and are covered by the `T-*` scenarios of §9.1; `P-8`, `D-wal-off`, and
`B-replay-length` are wired into the master + phasing. It was authored standalone and
folded into the linted suite in one integration pass — see §9.3 for what that pass did and
the single non-automated follow-up that remains.

### 0.1 Context decisions inherited (restated, not re-derived)

- The leader **executes each write once**, under fine-grained locks, and replicates a
  deterministic DB patch (`Put`/`Delete`/`Merge`); followers apply the bytes with **no
  business logic** (parent `I-inner-domain-agnostic`).
- Independent requests are **batched** by the leader into one Ratis transaction for
  throughput (parent batcher; the 3.3× lever). Batching is a *replication grouping*,
  not a unit of identity — see §4.
- The OM runs on NVMe; reads go to RocksDB directly once the table cache is gone (`D-3`).

---

## 1. The problem: why leader-side execution breaks today's dedup

### 1.1 What the current model does (the baseline)

Today the OM is a *command-replication* state machine and dedup is a **Ratis-layer,
in-memory response cache**:

- The client → OM hop is Hadoop IPC; the end-client's `(clientId, callId)` originate
  there. `clientId` also travels in the `OMRequest` envelope (field 3,
  `OmClientProtocol.proto`); `callId` is taken from the IPC layer server-side.
- The OM forwards the **whole `OMRequest`** as the Ratis log entry, stamping the
  end-client's `(clientId, callId)` onto the `RaftClientRequest`
  (`OzoneManagerRatisServer.java:512`).
- Every node re-executes the request in `applyTransaction` and produces an
  `OMResponse`. The leader's reply is wrapped as the Raft reply `Message`
  (`OMRatisHelper.java:57`) and stored in Ratis's in-memory `RetryCache`, keyed by
  `ClientInvocationId(clientId, callId)`.
- A retry within the cache window is short-circuited by a pre-submit probe
  (`OzoneManagerProtocolServerSideTranslatorPB.java:203`) that returns the cached
  `OMResponse` without re-committing (`OzoneManagerRatisServer.java:559`).
- The cache TTL is 10 minutes (`ozone.om.ratis.server.retry.cache.timeout` =
  600000 ms, `OMConfigKeys.java:246`); the cache is **in-memory, per-server, and lost
  on restart and on leader change**.

### 1.2 Why it breaks under leader-side execution

- **The key disappears.** Under leader-side execution the Ratis log entry is a
  leader-produced **DB patch**, not the `OMRequest`, and one batched patch carries
  *many* clients' work. Ratis's `(clientId, callId)` retry cache no longer keys on the
  user request — its dedup vanishes for the user-visible operation.
- **Failover was never covered.** Even today the cache is in-memory and not part of the
  replicated state, so dedup does **not** survive failover. The system "gets away with
  it" only because the operations clients retry across failover happen to be
  *idempotent by outcome* (e.g., a rename that overwrites). The idempotency audit shows
  that accidental safety runs out for the genuinely non-idempotent set (quota `Merge`,
  block allocation, MPU lifecycle): once those effects are batched and committed,
  re-execution double-applies.

The mechanism below **relocates** the response cache from Ratis's in-memory
`RetryCache` into a **durable, replicated** OM table, keyed correctly for the new
model. Same key material, same cached value — made durable and moved to the layer that
can still see the user request.

---

## 2. Decisions (R-1 … R-5)

- **R-1 — Identity.** Dedup is keyed on the opaque end-client `(clientId, callId)` pair,
  exactly as Ratis keys its cache today. **No client change**: the key material already
  exists server-side (`clientId` in the `OMRequest`, `callId` from IPC). *Deferred
  alternative:* Raft §6.3-style `RegisterClient` sessions with `SESSION_EXPIRED` (see
  §8) — a cleaner GC story but requires a client protocol change; recorded as future
  work, not V1.

- **R-2 — Garbage collection.** V1 GC is **TTL-only, durable, server-side**, with **no
  client change**. A retry within the (durable, generously-sized) TTL is deduped; a
  retry after the TTL may re-execute. *Deferred alternative:* ack-based GC (client
  piggybacks its lowest in-flight `callId`) + a lease backstop — both require a client
  protocol field and so are bundled into a future fast-follow (§8). The bounded residual
  this leaves is characterized in §3.5.

- **R-3 — Mechanism spine.** A dedicated RocksDB column family keyed
  `clientId#callId → serialized OMResponse`; the completion record is written **in the
  same `BatchOperation` as the data patch and `TransactionInfo`**; admission dedup
  checks a **leader-local in-flight registry** (attach-to-future) then the durable
  table; TTL eviction is **decided by the leader and replicated in the patch**
  (deterministic, never a per-replica wall-clock timer); stale-leader patches are
  **fenced by the Raft term + a term-tagged execution→replication handoff**. Detailed in
  §3.

- **R-4 — Scope is uniform across writes.** Because R-3 relocates a cache that today
  stores *every* write's reply, V1 writes a completion record for **every write
  operation** at its terminal step — no runtime per-operation tier check. The
  idempotency audit's tiers do **not** select which operations to cache; they identify
  where the **TTL residual is dangerous** (the non-idempotent set: double-apply after
  the window) versus harmless (idempotent operations self-heal). That mapping sizes the
  TTL (`B-retry-expiry`) and prioritizes the deferred ack/lease work — it is not a runtime
  switch. Read operations are never cached (they take no write path).

- **R-5 — No deterministic-id lever (rejected).** A tempting lever — deriving
  `clientID`/`uploadID` for the create path deterministically from `(callerClientId,
  callId)` to make re-plans idempotent — is **rejected as collision-unsafe**.
  `clientID` is a 64-bit value (`UniqueId.next()` = `time_ms << 16 | counter`,
  collision-free by construction); the would-be input `(callerClientId 128-bit UUID,
  callId 64-bit)` is 192 bits, so any derivation into the 64-bit field collides by
  pigeonhole, and `clientID` participates in the open-key table key `(path, clientID)`
  — a collision lets two distinct requests for the same path share an open-key row and
  silently clobber each other. That is a *regression* from today's collision-free
  `UniqueId`. The durable table (R-3) is the correct home for determinism because it
  keys on the **full, untruncated** `(clientId, callId)`. The create-path
  after-TTL residual (a re-plan mints a fresh `clientID`, producing a duplicate open
  key + a fresh block-set) is **rare and self-limiting**: the orphaned open key is
  uncommitted and is reclaimed by open-key expiry / `DeleteOpenKeys`. See `EXC-RETRY-1`.

---

## 3. The mechanism

### 3.1 The durable completion table

- A dedicated RocksDB column family (the **completion / retry CF**), value =
  the serialized `OMResponse` (the same artifact Ratis caches today), key =
  `clientId#callId`.
- Written by the leader at the operation's terminal step, **inside the same
  `BatchOperation`** that carries the data patch and the `TransactionInfo` update
  (`I-dedup-record-atomic`, `I-txninfo-atomic-with-patch`). A failover replica therefore
  inherits the data effect **and** its completion record together, or neither — this is
  the property that makes dedup survive failover, which the in-memory Ratis cache never
  provided.

### 3.2 Admission dedup flow

On each write, before acquiring locks or executing:

1. Check the **in-flight registry** (§3.3). Hit → attach to the existing future and
   return its result (no second execution).
2. Else check the **durable completion table**. Hit → return the cached `OMResponse`
   verbatim (no re-execution, no re-commit).
3. Miss → register in the in-flight registry, acquire locks, execute once, build the
   patch + completion record, batch, replicate, apply.

### 3.3 In-flight registry (leader-local)

- A leader-local map `(clientId, callId) → future`, covering retries that arrive
  **before the first attempt commits** (the durable table cannot help yet — nothing is
  committed). Behavior on a concurrent retry is **attach-to-future** (the retry waits
  and receives the identical response), matching the in-stack Ratis behavior and
  avoiding a new client-visible "in progress" error.
- **Leader-local and discarded on failover by design.** The requests it tracks had not
  committed, so a new leader correctly re-drives them from the client. Across a leader
  change the **durable table is the sole authority**; the in-flight registry's
  correctness role is *only* same-leader concurrent retries.

### 3.4 TTL eviction (deterministic)

- Eviction is **leader-decided and replicated in the patch**: the leader includes the
  set of records past TTL (by the leader's clock) as deletions in the same replicated
  batch, so every follower applies identical evictions. There is **no per-replica
  background timer** — that would make replicas diverge.
- `B-retry-expiry`: the TTL must be **≥ the worst-case failover + client-retry horizon**
  and no larger than necessary. Under uniform caching (R-4) the table holds
  ≈ `write-rate × TTL` records on disk (not heap — already an improvement over today's
  in-memory cache). The deferred ack-GC (§8) is what later shrinks this from
  time-bounded to in-flight-bounded.

### 3.5 Fencing and the bounded residual

- `I-dedup-fence`: a deposed leader's patch cannot commit (Raft term). Additionally the
  executed patch is **term-tagged** and refused at submit if the term advanced — the
  leader-side analogue of an epoch fence, closing the window where an old leader finishes
  *executing* after losing leadership (we execute before replicating, so this window is
  real).
- `EXC-RETRY-1` (accepted residual): a retry of a non-idempotent operation **after** the
  durable TTL may double-execute. This is the same *kind* of residual as today's
  10-minute in-memory window, but durable and sized via `B-retry-expiry`. It is eliminated
  for live clients by the deferred ack-GC + lease (§8). For the create path specifically,
  the residual is self-limiting (R-5 / `EXC-RETRY-1` note).

---

## 4. Batching ↔ retry orthogonality

Two different things are called "batch": a **client-level batch op** (one `DeleteKeys` /
`RenameKeys` RPC carrying many keys) and the **leader-side batch** (the OM merging many
*independent* clients' requests into one Ratis transaction). The dangerous question is
the latter: batch *composition* is non-deterministic across attempts (a re-batch after
failover groups different requests), so does dedup still hold?

**Yes — because dedup and idempotency are keyed per request, not per batch.** The batch
is a co-replication grouping; the completion record is per request, co-committed inside
whichever batch commits it.

- A leader-side batch `B` is **one atomic Ratis transaction** → one RocksDB
  `BatchOperation` → all-or-nothing. There is no partial-batch commit, so for each
  request `Ri`: "`Ri`'s effect committed" ⟺ "`Ri`'s completion record exists",
  atomically.
- The client reply is released **only after commit**, so a client never observes an
  uncommitted result. Its retry is therefore always a retry of something that either
  committed (record exists) or did not (nothing happened).
- **Case A — `B` committed before the leader crashed:** all records in `B` are durable;
  every retry deduped at admission; **the request is never re-batched.** Re-batch
  composition is irrelevant.
- **Case B — `B` did not commit:** nothing took effect; retries re-execute, possibly in a
  totally different grouping `B'`; harmless first-effective execution.
- **Concurrent double-retry:** the per-request in-flight registry attaches the second, so
  `(clientId, callId)` appears in **at most one** new batch.
- **Stale old leader:** fenced (§3.5); its re-batch cannot commit.

**I-batch-orthogonal** (stated for the spec): *A request's completion record is
co-committed atomically with that request's data patch inside whichever batch carries
it; dedup is keyed on the per-request `(clientId, callId)`. Therefore batch composition
may differ across attempts with no effect on exactly-once — a committed request is
deduped at admission (never re-batched) and an uncommitted request never had an effect
to duplicate.*

Consequence: the leader-side batch needs **no dedup design of its own**, and the
client-level batch op (R-4 / Q5) is simply one `(clientId, callId)` = one completion
record whose stored partial response is replayed verbatim on retry.

**One residual that is a property of leader-side execution, not of batching:** in Case B
the leader had already *executed* (allocated SCM blocks, minted objectIDs) before the
patch failed to commit, so those side effects are orphaned and reclaimed by SCM block
cleanup / open-key expiry. A single un-batched leader-side execution that fails to commit
has the identical leak; batching neither causes nor worsens it.

---

## 5. Invariants (retry mechanism)

- **I-dedup-key** — Dedup is keyed on the end-client `(clientId, callId)`; no other
  identity is minted for dedup purposes (R-1). The full pair is the key — never a
  width-truncated derivative (R-5).
- **I-dedup-record-atomic** — The completion record is written in the **same**
  `BatchOperation` as the request's data patch. A reader/replica sees both or neither.
- **I-txninfo-atomic-with-patch** *(reused from parent)* — `TransactionInfo` is written
  in that same `BatchOperation`. The completion record inherits this txn boundary.
- **I-dedup-handoff** — A request is removed from the in-flight registry **strictly after
  its completion record is durably applied**, never before. Earlier removal opens a gap
  in which a retry finds neither the in-flight future nor the durable record and
  re-executes (double-apply). Bind removal to the **post-apply** step, not post-submit.
  This is the atomic-replace ("install the new before releasing the old") pattern applied
  to dedup state: the durable record is the new resource, the in-flight entry the old.
- **I-dedup-fence** — Patches from a leader that has lost the term cannot commit (Raft
  term) and are refused at submit via the term tag (§3.5).
- **I-inner-domain-agnostic** *(reused from parent)* — Followers apply the completion
  record as opaque bytes alongside the data patch; no node re-derives the response.

Anti-patterns (must not appear):
- A per-replica wall-clock timer evicting completion records (replicas diverge — use
  leader-decided replicated eviction, §3.4).
- Removing the in-flight entry at submit time / on the leader reply path before apply
  (violates `I-dedup-handoff`).
- Caching only the "non-idempotent" subset by a runtime tier check (R-4: cache uniformly;
  the audit tiers are analysis, not a switch).
- Deriving a deterministic `clientID`/`uploadID` into the 64-bit field (R-5: collision).

---

## 6. The terminal write path (post-P-7 + P-8)

The retry mechanism is the last load-bearing user of the apply step, so once it is in
place the apply step can be collapsed to its minimum and the staging machinery deleted.

### 6.1 What the double buffer did, and where each job goes

The double buffer (`OzoneManagerDoubleBuffer.java`) is a **throughput optimization, not a
durability mechanism**. Its three jobs each evaporate:

| Double-buffer job today | Final state |
|---|---|
| Batch many txns into one RocksDB write (`OzoneManagerDoubleBuffer.java:364`) | Subsumed by the **leader-side batcher** — N requests → one merged patch → one `BatchOperation` at apply. Batching moved *upstream* of replication. |
| Decouple the apply thread from disk latency | The apply `db.write` becomes a memtable insert (WAL off, §7), fast enough to run **synchronously in `applyTransaction`**; SST flush/compaction remain RocksDB background work. Synchronous apply also restores natural backpressure (no unbounded staging queue). |
| Serve reads from cache during the apply→flush window (`OzoneManagerDoubleBuffer.java:475` cleanup; `TypedTable.java:203` read-through) | The window ceases to exist: `D-3` removes the table cache, so "applied" = "in RocksDB memtable" = readable. No `@CleanupTableInfo` epoch machinery. |

### 6.2 The unified apply

Every node's `applyTransaction(B)` performs **one synchronous RocksDB `BatchOperation`**:

```
{ data patch (Put/Delete/Merge across data CFs)
  + completion records (clientId#callId -> OMResponse)   // one per request in B
  + TransactionInfo = B.index }                          // the consistency boundary
```

zero business logic on any node. This single batch is the meeting point of the dedup
design (§3) and the write-path simplification: the completion records ride the same
atomic write as the data and the index.

### 6.3 The write path, end to end

1. Client → `OMRequest` (Hadoop IPC) → leader.
2. **Admission dedup** (§3.2): in-flight registry, then durable completion table. Hit →
   return cached `OMResponse`.
3. Leader **executes once**: acquire objectID-keyed locks (locking companion) → read
   current RocksDB state → compute mutations → build patch + completion record; hold
   locks to commit.
4. **Leader batcher** merges independent requests' patches into one Ratis transaction `B`.
5. **Ratis** replicates `B` (the durability authority) → commit on majority.
6. **Every node** applies `B` as the one synchronous `BatchOperation` of §6.2.
7. Leader: after apply (`I-dedup-handoff`) remove from the in-flight registry; reply.
8. **Reads** go straight to RocksDB (memtable + SST); no cache.
9. **Recovery**: restart → RocksDB at persisted `TransactionInfo`
   (`OzoneManagerStateMachine.java:709`) → Ratis replays the tail → exactly-once apply.
10. **Snapshot**: `takeSnapshot` waits for apply, writes `TransactionInfo`, and calls
    `flushDB()` (`OzoneManagerStateMachine.java:580`; `RDBStore.java:312`) → Ratis purges
    log to the snapshot index.

### 6.4 Removed / new

- **Removed:** `OzoneManagerDoubleBuffer` (queue, flusher daemon,
  `splitReadyBufferAtCreateSnapshot` snapshot barrier, backpressure semaphore); the
  in-memory table cache + `@CleanupTableInfo` cleanup (`D-3`); per-node business-logic
  re-execution; Ratis's retry-cache *role* for user dedup (→ §3 durable table).
- **New / changed:** the leader executor + objectID-keyed lock manager; the leader-side
  batcher; the durable completion table; synchronous unified-batch apply on all nodes;
  WAL disable + `atomic_flush` (§7).

---

## 7. D-wal-off — disabling the RocksDB WAL

### 7.1 Finding (verified, this commit)

- The OM RocksDB runs with the **WAL enabled** today: `DBStoreBuilder.java:227`
  constructs `WriteOptions` calling only `setSync(getSyncOption())` (sync defaults to
  **false**); there is **no** `setDisableWAL` anywhere in the tree, and the WAL is
  actively managed (`setWalTtlSeconds`/`setWalSizeLimitMB`, `DBStoreBuilder.java:422`).
- Because `sync=false`, the RocksDB WAL is not fsync'd per write, so **machine-crash
  durability already comes from the Ratis log** (the un-synced WAL is lost on power
  loss; the OM recovers by replaying the Ratis log from the persisted `TransactionInfo`).
  The RocksDB WAL today only accelerates **process**-crash recovery. The Ratis log is
  already the durability authority; the RocksDB WAL is redundant write amplification.
- `atomic_flush` is **set nowhere** (defaults to `false`): column-family memtables flush
  to SST independently, and cross-CF crash consistency relies on WAL replay.

### 7.2 Decision

**Disable the RocksDB WAL on the OM metadata DB and make the Ratis log the sole WAL.**
This decision is **inseparable from enabling `atomic_flush=true`** (§7.3). State it as a
pair: `D-wal-off ≡ {disableWAL on the apply-path writes} ∧ {atomic_flush=true}`.

### 7.3 Invariants and preconditions

- **I-atomic-flush** *(hard precondition)* — With the WAL off, RocksDB **must** run with
  `atomic_flush=true`. An OM transaction's `BatchOperation` spans ≥4 column families
  (a data table, `transactionInfoTable`, the completion CF, the quota counter CF). With
  `atomic_flush=false` the CFs flush independently, so a crash can recover the quota CF
  (or any data CF) **ahead** of `transactionInfoTable`; replay then restarts from the
  stale index and **re-applies committed work — double-counting the non-idempotent quota
  `Merge`**. `atomic_flush=true` forces all CFs to flush at one consistent sequence, so
  the persisted state is always at a transaction boundary.
- **I-merge-replay-safe** *(conditional on I-atomic-flush)* — `Merge` operands are
  appended exactly once: atomic flush keeps every CF at one consistent recovery sequence
  (= a txn boundary), replay starts from the persisted `TransactionInfo`, and each
  replayed txn's batch applies once. `Put`/`Delete` are idempotent regardless; `Merge`
  is the only operand that requires this argument, and the quota counter rides on it.
- **I-log-retention** — The Ratis log must retain every entry after the last **durably
  flushed** RocksDB sequence; log purge index ≤ RocksDB flushed index. This holds **iff
  purge happens only at a `flushDB()`-backed snapshot** (§6.3 step 10), never on
  applied-index alone. Verify Ratis purge configuration is snapshot-gated
  (`raft.server.log.purge.*`) before enabling.
- **I-wal-off-closure** — Every **durable** OM RocksDB write must flow through the
  Ratis-replicated apply path (so it is in the Ratis log and replayed on crash). Any
  direct-to-RocksDB durable write that bypasses Ratis would have **no** durability with
  the WAL off. Audit for such writes (snapshot-chain metadata, bootstrap, background
  services writing directly); route them through Ratis, or retain a per-write WAL for the
  enumerated exceptions (`WriteOptions.disableWAL` is per-write).

### 7.4 Bound and gate

- `B-replay-length` — replay-on-restart length = transactions since the last atomic
  flush, bounded by memtable size / flush cadence. More frequent flush = shorter replay
  + shorter log retention, at more fsync cost. This is a new operational knob the WAL-off
  state introduces.
- **Gate:** before enabling in production, (a) complete the `I-wal-off-closure` audit,
  and (b) **measure replay time on NVMe** at the expected memtable size (`T-wal-off-recovery`).
  Do not infer the cost — measure it (parent guardrail: perf claims require measurement).

### 7.5 Anti-patterns
- Disabling the WAL without `atomic_flush=true` (silent cross-CF torn write →
  double-merge; the single most dangerous mistake in this phase).
- Purging the Ratis log on applied-index rather than at a flushed snapshot (violates
  `I-log-retention` → data loss on crash).
- A durable write that skips the Ratis apply path after the WAL is off (violates
  `I-wal-off-closure` → that write is non-durable).

---

## 8. Phasing

- **P-7** *(parent, refined here):* remove the double buffer + table cache; synchronous
  unified-batch apply; **durability model unchanged** — WAL stays on / sync=false exactly
  as today. This isolates the *throughput* diff. The durable completion table (§3) lands
  with/just before P-7 so dedup survives the loss of the Ratis in-memory cache role.
- **P-8** *(new):* `D-wal-off` — disable the WAL **and** enable `atomic_flush=true`;
  config-flagged; gated on the §7.4 audit + replay measurement. This isolates the
  *durability-model* diff.

**Sequencing rationale (minimum-change discipline).** P-7 changes throughput with a
provably unchanged durability model; P-8 changes the durability model. Keeping them in
separate phases/PRs keeps each diff's blast radius reviewable and lets P-8 ship behind a
flag after measurement, rather than bundling a durability change into a cleanup phase.

---

## 9. Traceability, future work, and integration checklist

### 9.1 Invariant → proposed test → modeling owed

| Invariant | Proposed test (T-n) | Formal model owed |
|---|---|---|
| I-dedup-key | `T-retry-dedup-failover` (commit, kill leader pre-reply, retry on new leader → cached response, no re-exec) | TLA: client-retry + crash → no-double-apply |
| I-dedup-record-atomic | `T-retry-record-atomic` (record and data visible together or not at all) | covered by the above model's atomic-commit step |
| I-dedup-handoff | `T-retry-handoff-gap` (retry in the post-apply/pre-removal window → still deduped) | TLA: in-flight→durable handoff (no-gap) |
| I-dedup-fence | `T-retry-stale-leader` (partitioned old leader's re-exec cannot commit) | TLA: term-fenced submit |
| I-batch-orthogonal | `T-batch-retry-recompose` (commit B, fail over, new leader re-batches different composition → each request once) | TLA: per-request dedup under re-grouping |
| I-atomic-flush / I-merge-replay-safe | `T-crash-replay-merge-once` (WAL off, crash mid-flush with CFs at divergent points → quota counted once) | TLA: multi-CF flush + replay exactly-once |
| I-log-retention | `T-wal-off-log-retention` (purge gated at flushed snapshot; crash before flush → full recovery) | — (config assertion) |
| I-wal-off-closure | `T-wal-off-closure-audit` (enumerate non-Ratis durable writes; assert none, or WAL-retained) | — (static audit) |
| B-replay-length | `T-wal-off-recovery` (measured replay time on NVMe at memtable size) | — (benchmark gate) |

No invariant above is left without a mapped test (the suite rule: an unmapped invariant
is a spec defect). The TLA models are **owed** (the user green-lit modeling retry +
failover + WAL-off replay); they are the natural extension of the existing OBS/FSO
models in `ozone-11898-tla` and are tracked as the next artifact.

### 9.2 Future work (deferred, require a client protocol change)

- **Ack-based GC + lease backstop** (R-2 deferred): client piggybacks its lowest
  in-flight `callId`; the leader replicates ack-driven evictions, shrinking the table
  from time-bounded to in-flight-bounded and eliminating `EXC-RETRY-1` for live clients.
  Needs a new `OMRequest` envelope field + client-side in-flight tracking; RATIS-872 adds
  the analogous piggyback on the OM→Ratis leg but **not** on the client↔OM leg, so it
  does not cover this for free.
- **Raft §6.3 `RegisterClient` sessions** (R-1 deferred): explicit client sessions with
  `SESSION_EXPIRED`, a cleaner GC and overflow story than opaque `(clientId, callId)` +
  TTL. Larger client + protocol change; recorded for a future major version.

### 9.3 Integration into the linted suite (done) + the one non-automated follow-up

This companion was folded into the linted `DOCS` suite in one pass: it is listed in
`lint_spec.py`; its nine new invariants carry §28 (master) and §6.2 (test-plan) rows and are
covered by the `T-*` scenarios of §9.1; `P-8` and `D-wal-off` are added to the master and
phasing with an identical `must_pass` set; `B-replay-length` is added to master §25 and the
retry TTL reuses the existing `B-retry-expiry`; and the durability narrative (D-3 / F-8 /
P-7) now states that durability is Ratis-log-authoritative *today* (sync=false), unchanged
by P-7, and that P-8 makes the Ratis log the sole WAL under `atomic_flush=true`.

**One non-automated follow-up remains.** The §22 decision-dependency graph in the master is
hand-maintained (the lint does not regenerate it), so the `D-wal-off → P-8` edge should be
added to that mermaid graph by hand. It is not a lint gate; it is rationale-graph hygiene.
