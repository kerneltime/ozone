---
title: Leader-Side Execution — Phased Migration Playbook
summary: Per-command migration playbook expanding the hard-first plan (D-13). One PlannedRequest per write Type, behind a per-command runtime flag (D-14), finalization-gated (D-11), landed incrementally into master. Includes the full ~47-operation write inventory (grouped migration-simple / -core / -hard) and the Phase-0 prerequisites that gate every later phase.
date: 2026-06-15
jira: HDDS-11898
status: draft
author: Ritesh Shukla
evidence_commit: d0ef506bc53
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

# Leader-Side Execution — Phased Migration Playbook

> **Role.** This is the companion referenced from master `leader-planned-execution.md` §29
> (Implementation plan / phasing). The master holds the frozen `P-0..P-8` YAML blocks (scope,
> `depends_on_phases`, `must_satisfy`, `must_pass`, `config_flag`, `acceptance`); **this file does
> not re-litigate them** — it expands each phase into a per-command migration playbook: the exact
> command set, the PR/sub-task decomposition (one `PlannedRequest` subclass per write `Type` +
> tests + flag registration), the config flag name, the dependency on prior phases, and the
> acceptance gate. The `P-n` blocks reproduced here are copied verbatim from master §29 so this
> document is readable standalone; the master remains canonical (§A: projections are generated,
> never hand-maintained — if these blocks and master §29 ever diverge, `lint-spec` fails on
> projection-freshness, and master wins).
>
> **Provenance discipline (§A).** Every load-bearing claim about *what an operation does today*
> (which tables it mutates, whether it calls SCM, whether it touches quota, whether it is
> idempotent under re-execution) is cited to `file:line` in the worktree and tagged `verified`.
> Forward-looking statements about *what the migration will do* are tagged `inferred` (they
> describe code that does not exist yet) and pinned to the decision (`D-n`) that constrains them.
> The distinction matters: a reviewer must be able to tell "this is how the legacy code behaves,
> measured" from "this is the plan, designed."

---

## 0. Orientation: what "migrating a command" means, concretely

Leader-side execution does not rewrite the OM request catalogue from scratch. It re-homes each
write **`Type`** (the enum in `OmClientProtocol.proto:41`) from the *legacy* execution path —
where `OzoneManagerStateMachine.applyTransaction` runs `runCommand` → `handler.handleWriteRequest`
→ `OMClientRequest.validateAndUpdateCache` **on every node** (leader and all followers; verified
`OzoneManagerStateMachine.java:446`, `:668`, `:671`) — onto the *new* path, where the **leader**
plans the operation once into a deterministic DB patch and followers apply bytes with zero business
logic (D-1). One write `Type` becomes one **`PlannedRequest`** subclass (the new analogue of
today's `OMClientRequest` subclass; master §11 component "per-command subclasses").

A "migration" of a single command is therefore a small, self-contained, **independently revertible**
unit of work with four moving parts. This is the atom the whole plan is built from:

1. **One `PlannedRequest<T>` subclass** — re-expresses the legacy `validateAndUpdateCache` body as
   *plan-on-leader* (read current DB state, authorize, resolve non-determinism — SCM block IDs,
   objectIDs, timestamps — emit a `Batch{Put|Delete|Merge|Checkpoint}`) instead of
   *execute-on-every-node*. The legacy subclass stays in the tree, untouched, as the fallback the
   flag routes to (D-14).
2. **A flag registration** — one config key (table below), default **legacy** (D-14), wired into
   the router that chooses legacy-`OMClientRequest` vs new-`PlannedRequest` per `Type`. Default-off
   means landing the subclass into master changes nothing observable until an operator opts in.
3. **Tests** — the linearizability harness scenarios (`T-n`) that the command participates in,
   the per-command unit tests for the planner body, the determinism test (leader-emitted patch
   applied on a follower yields byte-identical DB state — `T-determinism-follower-byte-identical`),
   and the **flag-routing** test that proves *both* paths produce identical on-disk results for the
   same input (`T-flag-routing-both-paths`).
4. **The acceptance gate** — the phase does not advance until its `must_pass` `T-n` set is green
   *and* the flag-routing equivalence holds *and* (for perf-sensitive phases) the benchmark gate
   clears the prototype 40k baseline (master §27).

> **Why one-`PlannedRequest`-per-`Type` and not one-per-bucket-layout-class.** Today many key
> `Type`s fan out to **two** legacy request classes — an OBS class and an FSO `...WithFSO` class —
> selected by `BucketLayoutAwareOMKeyRequestFactory` (verified: the factory registers paired
> classes for `CreateKey`, `CreateFile`, `CreateDirectory`, `AllocateBlock`, `CommitKey`,
> `DeleteKey`, `DeleteKeys`, `RenameKey`, `RenameKeys`, the four MPU ops, `SetTimes`,
> `PutObjectTagging`, `DeleteObjectTagging` —
> `BucketLayoutAwareOMKeyRequestFactory.java:79-211`). The new model keeps that split where the
> *planning* logic genuinely differs between flat (OBS) and hierarchical (FSO) namespaces — FSO
> planning resolves parents and takes container/slot locks (locking companion §2.1) that OBS does
> not. The inventory below records the OBS/FSO split per op precisely so the phasing never
> accidentally migrates "half a command" (the OBS half of `CommitKey` without the FSO half), which
> would strand one layout on the legacy path while the other is on the new path — a mixed-mode
> footgun the per-command flag must gate as a *unit per `Type`*, not per class.

---

## 1. The hard-first principle, restated as a build order (D-13)

D-13 (locked; `rejects: [ALT-value-first-phasing]`) mandates migrating the **hardest**
commands and scenarios first and deferring the easy single-table leg-work. The rationale is risk,
not heroics: an intrusive refactor of the OM write path carries an **abandonment risk** — if the
easy, high-visibility commands ship first and "prove value," the politically-boring hard bits
(multi-step FSO orchestration, recursive delete, commutative quota, snapshot Checkpoint ordering)
can stall, leaving the cluster *permanently* in mixed mode with the showstoppers unsolved
(`ALT-value-first-phasing.reason`). Hard-first front-loads exactly the work whose feasibility is
in doubt, so that if the design is going to fail it fails early — while the legacy path is still
fully intact behind every flag (D-14) and finalization has not been crossed (D-11).

Concretely the order is:

- **P-0** builds the substrate (12 components) **plus the three prerequisites** (§3) and wires
  *nothing* live. Inert. The whole framework lands behind a feature that finalization gates and
  flags default-off.
- **P-1** migrates the **hardest single-step OBS** key path (CreateKey, CommitKey, AllocateBlock,
  DeleteKey) — this is where commutative quota (D-7), cache-free read-your-writes (D-3/D-5), and
  SCM block allocation as a non-deterministic leader-only step all first bite.
- **P-2** migrates the **hardest multi-step FSO** path (CreateFile/CreateDirectory with implicit
  parents, FSO delete, recursive `rm -rf` + the `DirectoryDeletingService` redesign) — the
  orchestration contract (D-6), the no-orphan purge (locking I-7), and the iterative-mkdir-p
  reference model (D-16). The showstoppers are *retired by the end of P-2/P-3*.
- **P-3** migrates **snapshot** (CreateSnapshot/Checkpoint op, SnapshotPurge standalone, moves).
- **P-4** migrates **MPU** (4 ops + AbortExpired) and revisits the large-value concern
  ([HDDS-8238](https://issues.apache.org/jira/browse/HDDS-8238) / RC-xichen-large-value).
- **P-5** sweeps the **batch/background** ops (DeleteKeys, RenameKey/Keys, DeleteOpenKeys,
  PurgeKeys/Directories).
- **P-6** sweeps the **easy Set-A** single-table ops (ACLs, tagging, SetTimes, secrets, tokens,
  tenant, snapshot props, vol/bucket props, prepare).
- **P-7** is **cleanup**: remove the double buffer and table cache, delete the legacy path, and
  drop the per-command flags. (The `OMLayoutFeature` finalization is **not** crossed here — it was
  crossed before P-1 per §16.1 / master §16; P-7 only retires the now-callerless legacy fallback.)

P-5 and P-6 deliberately `depend_on_phases` the early hard phases (P-2 / P-1 respectively in
master §29), **not** the other way round: the easy work is gated *behind* the hard work so it can
never be used to declare premature victory.

---

## 2. Reading the inventory (column semantics)

The inventory in §4 is the spine of this document: it is the enumerated, cited list of every write
`Type` the OM accepts, derived by reading two sources end-to-end —

- the **write `Type` enum**, `hadoop-ozone/interface-client/src/main/proto/OmClientProtocol.proto:41-165`
  (verified), and
- the **`createClientRequest` switch** that maps each write `Type` to its legacy request class,
  `hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/utils/OzoneManagerRatisUtils.java:127-352`
  (verified) — including the FSO/OBS fan-out via
  `BucketLayoutAwareOMKeyRequestFactory.java:79-211` (verified) and the `AddAcl/RemoveAcl/SetAcl`
  fan-out via `getOMAclRequest`, `OzoneManagerRatisUtils.java:354-…` (verified).

Read `default:` in that switch as the boundary: every read-only `Type` in the proto enum
(`LookupKey`, `ListKeys`, `InfoBucket`, `GetFileStatus`, `SnapshotDiff`, `ServiceList`, …) falls
through to `default:` and throws `INVALID_REQUEST` for a *write* path
(`OzoneManagerRatisUtils.java:345-347`, verified) — they are **not** writes and are **out of scope**
for this migration. The inventory lists only the ~47 cases that resolve to a concrete write request
class.

Column meanings:

| Column | Meaning | How derived |
|---|---|---|
| **Type** | The proto enum constant (`OmClientProtocol.proto`). | verified enum |
| **Request class(es)** | The legacy `OMClientRequest` subclass(es) the switch builds. `OBS / FSO` where the layout factory splits it. | verified switch + factory |
| **Tables mutated** | RocksDB column families the legacy `validateAndUpdateCache` writes (via `addCacheEntry` today; via `Batch` ops after migration). | verified per-class grep |
| **Quota?** | Does it read/write bucket `usedBytes`/`usedNamespace` (the commutative-Merge target, D-7) or a quota *limit*? "usage" = the Merge target; "limit" = a property write, not a Merge. | verified `incrUsed*/decrUsed*/setQuotaInBytes` |
| **SCM?** | Does planning call the SCM block client (`getScmClient().allocateBlock`) — a non-deterministic, leader-only step that must be resolved before replication (D-10)? | verified `getScmClient`/`allocateBlock` |
| **Multi-step?** | Does one client request decompose into an ordered chain of Ratis transitions (D-6), or is it a single transition (N=1 degenerate case)? | verified structure |
| **Idempotent on re-execution?** | If the leader **re-plans and re-applies** the same client request (failover retry, no retry-cache hit), does the second execution converge to the same DB state, or does it double-apply? This is the column that scopes D-OPEN-retry. | inferred from op semantics + cited mutation shape |

> **Critical nuance on the idempotency column (gates D-OPEN-retry).** There are two different
> idempotency questions and they have opposite answers, so the spec is careful to name which one
> each cell reports. (a) *Is the **DB patch** idempotent?* — almost always **yes**: the patch is
> whole-object `Put`/`Delete` over deterministic keys, so applying the identical bytes twice is a
> no-op (this is why a follower can crash-and-resync and re-apply committed entries safely, D-10).
> (b) *Is **re-execution** (re-planning from scratch) idempotent?* — **no** for any op that, during
> *planning*, consumes a non-idempotent external resource or emits a commutative `Merge`:
> specifically **SCM block allocation** (a fresh `allocateBlock` returns *new* block IDs every call)
> and **quota `Merge`** (a second `+correctedSpace` double-counts). The inventory's idempotency
> column reports **(b)** — re-execution — because that is what dictates which ops need the durable
> retry entry. Per D-OPEN-retry (master, resolved; locking §10), the DB batch is already idempotent
> and **only re-execution** of the SCM/quota/move/soft-delete set is not, which is why the retry audit
> classifies ~26 client-facing ops in two tiers, not 47.

---

## 3. Phase-0 prerequisites (gate EVERY later phase — call these out explicitly)

Master §29 folds **three** load-bearing prerequisites into `P-0`'s scope line
(`"...+ legacy->ManagedIndex objectID retrofit + dual-path index durability + OMLayoutFeature
finalization gate"`): **PR-0a** (legacy→ManagedIndex objectID retrofit, D-12), **PR-0b** (dual-path
applied-index durability), and **PR-0c** (`OMLayoutFeature` finalization gate, D-11). They are broken
out here because **no command may migrate until all three are in place** — they are not part of any
single command's PR; they are the soil. Each is stated as: what exists today (verified), what must
change, why it gates the rest, and which decision constrains it.

### 3.1 Prerequisite PR-0a — Legacy → ManagedIndex objectID retrofit (D-12, enables D-8)

**What exists today (verified).** Every object's `objectID` is minted by
`OmUtils.getObjectIdFromTxId(epoch, txId)` where `txId` is the **Ratis transaction log index**
(`OmUtils.java:766-783`, verified — the encoding is `(epoch<<62) | (txId<<8)`, the low 8 bits
reserved for the 256-wide recursive-directory window). The call sites that source `objectID` from
the Ratis index are enumerated and load-bearing (all verified):

- `OzoneManager.getObjectIdFromTxId(trxnId)` — the central accessor — `OzoneManager.java:2380-2382`;
- key create — `OMKeyCreateRequest.java:306`, `OMKeyRequest.java:437`, `:494`, `:1185`;
- file/dir create — `OMFileCreateRequest.java:250`, `OMDirectoryCreateRequest.java:178`;
- key commit (pseudo objId for the committed entry) — `OMKeyCommitRequest.java:335`,
  `OMKeyCommitRequestWithFSO.java:262`, `:314`;
- MPU initiate — `S3InitiateMultipartUploadRequest.java:139`;
- tenant create — `OMTenantCreateRequest.java:292`;
- prefix ACLs — `PrefixManagerImpl.java:244`, `:335`.

**What must change.** Both the **legacy** path and the **new** `PlannedRequest` path must draw
`objectID`/`updateID` from **one** monotonic **ManagedIndex** counter (master §11 component
`ManagedIndexService`), not from the Ratis index. The retrofit redirects the legacy call sites
above to source from the managed counter while keeping the *encoding* identical (D-8: same
`getObjectIdFromTxId` format, low 8 bits go dead-zero once the 256-window is retired).

**Why it gates everything (the collision hazard).** Mixed mode is **long-lived and first-class**
(D-14): for an extended window some commands execute on the legacy path (objectID = f(Ratis index))
and others on the new path (objectID = f(managed index)). If the two counters are independent, a
legacy op and a new op can mint the **same** objectID — a silent namespace corruption. D-12 closes
this by construction: a single shared counter means old and new objectID ranges are disjoint
(D-8 `consequences: "old/new objectID disjoint by construction on upgrade"`). Because *any*
migrated command can collide with *any* still-legacy command, this retrofit must precede the **first**
command migration, P-1. (Provenance for the hazard: PR#7583 review note "use managed index in both
flows"; grill catch 2026-06-15 — recorded in master D-12 `evidence`.)

**Anti-pattern to avoid.** Do not migrate `CreateKey` (P-1) "to test the managed index" before this
retrofit lands — that is precisely the mixed-mode collision window D-12 exists to prevent.

### 3.2 Prerequisite PR-0b — Dual-path #TRANSACTIONINFO / applied-index durability

**What exists today (verified).** The double buffer is the **sole** RocksDB writer; it flushes
each batch and atomically stamps `#TRANSACTIONINFO` (the persisted last-applied Ratis index) in the
same write (`OzoneManagerDoubleBuffer.java`, `OzoneManagerStateMachine.java` — both reference
`TRANSACTION_INFO_KEY`, verified by presence). The state machine already tracks a *second* index
notion for entries Ratis notifies but the state machine **skips** (e.g. metadata/no-op entries):
`lastSkippedIndex`, advanced in `notifyTermIndexUpdated` (`OzoneManagerStateMachine.java:108-111`
field decls, `:242-269` update logic, `:582` the catch-up loop `while
(getLastAppliedTermIndex().getIndex() < lastSkippedIndex)`, all verified).

**What must change.** Under leader-side execution the *new* path writes the data patch **and** the
applied-index stamp together (master invariant `I-txninfo-atomic-with-patch` — "#TRANSACTIONINFO atomic with
patch"), but during mixed mode the **legacy** double-buffer path and the **new** patch-apply path
both advance durability *concurrently for different commands*. The applied-index bookkeeping must be
**dual-path**: a single, monotonic, crash-consistent notion of "last durably applied index" that
both writers advance correctly, extending the existing `lastSkippedIndex` mechanism so that the new
path's committed indices and the legacy path's flushed indices interleave without either regressing
the stamp or double-applying on restart. (Master §19 lists this under AFFECTED: "dual-path
applied-index durability (extend lastSkippedIndex)".)

**Algorithm.** Both writers — the legacy `OzoneManagerDoubleBuffer.flushBatch` and the new apply
engine — maintain ONE monotonic durable index via `#TRANSACTIONINFO`: (1) each writer, in the SAME
RocksDB `BatchOperation` as its data, writes `#TRANSACTIONINFO=TransactionInfo(termIndex)` for the
highest index in that batch; (2) after `commitBatchOperation` returns, it advances the in-memory
`lastAppliedTermIndex` via the existing `updateLastAppliedTermIndex` consumer (monotonic max —
out-of-order completion between paths cannot regress it); (3) `notifyTermIndexUpdated` /
`lastSkippedIndex` keep reconciling no-op/metadata entries; the migrated apply is a THIRD index
source feeding the same `updateLastAppliedTermIndex`, so all three compose monotonically;
(4) `takeSnapshot` waits until `lastAppliedTermIndex>=lastSkippedIndex` AND both writers' in-flight
batches up to the snapshot index have committed (`awaitFlush` for the double buffer AND an
in-flight-commit drain for the apply engine) before persisting `#TRANSACTIONINFO`+flushDB.
Failure table: (a) new-apply batch commit throws -> nothing written (atomic batch) -> terminate+resync
(D-10), index not advanced; (b) `#TRANSACTIONINFO` write fails -> impossible separately (same atomic
batch as data); (c) `lastAppliedTermIndex` advance throws post-commit -> DB durable, in-memory stale
-> terminate; (d) `takeSnapshot` persisting before a migrated batch commits -> prevented (it drains
both paths first). Invariant: `#TRANSACTIONINFO` is monotonic = max(durable legacy index, durable
migrated index); the applied index advances ONLY after the durable commit of whichever path produced
it.

**Why it gates everything.** If the durability stamp is not correct across both paths, a crash mid
mixed-mode can replay a committed-but-unstamped patch (double-apply) or skip a stamped-but-unflushed
legacy batch (lost write). Either breaks the determinism contract (D-10) the whole feature rests on.
So this lands in P-0, inert, before any command flips.

### 3.3 Prerequisite PR-0c — `OMLayoutFeature` finalization gate (D-11)

**What exists today (verified).** OM upgrade is gated by `OMLayoutFeature`
(`OMLayoutFeature.java:26-47`, verified). The current highest feature is `SNAPSHOT_DEFRAG(9)`. The
finalization-gate idiom is `versionManager.isAllowed(OMLayoutFeature.X)` — exemplar at
`OMBucketCreateRequest.java:421` (`ERASURE_CODED_STORAGE_SUPPORT`) and `:444`
(`BUCKET_LAYOUT_SUPPORT`), both verified (these are the exact lines master D-11 cites).

**What must change.** Add a new layout feature — `LEADER_SIDE_EXECUTION(10, ...)` (inferred: next
ordinal after `SNAPSHOT_DEFRAG(9)`) — that gates the binary-level safety of the new path. The new
proto/`PersistDb` machinery and the `#MANAGED_INDEX` entry are **additive and inert until
finalization** (master D-11 `consequences`). The finalization gate is the *binary*-safety boundary
(a new leader must not replicate a patch an old follower cannot apply — that is split-brain,
`ALT-no-backwards-compat`). It is **separate from** the per-command runtime flag (D-14), which is
the *operational* revert knob: finalization says "the cluster is uniformly new-binary and may use
the new wire format"; the flag says "for this command, use the new path or the legacy path." Both
must be true for a command to actually run on the new path.

**Why it gates everything.** Until the feature is defined and the gate is wired, there is no safe
way to even *enable* a command on a rolling-upgraded cluster (D-11). P-0.

> **The three prerequisites are interlocked.** PR-0a (shared counter) makes mixed-mode objectIDs
> safe; PR-0b (dual-path durability) makes mixed-mode crash recovery safe; PR-0c (finalization gate)
> makes mixed-mode *binaries* safe. Mixed mode is unavoidable (rolling upgrade) and long-lived
> (D-14), so all three are non-negotiable preconditions for P-1. None of them changes observable
> behavior on its own — that is the point of doing them in inert P-0.

---

## 4. The write-operation inventory (all ~47 write Types, grouped A / B / C)

Grouping follows D-13's difficulty axis and master §29's phase scopes:

- **Group C — migration-hard (17 ops):** quota-bearing, SCM-calling, multi-step, snapshot/Checkpoint,
  or recursive-delete. These are the showstoppers; they migrate **first** (P-1..P-4, with the
  recursive-delete/purge background ops landing across P-2/P-5).
- **Group B — migration-core (8 ops):** batch/background key ops and the volume/bucket structural
  ops that touch quota *limits* or multiple structural tables but are single-step and SCM-free.
- **Group A — migration-simple (~22 ops):** single-table, quota-free, SCM-free, single-step,
  whole-row `Put`/`Delete`, idempotent on re-execution. The easy Set-A sweep (P-6).

Counts: C = 17, B = 8, A = 22 → **47**. (Matches the master scaffold's "migration-simple ~22,
migration-core 8, migration-hard 17".)

### 4.1 Group C — migration-hard (17)  → P-1, P-2, P-3, P-4, P-5(purge)

| # | Type | Request class(es) OBS / FSO | Tables mutated | Quota? | SCM? | Multi-step? | Idempotent on re-exec? | Phase | Evidence (file:line) |
|---|---|---|---|---|---|---|---|---|---|
| C1 | CreateKey | `OMKeyCreateRequest` / `OMKeyCreateRequestWithFSO` | openKeyTable (+ directoryTable for FSO missing parents) | usage (usedNamespace for missing parents) | **yes** (`allocateBlock`) | no (FSO parents created in-line today) | **no** (fresh SCM block IDs each plan) | P-1 (OBS) / P-2 (FSO) | `OMKeyCreateRequest.java:165` (scmClient), `:338` (incrUsedNamespace), `:351` (openKeyTable); factory `:99-105` |
| C2 | CommitKey | `OMKeyCommitRequest` / `OMKeyCommitRequestWithFSO` | keyTable/fileTable (put), openKeyTable (delete), deletedTable (overwrite soft-delete) | **usage** (`incrUsedBytes`,`incrUsedNamespace`) | no | no | **no** (quota `Merge` double-counts) | P-1 (OBS) / P-2 (FSO) | OBS `OMKeyCommitRequest.java:407` (keyTable),`:410` (incrUsedBytes),`:378` (incrUsedNamespace),`:288` (openKey); FSO `OMKeyCommitRequestWithFSO.java:305,348`; factory `:118-123` |
| C3 | AllocateBlock | `OMAllocateBlockRequest` / `OMAllocateBlockRequestWithFSO` | openKeyTable (append block) | no | **yes** (`allocateBlock`) | no | **no** (fresh SCM block IDs) | P-1 (OBS) / P-2 (FSO) | `OMAllocateBlockRequest.java:115` (scmClient); factory `:108-114` |
| C4 | DeleteKey | `OMKeyDeleteRequest` / `OMKeyDeleteRequestWithFSO` | keyTable/fileTable (tombstone), deletedTable (move), openKeyTable | **usage** (`decrUsedBytes`,`decrUsedNamespace`) | no | no | **no** (quota `Merge` decrement double-applies) | P-1 (OBS) / P-2 (FSO) | OBS `OMKeyDeleteRequest.java:155,166,167`; FSO recursive `OMKeyDeleteRequestWithFSO.java:142,149,154`; factory `:128-131` |
| C5 | CreateFile | `OMFileCreateRequest` / `OMFileCreateRequestWithFSO` | openKeyTable + directoryTable (missing parents) | **usage** (`incrUsedNamespace` over parents) | **yes** (`allocateBlock`) | **YES** (implicit mkdir-p chain, D-6/D-16) | partial (dir creates idempotent; SCM-bearing open-file not) | P-2 | `OMFileCreateRequestWithFSO.java:154` (getAllMissingParentDirInfo),`:195` (incrUsedNamespace); `OMFileCreateRequest.java:250` (objId); factory `:89-93` |
| C6 | CreateDirectory | `OMDirectoryCreateRequest` / `OMDirectoryCreateRequestWithFSO` | directoryTable (the dir + missing parents) | **usage** (`incrUsedNamespace`) | no | **YES** (implicit mkdir-p chain, D-6/D-16) | **yes** (create-on-existing-dir is a no-op) | P-2 | `OMDirectoryCreateRequestWithFSO.java:138` (getAllMissingParentDirInfo),`:148` (incrUsedNamespace),`:158`; factory `:80-86` |
| C7 | DeleteKey (FSO recursive dir, `rm -rf`) | `OMKeyDeleteRequestWithFSO` (recursive=true branch) | directoryTable (root tombstone), deletedDirTable (move), keyTable | usage (deferred/lazy release, EXC-1) | no | **YES** (sync root tombstone + async per-node purge, locking §4.2) | **yes** (re-tombstone is no-op) | P-2 | `OMKeyDeleteRequestWithFSO.java:86` (recursive),`:142,149,154` |
| C8 | PurgeDirectories | `OMDirectoriesPurgeRequestWithFSO` | directoryTable (remove node), fileTable (remove), deletedDirTable, snapshotInfoTable | **usage** (`decrUsedBytes`,`decrUsedNamespace` lazy) | no | **YES** (background drain of recursive delete, locking I-7) | **yes** (purge of absent node is a no-op) | P-2 (with DDS redesign) | `OMDirectoriesPurgeRequestWithFSO.java:154,158,192,193,197,226` |
| C9 | CreateSnapshot | `OMSnapshotCreateRequest` | snapshotInfoTable + **RocksDB Checkpoint** (the snapshot image) | no | no | no (single transition, but Checkpoint op) | **yes** (snapshot key uniqueness rechecked) | P-3 | `OMSnapshotCreateRequest.java:166` (isExist),`:275` (snapshotInfoTable); Checkpoint subsumes barrier (D-1) |
| C10 | SnapshotPurge | `OMSnapshotPurgeRequest` | snapshotInfoTable + delete RocksDB checkpoint | no | no | no (standalone, master §19/§29 P-3) | **yes** (already-purged is a no-op, `:107-109`) | P-3 | `OMSnapshotPurgeRequest.java:97-102,107,121` |
| C11 | SnapshotMoveDeletedKeys | `OMSnapshotMoveDeletedKeysRequest` | deletedTable / deletedDirTable across snapshot↔AOS (move), snapshotInfoTable | usage (reclaim accounting) | no | no | **no** (re-moving already-moved keys double-counts) | P-3 | `OMSnapshotMoveDeletedKeysRequest.java:79-93` (move/reclaim lists), `:85` (updateCache) |
| C12 | SnapshotMoveTableKeys | `OMSnapshotMoveTableKeysRequest` | deletedTable/deletedDirTable across snapshots (move), snapshotInfoTable | usage (reclaim accounting) | no | no | **no** (move is not re-execution-safe) | P-3 | switch `OzoneManagerRatisUtils.java:228-229`; class present (snapshot move family) |
| C13 | InitiateMultiPartUpload | `S3InitiateMultipartUploadRequest` / `...WithFSO` | openKeyTable, multipartInfoTable | no | no | no | **no** (mints objectID from index — needs ManagedIndex, D-12) | P-4 | `S3InitiateMultipartUploadRequest.java:139,221,224`; factory `:157-160` |
| C14 | CommitMultiPartUpload (commit part) | `S3MultipartUploadCommitPartRequest` / `...WithFSO` | multipartInfoTable, openKeyTable, deletedTable (replaced part) | **usage** (`incrUsedBytes`) | no | no | **no** (quota `Merge`; part replacement) | P-4 | `S3MultipartUploadCommitPartRequest.java:223,227,257`; factory `:165-168` |
| C15 | CompleteMultiPartUpload | `S3MultipartUploadCompleteRequest` / `...WithFSO` | keyTable (assemble), openKeyTable (delete), multipartInfoTable (delete), deletedTable (overwrite) | **usage** (`incrUsedBytes`,`incrUsedNamespace`) | no | **YES** (assembles N parts; FSO parent resolution) | **no** (quota `Merge` + assembly) | P-4 | `S3MultipartUploadCompleteRequest.java:334,341,570,675,679`; factory `:181-184` |
| C16 | AbortMultiPartUpload | `S3MultipartUploadAbortRequest` / `...WithFSO` | openKeyTable (delete), multipartInfoTable (delete), deletedTable (parts → deleted) | **usage** (decr, parts freed) | no | no | **yes** (abort of absent MPU is a no-op) | P-4 | `S3MultipartUploadAbortRequest.java:185,188`; factory `:173-176` |
| C17 | AbortExpiredMultiPartUploads | `S3ExpiredMultipartUploadsAbortRequest` | openKeyTable, multipartInfoTable, deletedTable (background sweep of expired MPUs) | usage (decr) | no | **YES** (batch over many expired uploads) | **yes** (re-abort of already-gone is a no-op) | P-4 | switch `OzoneManagerRatisUtils.java:331-332` |

### 4.2 Group B — migration-core (8)  → P-1(structural), P-5(batch)

| # | Type | Request class(es) | Tables mutated | Quota? | SCM? | Multi-step? | Idempotent on re-exec? | Phase | Evidence (file:line) |
|---|---|---|---|---|---|---|---|---|---|
| B1 | DeleteKeys (batch) | `OMKeysDeleteRequest` / `OMKeysDeleteRequestWithFSO` | keyTable/fileTable (tombstone N), deletedTable (move N), openKeyTable | **usage** (decr per key) | no | **YES** (N keys, multi-slot lock set, locking §5) | **no** (quota decrement double-applies) | P-5 | switch `:286-292`; factory `:136-139`; class `OMKeysDeleteRequest.java` |
| B2 | RenameKey (single) | `OMKeyRenameRequest` / `OMKeyRenameRequestWithFSO` | keyTable (put new + delete old) — FSO: directoryTable re-parent (O(1), F-2) | no (no usage change) | no | no | **yes** (whole-row put+delete; target-exists rejected, no soft-delete) | P-5 | `OMKeyRenameRequest.java:164` (reject exists),`:183,194`; factory `:144-147` |
| B3 | RenameKeys (batch) | `OMKeysRenameRequest` | keyTable (N renames) | no | no | **YES** (N target slots sorted, locking §5) | **yes** (whole-row, no quota) | P-5 | switch `:298-303`; factory `:152-153`; class `OMKeysRenameRequest.java` |
| B4 | DeleteOpenKeys | `OMOpenKeysDeleteRequest` | openKeyTable (remove N), deletedTable (orphaned blocks) | usage (decr for orphaned data) | no | **YES** (batch cleanup of expired open keys) | **yes** (re-delete of gone open key is a no-op) | P-5 | `OMOpenKeysDeleteRequest.java:188,205`; switch `:234-240` |
| B5 | PurgeKeys | `OMKeyPurgeRequest` | deletedTable (final block-purge), snapshotInfoTable (snapshot-scoped) | no (already decremented at delete) | no | **YES** (background batch; snapshot-aware) | **yes** (purge of absent is a no-op) | P-5 | `OMKeyPurgeRequest.java:83,100,137`; switch `:196-197` |
| B6 | CreateBucket | `OMBucketCreateRequest` | bucketTable (put), volumeTable (cache) | **usage** (`incrUsedNamespace(1L)` on volume) | no | no | **yes** (exists-check rejects dup) | P-1 (structural; lands with key path) | `OMBucketCreateRequest.java:248,273,277,279`; switch `:162-163` |
| B7 | DeleteBucket | `OMBucketDeleteRequest` | bucketTable (remove), volumeTable (cache update) | **usage** (volume `usedNamespace` decremented via volume-args path) | no | no | **yes** (non-empty rejected; absent is rejected) | P-1 (structural) | switch `:164-165`; `OMBucketDeleteRequest.java:167` (bucketTable),`:177,186` (volumeTable) |
| B8 | SetBucketProperty | `OMBucketSetPropertyRequest` / `OMBucketSetOwnerRequest` | bucketTable (put) | **limit** (`setQuotaInBytes` — the quota *limit*, not usage) | no | no | **yes** (whole-row property put) | P-6 (simple) but **quota-limit aware** | `OMBucketSetPropertyRequest.java:189,211,280-316`; switch `:166-173` |

> **Why B6/B7/CreateBucket are "core" not "simple" despite being single structural ops.** They
> touch **two** structural tables (volume + bucket) and they move the **volume's** `usedNamespace`
> (verified `OMBucketCreateRequest.java:273` `incrUsedNamespace(1L)` on the volume args), so they
> are quota-usage-bearing even though no SCM and no multi-step. They are listed adjacent to P-1 so
> the bucket-level usedNamespace counter is on the commutative-Merge model *before* the key path
> increments bucket-level usedBytes/usedNamespace under it — you cannot have keys updating
> bucket usage on the new path while the bucket's own create/delete still mutates volume usage on
> the legacy path without reasoning about the seam. SetBucketProperty (B8) only writes a quota
> *limit* (a property), never usage, so it sweeps with the easy ops (P-6) — but the inventory flags
> it explicitly so nobody mistakes a limit write for a usage Merge.

### 4.3 Group A — migration-simple (~22)  → P-6 (easy Set-A sweep)

All Group-A ops share the shape: **single column family**, **no quota usage** (some carry a quota
*limit* property, flagged), **no SCM**, **single transition**, **whole-row `Put`/`Delete`**,
**idempotent on re-execution**. They are the leg-work D-13 defers to last (P-6), behind a
per-command flag, after the hard machinery is proven.

| # | Type | Request class | Table mutated | Quota / SCM / Multi-step | Idempotent? | Evidence (file:line) |
|---|---|---|---|---|---|---|
| A1 | CreateVolume | `OMVolumeCreateRequest` | volumeTable | limit only / no / no | yes | switch `:144-145`; `OMVolumeCreateRequest.java:154` |
| A2 | SetVolumeProperty (quota) | `OMVolumeSetQuotaRequest` | volumeTable | **limit** / no / no | yes | switch `:146-159` |
| A3 | SetVolumeProperty (owner) | `OMVolumeSetOwnerRequest` | volumeTable | no / no / no | yes | switch `:146-159` |
| A4 | DeleteVolume | `OMVolumeDeleteRequest` | volumeTable | no / no / no | yes (non-empty rejected) | switch `:160-161` |
| A5 | AddAcl (vol/bucket/key/prefix) | `OMVolumeAddAclRequest` / `OMBucketAddAclRequest` / `OMKeyAddAclRequest(+WithFSO)` / `OMPrefixAddAclRequest` | volumeTable / bucketTable / keyTable(fileTable) / prefixTable | no / no / no | yes (set-union semantics) | `getOMAclRequest` `:354-373` |
| A6 | RemoveAcl (vol/bucket/key/prefix) | `OM*RemoveAclRequest(+WithFSO)` | as A5 | no / no / no | yes (set-difference) | `getOMAclRequest` `:374-390` |
| A7 | SetAcl (vol/bucket/key/prefix) | `OM*SetAclRequest(+WithFSO)` | as A5 | no / no / no | yes (whole-row ACL replace) | `getOMAclRequest` `:392-…` |
| A8 | GetDelegationToken | `OMGetDelegationTokenRequest` | (delegation token table) | no / no / no | yes (token persisted whole) | switch `:178-179` |
| A9 | RenewDelegationToken | `OMRenewDelegationTokenRequest` | (delegation token table) | no / no / no | yes | switch `:182-183` |
| A10 | CancelDelegationToken | `OMCancelDelegationTokenRequest` | (delegation token table) | no / no / no | yes (cancel of gone is a no-op) | switch `:180-181` |
| A11 | GetS3Secret | `S3GetSecretRequest` | s3SecretTable | no / no / no | yes (whole-row secret put) | switch `:184-185` |
| A12 | SetS3Secret | `OMSetSecretRequest` | s3SecretTable | no / no / no | yes | switch `:192-193` |
| A13 | RevokeS3Secret | `S3RevokeSecretRequest` | s3SecretTable | no / no / no | yes (revoke of gone is a no-op) | switch `:194-195` |
| A14 | CreateTenant | `OMTenantCreateRequest` | tenantStateTable, tenantAccessIdTable, volumeTable | no / no / no | yes (mints objId — needs ManagedIndex D-12) | switch `:200-202`; `OMTenantCreateRequest.java:292` |
| A15 | DeleteTenant | `OMTenantDeleteRequest` | tenantStateTable | no / no / no | yes | switch `:203-205` |
| A16 | TenantAssignUserAccessId | `OMTenantAssignUserAccessIdRequest` | tenantAccessIdTable, principalToAccessIdsTable | no / no / no | yes | switch `:206-208` |
| A17 | TenantRevokeUserAccessId | `OMTenantRevokeUserAccessIdRequest` | tenantAccessIdTable | no / no / no | yes | switch `:209-211` |
| A18 | TenantAssignAdmin | `OMTenantAssignAdminRequest` | tenantAccessIdTable | no / no / no | yes | switch `:212-214` |
| A19 | TenantRevokeAdmin | `OMTenantRevokeAdminRequest` | tenantAccessIdTable | no / no / no | yes | switch `:215-217` |
| A20 | SetRangerServiceVersion | `OMSetRangerServiceVersionRequest` | (ranger-version meta) | no / no / no | yes | switch `:218-219` |
| A21 | RenameSnapshot | `OMSnapshotRenameRequest` | snapshotInfoTable | no / no / no | yes (whole-row rename) | switch `:224-225` |
| A22 | SetSnapshotProperty | `OMSnapshotSetPropertyRequest` | snapshotInfoTable | no / no / no | yes | switch `:232-233` |
| A23 | SetTimes | `OMKeySetTimesRequest` / `OMKeySetTimesRequestWithFSO` | keyTable / fileTable | no / no / no | yes (whole-row mtime put) | `OMKeySetTimesRequest.java:236`; factory `:189-194` |
| A24 | PutObjectTagging | `S3PutObjectTaggingRequest` / `...WithFSO` | keyTable / fileTable | no / no / no | yes (whole-row tag put) | `S3PutObjectTaggingRequest.java:139`; factory `:198-202` |
| A25 | DeleteObjectTagging | `S3DeleteObjectTaggingRequest` / `...WithFSO` | keyTable / fileTable | no / no / no | yes (whole-row tag clear) | factory `:206-210`; switch `:340-343` |
| A26 | Prepare | `OMPrepareRequest` | (prepare marker; quiesces double buffer) | no / no / no | yes (idempotent marker) | switch `:188-189` |
| A27 | CancelPrepare | `OMCancelPrepareRequest` | (prepare marker) | no / no / no | yes | switch `:190-191` |
| A28 | FinalizeUpgrade | `OMFinalizeUpgradeRequest` | (layout-version meta) | no / no / no | yes (one-way, A-3) | switch `:186-187` |
| A29 | RecoverLease | `OMRecoverLeaseRequest` | openKeyTable/fileTable (FSO only) | no / no / no | yes (lease recovery converges) | switch `:241-251` |
| A30 | EchoRPC | `OMEchoRPCWriteRequest` | (none — test/echo) | no / no / no | yes (no-op) | switch `:329-330` |
| A31 | QuotaRepair | `OMQuotaRepairRequest` | bucketTable (recompute usedBytes/usedNamespace) | recompute / no / no | yes (idempotent reconcile) | switch `:333-334` |

> **Counting note.** §4.3 lists 31 numbered rows but several collapse the ACL fan-out (A5/A6/A7 each
> cover 4 resource-typed classes) and the per-property SetVolumeProperty split (A2/A3). The master
> scaffold's "~22 single-table ops" counts at the **`Type`-with-distinct-planner** grain — ACLs as
> one migration unit (`AddAcl`/`RemoveAcl`/`SetAcl` share `getOMAclRequest`), the two
> SetVolumeProperty variants as one, the prepare/finalize cluster as one. The "~" in "~22" is exactly
> this fan-out latitude; the inventory is intentionally listed at the finer class grain so no class
> is missed, and the count reconciles to ~22 *migration units* at the `Type` grain. **`QuotaRepair`
> (A31) is listed in Group A as a single-bucket reconcile**, but note it relates to quota enforcement
> (see §6.1): with D-OPEN-quota-enforcement resolved to **exact** admission (the leader-local reservation
> is the gate), `QuotaRepair` is no longer the quota-enforcement *mitigation path* — it remains a
> background counter-reconcile/repair backstop, so its migration ordering has no special quota pull.

---

## 5. Per-phase playbook (P-0 … P-8)

Each phase below reproduces its master §29 `P-n` block verbatim (the frozen contract), then gives
the per-command PR/sub-task breakdown, the config flag, the prior-phase dependency, and the
acceptance gate.

### P-0 — Framework substrate + the three prerequisites (inert)

```yaml
- {id: P-0, scope: "framework substrate (12 components) unwired + legacy→ManagedIndex objectID retrofit + dual-path index durability + cross-model one-shared-lock-manager + cache-coherent reads + migrated-writes-via-shared-drain (D-17)", depends_on_phases: [], must_satisfy: [I-inner-domain-agnostic, I-txninfo-atomic-with-patch, I-managed-index-monotonic, I-mixed-shared-lock, I-mixed-mode-cache-coherent, I-mixed-write-order], must_pass: [T-cross-thread-release, T-objectid-disjoint, T-proto-roundtrip, T-mixed-mode-cross-model-race, T-mixed-mode-stale-read, T-mixed-write-reorder], config_flag: "n/a (inert)", acceptance: "zero behavior change; all unit tests green; lint-spec passes"}
```

**Command set.** None migrated. This phase builds the 12 components (master §11) and the three
prerequisites (§3 above) and wires nothing into the live write path.

**Sub-tasks / PRs (one component or prerequisite each — keep them small and independently
mergeable):**

- **PR-0a** — Legacy → ManagedIndex objectID retrofit (§3.1). Redirect every `getObjectIdFromTxId`
  call site (enumerated §3.1) to the shared `ManagedIndexService` counter; retire the 256-window
  (D-8). Tests: `T-objectid-disjoint`, `T-mixed-mode-no-collision`.
- **PR-0b** — Dual-path applied-index durability (§3.2). Extend `lastSkippedIndex`
  (`OzoneManagerStateMachine.java:108-111,242-269,582`) to a dual-path crash-consistent stamp.
  Tests: `T-txninfo-crash-atomicity` (crash mid-apply re-applies exactly once).
- **PR-0c** — `OMLayoutFeature.LEADER_SIDE_EXECUTION(10)` + finalization gate (§3.3); `PersistDb`
  proto + `#MANAGED_INDEX` entry, additive and inert. Tests: `T-rolling-upgrade-mixed-binary`.
- **PR-0d** — replicated-DB module (`Batch{Put/Delete/Merge/Checkpoint}`) in `hadoop-hdds/framework`
  (D-1, D-2). Tests: `T-proto-roundtrip`, `T-determinism-follower-byte-identical` (harness scaffold).
- **PR-0e** — lean lock manager (D-4, D-15): striped non-thread-affine RW semaphore (locking §6).
  Tests: `T-cross-thread-release` (acquire thread A / release thread B), `T-7` scaffold.
- **PR-0f** — orchestrator / `LeaderPlanner` + `PlannedRequest` base + change recorder (D-6).
- **PR-0g** — dual-path state machine routing (the flag dispatcher, D-14) — inert; default legacy.
- **PR-0h** — quota merge operator, Option B (D-7) registered on every node (A-5) — inert until P-1.
- **PR-0i** — test harness: linearizability checker + sequential reference model (locking §7),
  short-circuit-DB-without-Ratis testability (master §27).

**Config flag.** None — everything inert. (The flag dispatcher PR-0g exists but every key defaults
legacy.)

**Dependencies.** None (root phase).

**Acceptance gate.** Zero behavior change (every existing integration test green with no flag set);
`T-cross-thread-release`, `T-objectid-disjoint`, `T-proto-roundtrip` green; `lint-spec` passes;
the merge operator is registered on every node *before* any node can receive a `Merge` (A-5 — a
node that receives a `Merge` it cannot resolve violates D-10 and crashes). **No command may proceed
to P-1 until all three prerequisites (PR-0a/b/c) are merged.**

```yaml
# P-0 playbook block (companion expansion; master §29 holds the canonical P-0)
id: P-0
scope: "12 framework components unwired + 3 prerequisites: ManagedIndex objectID retrofit (PR-0a), dual-path applied-index durability (PR-0b), OMLayoutFeature finalization gate (PR-0c) + cross-model one-shared-lock-manager + cache-coherent reads + migrated-writes-via-shared-drain (D-17)"
depends_on_phases: []
must_satisfy: [I-inner-domain-agnostic, I-txninfo-atomic-with-patch, I-managed-index-monotonic, I-mixed-shared-lock, I-mixed-mode-cache-coherent, I-mixed-write-order]
must_pass: [T-cross-thread-release, T-objectid-disjoint, T-proto-roundtrip, T-mixed-mode-cross-model-race, T-mixed-mode-stale-read, T-mixed-write-reorder]
config_flag: "n/a (inert)"
acceptance: "zero behavior change; PR-0a/b/c merged; merge operator registered on all nodes (A-5); lint-spec green"
provenance: inferred
evidence: ["master §29 P-0", "OmUtils.java:766-783", "OzoneManagerStateMachine.java:108-111,242-269,582", "OMLayoutFeature.java:26-47", "OMBucketCreateRequest.java:421,444"]
```

### P-1 — Hardest single-step OBS key path

```yaml
- {id: P-1, scope: "hardest single-step OBS: CreateKey, CommitKey, AllocateBlock, DeleteKey, CreateBucket, DeleteBucket", depends_on_phases: [P-0], must_satisfy: [I-quota-commutative, I-cache-free-ryw, I-quota-admission-exact, I-quota-reservation-lifecycle], must_pass: [T-quota-concurrent, T-ryw-from-db, T-quota-failover, T-quota-leader-flap], config_flag: "ozone.om.leader.execution.obs.key.enabled", acceptance: "OBS key path on new model; perf ≥ baseline; quota correct; production flag gated on D-OPEN-retry closure (durable retry) for the four non-idempotent ops — dev/staging may precede"}
```

**Command set.** The OBS halves of C1 CreateKey, C2 CommitKey, C3 AllocateBlock, C4 DeleteKey. (The
FSO halves of these same `Type`s land in P-2 — see the "half a command" warning in §0. The flag is
per-`Type`, so until P-2 the FSO halves stay legacy; that is a deliberate, supported mixed state.)
Structural B6 CreateBucket / B7 DeleteBucket migrate adjacent here so bucket `usedNamespace` is on
the Merge model under the key path (§4.2 note).

**Sub-tasks / PRs (one `PlannedRequest` per command + tests + flag wiring):**

- **PR-1.1** `CreateKeyPlannedRequest` (OBS) — plan: authorize, allocate SCM blocks on the leader
  (the non-deterministic step, C1 evidence `OMKeyCreateRequest.java:165`), emit openKeyTable `Put`.
  Tests: `T-ryw-from-db` (successor read sees committed bytes, no cache, D-3/D-5).
- **PR-1.2** `CommitKeyPlannedRequest` (OBS) — plan: keyTable `Put`, openKeyTable `Delete`,
  deletedTable `Put` for overwrite soft-delete, **quota `Merge`** for `usedBytes`/`usedNamespace`
  (C2 evidence `:407,410,378`). Tests: `T-quota-concurrent` (N parallel commits — exact admission via
  the reservation, D-OPEN-quota-enforcement resolved; over-commit only in the failover window, EXC-3),
  `T-quota-failover`, `T-quota-leader-flap`.
- **PR-1.3** `AllocateBlockPlannedRequest` (OBS) — plan: leader SCM `allocateBlock`, openKeyTable
  block-append `Put` (C3 evidence `:115`).
- **PR-1.4** `DeleteKeyPlannedRequest` (OBS) — plan: keyTable tombstone, deletedTable `Put`, quota
  `Merge` decrement (C4 evidence `:155,166,167`).
- **PR-1.5** structural `CreateBucketPlannedRequest` / `DeleteBucketPlannedRequest` (B6/B7) — volume
  `usedNamespace` Merge.
- **PR-1.6** flag registration `ozone.om.leader.execution.obs.key.enabled` (default false) +
  `T-flag-routing-both-paths` proving legacy-path and new-path produce byte-identical DB for the
  CreateKey→AllocateBlock→CommitKey→DeleteKey lifecycle.

**Config flag.** `ozone.om.leader.execution.obs.key.enabled` (default legacy/false, D-14).

**Dependencies.** P-0 (all three prerequisites; the merge operator must be live for PR-1.2/1.4 quota
Merges; the ManagedIndex retrofit must be live or CreateKey's objectIDs collide with still-legacy
FSO creates).

**Acceptance gate.** OBS key path fully on the new model under the flag; `T-quota-concurrent` and
`T-ryw-from-db` green; benchmark ≥ prototype 40k baseline (master §27); flag-routing equivalence
holds; quota counter `UsedConsistent` and **exact admission** via the leader-local reservation
(`I-quota-admission-exact`; `T-quota-leader-flap` green for the reserve lifecycle). **Admission is
resolved exact by D-OPEN-quota-enforcement; over-commit is bounded to the failover window
(`B-quota-failover-window`). P-1 lands the commutative Merge AND the reservation.**

```yaml
id: P-1
scope: "OBS halves of CreateKey, CommitKey, AllocateBlock, DeleteKey; structural CreateBucket/DeleteBucket"
depends_on_phases: [P-0]
must_satisfy: [I-quota-commutative, I-cache-free-ryw, I-quota-admission-exact, I-quota-reservation-lifecycle]
must_pass: [T-quota-concurrent, T-ryw-from-db, T-quota-failover, T-quota-leader-flap]
config_flag: "ozone.om.leader.execution.obs.key.enabled"
acceptance: "OBS key lifecycle on new model; perf ≥ 40k baseline; UsedConsistent holds; flag-routing byte-identical; production flag gated on D-OPEN-retry closure (durable retry) for the four non-idempotent ops — dev/staging may precede"
provenance: inferred
evidence: ["master §29 P-1", "OMKeyCreateRequest.java:165,338,351", "OMKeyCommitRequest.java:407,410,378,288", "OMAllocateBlockRequest.java:115", "OMKeyDeleteRequest.java:155,166,167"]
```

### P-2 — Hardest multi-step FSO + recursive delete

```yaml
- {id: P-2, scope: "hardest multi-step FSO: CreateFile/CreateDirectory (implicit parents), FSO delete, recursive rm-rf + DirectoryDeletingService redesign", depends_on_phases: [P-1], must_satisfy: [I-3, I-5, I-6, I-7, I-11], must_pass: [T-1, T-2, T-3, T-4, T-5, T-6, T-7, T-8], config_flag: "ozone.om.leader.execution.fso.enabled", acceptance: "FSO linearizable under T-1..T-8; no orphan; showstoppers retired; gated-open: [Q-rename-mtime-merge] (FSO cross-parent rename mtime merge-operator interaction, resolve within P-2 — §30)"}
```

**Command set.** C5 CreateFile (FSO, multi-step mkdir-p), C6 CreateDirectory (FSO, multi-step),
the FSO halves of C1/C2/C3/C4 (CreateFile-FSO/CommitKey-FSO/AllocateBlock-FSO/DeleteKey-FSO), C7
DeleteKey-FSO-recursive (`rm -rf`), C8 PurgeDirectories + the `DirectoryDeletingService` redesign.

**Sub-tasks / PRs:**

- **PR-2.1** `CreateDirectoryPlannedRequest` (FSO) — the orchestration archetype: decompose
  `create /a/b/c` with missing `/a/b` into an **ordered chain** of per-step-locked transitions
  (D-6; locking §4.1), each created dir getting one ManagedIndex objectID (D-8, retires the
  256-window). Reference model = iterative mkdir-p (D-16). Tests: `T-5` (deep mkdirs vs ancestor
  rename), `T-8` (failover mid-orchestration, retry-cache on terminal step only).
- **PR-2.2** `CreateFilePlannedRequest` (FSO) — same chain + terminal SCM-bearing open-file
  (C5 evidence `OMFileCreateRequestWithFSO.java:154,195`). Non-atomic on partial failure is the
  *defined* contract (B-2, D-16), not a bug.
- **PR-2.3** `CommitKeyPlannedRequest(FSO)` / `AllocateBlockPlannedRequest(FSO)` — FSO container/slot
  locks (locking §2.1) layered on the P-1 planners. Tests: `T-4` (delete-empty vs create-child),
  `T-6` (rename vs delete same node), `T-7` (hot-parent throughput).
- **PR-2.4** `DeleteKeyPlannedRequest(FSO, recursive)` — synchronous root tombstone (locking §4.2;
  C7 evidence `:142`) so I-5 makes new descending resolutions fail immediately.
- **PR-2.5** `DirectoriesPurgePlannedRequest` + **`DirectoryDeletingService` redesign** — the
  decomposed per-node-locked background purge honoring I-7 (no orphan), lazy quota release (EXC-1).
  C8 evidence `OMDirectoriesPurgeRequestWithFSO.java:154,158,192,193`. This is the master §19
  AFFECTED "DirectoryDeletingService recursive-delete redesign". Tests: `T-1` (create-under-rm-rf,
  no orphan), `T-2` (already-purged parent).
- **PR-2.6** flag `ozone.om.leader.execution.fso.enabled` + the full `T-1..T-8` linearizability
  suite as the gate.

**Config flag.** `ozone.om.leader.execution.fso.enabled` (default legacy, D-14).

**Dependencies.** P-1 (the OBS key planners and the quota Merge are reused by the FSO commit/delete;
FSO is OBS + parent resolution + container/slot locks).

**Acceptance gate.** FSO path linearizable under `T-1..T-8` (locking §7 — the bar is
linearizability, not "op X always succeeds"); **no orphan** (I-7) under concurrent
create-vs-`rm -rf`; deadlock-free (I-11) under crossing renames; showstoppers (multi-step FSO,
recursive delete) retired. This is the highest-risk gate in the plan (D-13's whole point); it must
clear before P-3/P-4.

**gated-open: [Q-rename-mtime-merge].** Master open question (status `open`,
`leader-planned-execution.md` open-questions ledger; evidence locking §10): the exact
merge-operator interaction for FSO directory mtime updates on a cross-parent rename is unspecified
and is to be resolved **within P-2**. P-2 cannot declare its acceptance gate met until this is
settled, because the FSO commit/rename planners landed here (PR-2.3) drive that mtime `Merge`. Not
fabricating a resolution — surfacing the blocking open gate so the phase reader sees it.

```yaml
id: P-2
scope: "FSO CreateFile/CreateDirectory (multi-step mkdir-p), FSO commit/allocate/delete, rm-rf root tombstone, DirectoryDeletingService redesign + PurgeDirectories"
depends_on_phases: [P-1]
must_satisfy: [I-3, I-5, I-6, I-7, I-11]
must_pass: [T-1, T-2, T-3, T-4, T-5, T-6, T-7, T-8]
config_flag: "ozone.om.leader.execution.fso.enabled"
acceptance: "linearizable under T-1..T-8; no orphan (I-7); deadlock-free (I-11); showstoppers retired"
provenance: inferred
evidence: ["master §29 P-2", "leader-execution-locking.md §4.1,§4.2,§2.1,I-5,I-6,I-7", "OMFileCreateRequestWithFSO.java:154,195", "OMDirectoryCreateRequestWithFSO.java:138,148", "OMKeyDeleteRequestWithFSO.java:142", "OMDirectoriesPurgeRequestWithFSO.java:154,158,192,193"]
```

### P-3 — Snapshot (Checkpoint op + moves + standalone purge)

```yaml
- {id: P-3, scope: "snapshot: CreateSnapshot/Checkpoint op, SnapshotPurge standalone, moves", depends_on_phases: [P-2], must_satisfy: [I-checkpoint-exact-index], must_pass: [T-snapshot-consistency], config_flag: "ozone.om.leader.execution.snapshot.enabled", acceptance: "snapshot image == DB@createTransactionInfo index"}
```

**Command set.** C9 CreateSnapshot (the `Checkpoint` op, which subsumes the legacy snapshot barrier,
D-1), C10 SnapshotPurge (standalone), C11 SnapshotMoveDeletedKeys, C12 SnapshotMoveTableKeys. (A21
RenameSnapshot / A22 SetSnapshotProperty are simple snapshotInfoTable property writes and can ride
along here or defer to P-6 — they carry no Checkpoint/move hazard.)

**Sub-tasks / PRs:**

- **PR-3.1** `CreateSnapshotPlannedRequest` — emit a `Checkpoint` op at the **exact**
  createTransactionInfo index so the snapshot image equals the DB at that index
  (`I-checkpoint-exact-index`; C9 evidence `OMSnapshotCreateRequest.java:275`). The Checkpoint op is
  the replicated-module primitive that replaces `splitReadyBufferAtCreateSnapshot` (master §9).
- **PR-3.2** `SnapshotPurgePlannedRequest` (standalone, master §19/§29) — snapshotInfoTable
  remove + checkpoint delete; already-purged is a no-op (C10 evidence `:107-109`).
- **PR-3.3** `SnapshotMoveDeletedKeysPlannedRequest` / `SnapshotMoveTableKeysPlannedRequest` — the
  cross-snapshot deletedTable/deletedDirTable **moves**. These are **non-idempotent on
  re-execution** (C11/C12) and are therefore prime candidates for the durable retry entry once
  D-OPEN-retry settles (§6.2).
- **PR-3.4** flag `ozone.om.leader.execution.snapshot.enabled` + `T-snapshot-consistency`.

**Config flag.** `ozone.om.leader.execution.snapshot.enabled`.

**Dependencies.** P-2 (snapshot Checkpoint ordering vs in-flight fine-grained FSO ops — locking §10
open item; the Checkpoint must order correctly against the fine-grained locks introduced in P-2).

**Acceptance gate.** Snapshot image byte-equals the DB at the createTransactionInfo index
(`I-checkpoint-exact-index`); `T-snapshot-consistency` green; moves do not lose or double-count
reclaim accounting.

```yaml
id: P-3
scope: "CreateSnapshot (Checkpoint op), SnapshotPurge standalone, SnapshotMoveDeletedKeys, SnapshotMoveTableKeys"
depends_on_phases: [P-2]
must_satisfy: [I-checkpoint-exact-index]
must_pass: [T-snapshot-consistency]
config_flag: "ozone.om.leader.execution.snapshot.enabled"
acceptance: "snapshot image == DB@createTransactionInfo index; moves reclaim-accurate"
provenance: inferred
evidence: ["master §29 P-3", "OMSnapshotCreateRequest.java:166,275", "OMSnapshotPurgeRequest.java:97-102,107,121", "OMSnapshotMoveDeletedKeysRequest.java:79-93"]
```

### P-4 — MPU + large-value revisit

```yaml
- {id: P-4, scope: "MPU (4 ops + AbortExpired) + large-value ([HDDS-8238](https://issues.apache.org/jira/browse/HDDS-8238)) revisit", depends_on_phases: [P-3], must_satisfy: [], must_pass: [T-mpu-lifecycle], config_flag: "ozone.om.leader.execution.mpu.enabled", acceptance: "MPU on new model"}
```

**Command set.** C13 InitiateMultiPartUpload, C14 CommitMultiPartUpload (commit part), C15
CompleteMultiPartUpload, C16 AbortMultiPartUpload, C17 AbortExpiredMultiPartUploads. Plus the
**large-value revisit** (RC-xichen-large-value / [HDDS-8238](https://issues.apache.org/jira/browse/HDDS-8238)): whole-object replication of a
completed multi-GB MPU object is the case xichen01 flagged as big network overhead (master §23, §30
R-mpu-large-value). This phase confronts it; it is `open` in the master ledger and may produce its
own sub-decision.

**Sub-tasks / PRs:**

- **PR-4.1** `InitiateMPUPlannedRequest` — openKeyTable + multipartInfoTable `Put`, ManagedIndex
  objectID (C13 evidence `S3InitiateMultipartUploadRequest.java:139,221,224`).
- **PR-4.2** `CommitMPUPartPlannedRequest` — multipartInfoTable + openKeyTable `Put`, deletedTable
  for replaced part, quota `Merge` (C14 evidence `:223,227,257`).
- **PR-4.3** `CompleteMPUPlannedRequest` — assemble N parts into keyTable, delete openKey + MPU info,
  quota `Merge` (C15 evidence `:334,341,570`). **Multi-step** (assembles N parts; FSO parent
  resolution). This is where the large-value concern is sharpest (assembled object can be huge).
- **PR-4.4** `AbortMPUPlannedRequest` / `AbortExpiredMPUPlannedRequest` (C16/C17) — background
  sweep; idempotent re-abort.
- **PR-4.5** flag `ozone.om.leader.execution.mpu.enabled` + `T-mpu-lifecycle`. If the large-value
  revisit produces a decision (e.g. block-list delta instead of whole-object for completed MPU),
  it gets its own `D-n` in the master and a sub-PR here.

**Config flag.** `ozone.om.leader.execution.mpu.enabled`.

**Dependencies.** P-3 (MPU interacts with snapshots for deletedTable accounting; the snapshot move
ops from P-3 reclaim MPU-orphaned parts).

**Acceptance gate.** Full MPU lifecycle (initiate → commit parts → complete | abort) on the new
model; `T-mpu-lifecycle` green; large-value behavior characterized (and, if a sub-decision is made,
implemented). RC-xichen-large-value moves from `open` toward a resolution or an explicit deferral.

```yaml
id: P-4
scope: "InitiateMPU, CommitMPUPart, CompleteMPU, AbortMPU, AbortExpiredMPU; HDDS-8238 large-value revisit (RC-xichen-large-value)"
depends_on_phases: [P-3]
must_satisfy: []
must_pass: [T-mpu-lifecycle]
config_flag: "ozone.om.leader.execution.mpu.enabled"
acceptance: "MPU lifecycle on new model; large-value behavior characterized"
provenance: inferred
evidence: ["master §29 P-4", "master §23 RC-xichen-large-value", "S3InitiateMultipartUploadRequest.java:139,221,224", "S3MultipartUploadCommitPartRequest.java:223,227,257", "S3MultipartUploadCompleteRequest.java:334,341,570"]
```

### P-5 — Batch / background key ops

```yaml
- {id: P-5, scope: "batch/background: DeleteKeys, RenameKey/Keys, DeleteOpenKeys, PurgeKeys/Directories", depends_on_phases: [P-2], must_satisfy: [], must_pass: [T-batch-quota-no-double-decrement], config_flag: "per-command", acceptance: "leg work complete"}
```

**Command set.** B1 DeleteKeys (batch), B2 RenameKey, B3 RenameKeys (batch), B4 DeleteOpenKeys,
B5 PurgeKeys. (C8 PurgeDirectories already landed in P-2 with the recursive-delete redesign; it is
named in the master P-5 scope line for completeness but its code home is P-2.)

**Sub-tasks / PRs:** one `PlannedRequest` per command, each with a multi-slot lock set sorted by the
locking comparator (locking §5):

- **PR-5.1** `DeleteKeysPlannedRequest` (B1) — N keys, multi-slot, quota Merge decrement per key
  (non-idempotent on re-exec).
- **PR-5.2** `RenameKeyPlannedRequest` (B2) — keyTable put+delete (FSO: O(1) re-parent, F-2);
  idempotent, no quota.
- **PR-5.3** `RenameKeysPlannedRequest` (B3) — N renames, sorted multi-slot.
- **PR-5.4** `DeleteOpenKeysPlannedRequest` (B4) — batch cleanup; deletedTable for orphaned blocks.
- **PR-5.5** `PurgeKeysPlannedRequest` (B5) — final block-purge of deletedTable, snapshot-aware
  (B5 evidence `OMKeyPurgeRequest.java:83,100,137`).
- **PR-5.6** per-command flags (one each; master `config_flag: "per-command"`).

**Config flag.** Per-command (e.g. `ozone.om.leader.execution.obs.deletekeys.enabled`, etc.).

**Dependencies.** P-2 (these batch/background ops share the FSO container/slot locking and the
deletedTable/deletedDirTable plumbing introduced in P-2's recursive-delete work).

**Acceptance gate.** Batch ops linearizable (no deadlock under multi-slot acquisition, I-11);
leg-work complete. No new invariant beyond those proven in P-1/P-2.

```yaml
id: P-5
scope: "batch/background: DeleteKeys, RenameKey, RenameKeys, DeleteOpenKeys, PurgeKeys (PurgeDirectories code-homes in P-2)"
depends_on_phases: [P-2]
must_satisfy: []
must_pass: [T-batch-quota-no-double-decrement]
config_flag: "per-command"
acceptance: "batch ops linearizable; multi-slot deadlock-free; leg work complete"
provenance: inferred
evidence: ["master §29 P-5", "leader-execution-locking.md §5", "OMKeyPurgeRequest.java:83,100,137", "OMKeysDeleteRequest.java", "OMKeysRenameRequest.java", "OMOpenKeysDeleteRequest.java:188,205"]
```

### P-6 — Easy Set-A sweep (~22 single-table ops)

```yaml
- {id: P-6, scope: "easy Set-A sweep (~22 single-table ops: ACLs, tagging, SetTimes, secrets, tokens, tenant, snapshot props, vol/bucket props, prepare)", depends_on_phases: [P-1], must_satisfy: [], must_pass: [], config_flag: "per-command", acceptance: "legacy path retired for simple ops; gated-open: [Q-setacl-settimes-placement, Q-hsync-lease] (SetAcl/SetTimes lock placement + hsync/lease-recovery X(parent,file) interaction, resolve before these ops migrate in P-6 — §30)"}
```

**Command set.** All of Group A (§4.3): A1-A31 (collapsing to ~22 migration units at the `Type`
grain — see the §4.3 counting note). Volume create/delete/setquota/setowner (A1-A4), the ACL family
(A5-A7), delegation tokens (A8-A10), S3 secrets (A11-A13), tenancy (A14-A20), snapshot props
(A21-A22), SetTimes (A23), object tagging (A24-A25), prepare/finalize cluster (A26-A28),
RecoverLease (A29), EchoRPC (A30), QuotaRepair (A31), plus B8 SetBucketProperty (the quota-*limit*
property write).

**Sub-tasks / PRs:** these are mechanical — each is a single-table whole-row `Put`/`Delete` planner.
Batch them by table family to keep PR count sane (e.g. one PR for the ACL family across all four
resource types since they share `getOMAclRequest` planning; one for the tenancy family; one for
tokens; one for secrets). Each PR: planner + flag + `T-flag-routing-both-paths` for that family.

**Config flag.** Per-command (or per-family).

**Dependencies.** P-1 only (master §29 — deliberately gated *behind* the hard P-1, never used to
declare premature victory, per §1). Independent of P-2/P-3/P-4 because these ops touch single
non-key tables with no FSO/snapshot/MPU interaction.

**Acceptance gate.** Legacy path retired for all simple ops under their flags; flag-routing
byte-identical for each. No new invariant.

**gated-open: [Q-setacl-settimes-placement, Q-hsync-lease].** Two master open questions (both
status `open`, `leader-planned-execution.md` open-questions ledger; evidence locking §10) land on
P-6's command set and must be resolved before the ops they cover migrate here:
(1) **Q-setacl-settimes-placement** — the `AllocateBlock` clause is RESOLVED (locking §2.1, Batch 3,
which is why P-1 is unblocked on that op), but lock placement for **SetAcl/SetTimes** (expected
`S(bucket)+S(P)+X(P,name)`, locking §2.1 coverage map) is **still open** and must be confirmed before those
ops — A5-A7 (ACL family) and A23 (SetTimes) in P-6's command set — migrate. (2) **Q-hsync-lease** —
the hsync / lease-recovery interaction with `X(parent, file)` on commit is unspecified and must be
resolved before the hsync/lease commands migrate; RecoverLease (A29) is in P-6's command set. Not
fabricating resolutions — surfacing the blocking open gates consistently with the master.

```yaml
id: P-6
scope: "Group A simple ops (~22 units): volume ops, ACL family, tokens, S3 secrets, tenancy, snapshot props, SetTimes, object tagging, prepare/finalize, RecoverLease, EchoRPC, QuotaRepair, SetBucketProperty(limit)"
depends_on_phases: [P-1]
must_satisfy: []
must_pass: []
config_flag: "per-command"
acceptance: "legacy path retired for simple ops; flag-routing byte-identical"
provenance: inferred
evidence: ["master §29 P-6", "getOMAclRequest OzoneManagerRatisUtils.java:354-373", "OMKeySetTimesRequest.java:236", "S3PutObjectTaggingRequest.java:139", "OMBucketSetPropertyRequest.java:189,211"]
```

### P-7 — Cleanup: remove double buffer + table cache; delete legacy path

```yaml
- {id: P-7, scope: "cleanup: remove double buffer + table cache; delete legacy path; drop per-command flags (OMLayoutFeature finalization already crossed pre-P-1, §16.1)", depends_on_phases: [P-3, P-4, P-5, P-6], must_satisfy: [], must_pass: [], config_flag: "n/a", acceptance: "double buffer gone; single execution model"}
```

**Command set.** None new. This phase **deletes**: the double buffer (`OzoneManagerDoubleBuffer`),
the OM table caches (D-3), and every legacy `OMClientRequest.validateAndUpdateCache` body now that
all commands route to `PlannedRequest`. Then it drops the per-command flags (D-14): once every
command is on the new path — and finalization was already crossed cluster-wide before P-1 (§16.1) —
the legacy fallback has no callers.

**Sub-tasks / PRs:**

- **PR-7.1** delete the double buffer; the replicated-DB module's apply path is now the sole RocksDB
  writer (the #TRANSACTIONINFO-atomic-with-patch invariant `I-txninfo-atomic-with-patch` is now the *only*
  durability path, not the dual-path of P-0).
- **PR-7.2** remove OM table caches (D-3); reads go straight to RocksDB (A-1: NVMe + block cache).
- **PR-7.3** delete the legacy `validateAndUpdateCache` request classes (the `createClientRequest`
  switch in `OzoneManagerRatisUtils.java:127-352` collapses — every case now builds a
  `PlannedRequest`). **Dead-code sweep**: confirm no remaining caller of the legacy `OMClientRequest`
  subclasses or the `BucketLayoutAwareOMKeyRequestFactory` legacy registrations before deleting
  (post-refactor dead-code discipline — a class written for mixed-mode fallback is dead once the
  flag is gone).
- **PR-7.4** retire the per-command flags; the dispatcher (PR-0g) collapses to a single path.

**Config flag.** None (flags retired).

**Dependencies.** P-3, P-4, P-5, P-6 — **every** command must be migrated and stable before the
legacy path can be deleted (deleting it strands any un-migrated command). This is why P-7 depends on
the full set, not just the last phase.

**Acceptance gate.** Double buffer gone; single execution model; all flags retired; the
replicated-DB apply is the sole writer; perf target hit; all `I-n` tested; `lint-spec` green; TLA+
tiers green (master §31 Definition of Done).

```yaml
id: P-7
scope: "delete double buffer (PR-7.1), remove table caches (PR-7.2), delete legacy validateAndUpdateCache path (PR-7.3, with dead-code sweep), retire per-command flags (PR-7.4)"
depends_on_phases: [P-3, P-4, P-5, P-6]
must_satisfy: []
must_pass: []
config_flag: "n/a"
acceptance: "double buffer gone; single execution model; legacy path deleted; flags retired; all I-n tested; lint-spec + TLA+ green"
provenance: inferred
evidence: ["master §29 P-7", "OzoneManagerDoubleBuffer.java", "OzoneManagerRatisUtils.java:127-352", "BucketLayoutAwareOMKeyRequestFactory.java:79-211"]
```

---

### P-8 — Disable the RocksDB WAL; Ratis log as sole WAL

```yaml
- {id: P-8, scope: "disable RocksDB WAL; Ratis log as sole WAL; enable atomic_flush=true", depends_on_phases: [P-7], must_satisfy: [I-atomic-flush, I-merge-replay-safe, I-log-retention, I-wal-off-closure], must_pass: [T-crash-replay-merge-once, T-wal-off-recovery], config_flag: "ozone.om.db.wal.disabled", acceptance: "WAL off under atomic_flush; Ratis log sole WAL; quota Merge exactly-once on torn-flush replay; replay within budget"}
```

**Command set.** None. This phase flips RocksDB write options (disable WAL + `atomic_flush=true`) and
wires log-purge to the flushed snapshot index. It is **not** a command migration; it is a
durability-model change, separated from P-7 so the throughput diff and the durability diff land
independently.

**Sub-tasks / PRs:**

- **PR-8.1** enable `atomic_flush=true` on the OM RocksDB (`DBStoreBuilder` DB options) — lands FIRST and
  independently, since it is safe with the WAL still on and is the precondition for WAL-off
  (`I-atomic-flush`: without it the multi-CF batch tears on crash → quota `Merge` double-counts on replay).
- **PR-8.2** add the `ozone.om.db.wal.disabled` flag; when set, the apply-path `WriteOptions` disable the
  WAL (`DBStoreBuilder.java:227` currently sets only `setSync`, never `setDisableWAL`). Default off.
- **PR-8.3** gate log purge at the `flushDB`-backed snapshot index (`I-log-retention`); add the closure
  audit (`T-wal-off-closure-audit`) asserting no durable write bypasses the Ratis apply path
  (`I-wal-off-closure`).
- **PR-8.4** the crash-replay exactly-once test (`T-crash-replay-merge-once`, with the `atomic_flush=false`
  negative variant) and the NVMe replay benchmark (`T-wal-off-recovery`, `B-replay-length`).

**Config flag.** `ozone.om.db.wal.disabled` (default off; enabled per-cluster after the audit + benchmark).

**Dependencies.** P-7 — the replicated-apply path must be the **sole** writer before the WAL is removed,
so the Ratis log is unambiguously the durability authority for every write.

**Acceptance gate.** WAL disabled with `atomic_flush=true`; the Ratis log is the sole WAL; the quota
`Merge` applies exactly once across a torn-flush crash; log retention gated at flushed snapshots; replay
time within the operational restart budget. Full mechanism + invariants: `leader-execution-retry.md` §6–§8.

```yaml
id: P-8
scope: "enable atomic_flush=true (PR-8.1), add ozone.om.db.wal.disabled + disable apply-path WAL (PR-8.2), gate log purge at flushed snapshot + closure audit (PR-8.3), crash-replay exactly-once test + NVMe replay benchmark (PR-8.4)"
depends_on_phases: [P-7]
must_satisfy: [I-atomic-flush, I-merge-replay-safe, I-log-retention, I-wal-off-closure]
must_pass: [T-crash-replay-merge-once, T-wal-off-recovery]
config_flag: "ozone.om.db.wal.disabled"
acceptance: "WAL off under atomic_flush; Ratis log sole WAL; quota Merge exactly-once on torn-flush replay; log retention gated at flushed snapshot; replay within budget"
provenance: verified
evidence: ["master §29 P-8", "leader-execution-retry.md §6-§8 (D-wal-off, I-atomic-flush, I-log-retention, I-wal-off-closure)", "DBStoreBuilder.java:227 (WriteOptions), DBStoreBuilder.java:422 (WAL managed)"]
```

---

## 6. Open-decision touchpoints that the phasing must honor (do not pre-settle)

Two decisions are **not settled** and the playbook must route around them without forcing a
premature resolution. Stating them here prevents a phase from silently assuming an answer.

### 6.1 D-OPEN-quota-enforcement (RESOLVED → exact — touches P-1, P-4)

D-OPEN-quota-enforcement (master, status `locked`) is **resolved**: quota admission is **exact** via a
leader-local atomic reservation (the DB `Merge` stays the durable truth; the reserve is advisory,
reset-on-role-transition, term-fenced — `I-quota-admission-exact` / `I-quota-reservation-lifecycle`).
The TLA+/TLC counterexample that **confirmed over-commit** under the unreserved soft model (two commits
plan at `used=0`, both apply, `used=2 > limit=1` — `QuotaOvercommit.cfg` against `ObsAbstractExact`) is
now the motivation for the reserve. Over-commit is bounded to the failover window
(`B-quota-failover-window`), not the steady state.

**Phasing consequence.** P-1 lands the **commutative `Merge`** (D-7) **and** the leader-local
reservation — the `Merge` is the durable counter, the reservation is the exact admission gate, both on
the OBS commit path. P-1's acceptance gate now asserts `UsedConsistent` (counter exact) **and** exact
admission (`T-quota-concurrent`) plus the reserve lifecycle (`T-quota-leader-flap`). The same touchpoint
exists at P-4 (MPU commit/complete quota Merges). **Anti-pattern:** do not implement the reserve as a
static, process-global map without the role-transition reset — that is the killed
`ALT-quota-reserved-static` shape and leaks phantom usedBytes on leader flap (`I-quota-reservation-lifecycle`).

```yaml
# touchpoint, not a new decision — mirrors master D-OPEN-quota-enforcement (resolved)
id: D-OPEN-quota-enforcement
status: locked
phase_touchpoints: [P-1, P-4]
phasing_rule: "land the commutative Merge AND the leader-local reservation in P-1 (exact admission); the reserve must be advisory, per-OM instance-scoped, reset-on-role-transition, term-fenced (I-quota-reservation-lifecycle) — not a static map"
provenance: verified
evidence: ["master D-OPEN-quota-enforcement (locked, exact)", "I-quota-admission-exact, I-quota-reservation-lifecycle, B-quota-failover-window", "leader-execution-locking.md EXC-3 (narrowed)", "#7406 failover audit 2026-06-21"]
```

### 6.2 D-OPEN-retry (RESOLVED — durable replicated completion table; scopes which ops need a durable entry)

D-OPEN-retry (master, status `locked`; companion `leader-execution-retry.md` R-1..R-5) uses an in-flight
registry + a **durable replicated `(clientId, callId) → response` completion table written atomically
with the data batch** (one record per request in the batch), checked at admission before execution —
**resolved**, not in-memory-only. The per-operation idempotency audit
(`leader-execution-idempotency-audit.md`) supplies the classification. The idempotency column (§2, §4)
is that audit: the **DB batch is already idempotent** (whole-object `Put`/`Delete`), and **only
re-execution (re-plan)** of the non-idempotent set is unsafe. The re-plan-unsafe set the phasing must
track:

- **SCM-bearing:** C1 CreateKey, C3 AllocateBlock, C5 CreateFile (fresh block IDs each plan).
- **Quota-`Merge`-bearing:** C2 CommitKey, C4 DeleteKey, C14 CommitMPUPart, C15 CompleteMPU,
  B1 DeleteKeys (a re-planned `Merge` double-counts).
- **Table-move:** C11 SnapshotMoveDeletedKeys, C12 SnapshotMoveTableKeys (re-move double-counts
  reclaim).

The audit's full client-facing non-idempotent set is **~26 ops** in two tiers (Tier A durable-entry-
required, Tier B suppress-spurious-error); the list above is the phasing-critical re-plan subset. R-4:
V1 caches **every** write op uniformly (the tiers size the TTL, not a runtime switch), so the
naturally-idempotent `Put`/`Delete` ops also get an entry but their TTL residual is harmless.

**Phasing consequence.** Each phase that introduces a non-idempotent op (P-1 SCM+quota, P-3 moves,
P-4 MPU, P-5 DeleteKeys) wires the **terminal-step retry-cache write** (the multi-step framework writes
the entry on the terminal step only — locking §4.3). The decision is locked; what each phase gates is the
mechanism **landing in code** — P-1's production flag for {CreateKey, CommitKey, AllocateBlock, DeleteKey}
is held until the durable retry path lands (dev/staging may precede).

```yaml
# touchpoint, not a new decision — mirrors master D-OPEN-retry (resolved)
id: D-OPEN-retry
status: locked
phase_touchpoints: [P-1, P-3, P-4, P-5]
non_idempotent_on_reexec: [CreateKey, AllocateBlock, CreateFile, CommitKey, DeleteKey, CommitMPUPart, CompleteMPU, DeleteKeys, SnapshotMoveDeletedKeys, SnapshotMoveTableKeys]
phasing_rule: "wire the terminal-step durable atomic-with-batch completion entry (locking §4.3) at each phase introducing a non-idempotent op; R-4 caches every write op uniformly; P-1 production flag held until the durable retry path lands in code (dev/staging may precede)"
provenance: verified
evidence: ["master D-OPEN-retry (locked)", "leader-execution-retry.md R-1..R-5", "leader-execution-locking.md §4.3,§10", "audit §4 idempotency column (~26 ops, two tiers)"]
```

---

## 7. Cross-phase invariants the playbook must not violate (summary)

A compact restatement, so a reviewer can check any single phase's PRs against the whole:

- **One flag per `Type`, default legacy (D-14).** Never migrate "half a command" (one layout). The
  OBS/FSO split in the inventory is the unit boundary; a `Type` flips both halves or neither
  (§0 warning). P-1 (OBS halves) → P-2 (FSO halves) is the *intended* per-`Type` staging, and the
  flag gates the `Type`, so the FSO half staying legacy through P-1 is supported mixed mode, not a
  half-migration bug.
- **Finalization is the binary gate; the flag is the operational gate (D-11 vs D-14).** Both must be
  true for a command to run new. Never conflate them.
- **Mixed mode is long-lived and first-class.** Every phase boundary is a stable resting state
  (master always stable, D-13/D-14). No phase may assume the next one lands.
- **Prerequisites gate everything (§3).** No command before PR-0a/b/c. The shared ManagedIndex
  counter (D-12) is the one that bites first and silently if skipped.
- **Hard-first ordering is a risk control, not a preference (D-13).** P-5/P-6 depend *behind* the
  hard phases; the easy work can never be used to declare premature victory.
- **The resolved decisions land in code, not re-litigated (§6.1, §6.2).** P-1 lands the quota `Merge`
  AND the leader-local reservation (exact admission, resolved), and wires the durable retry-cache entry
  (resolved) at the terminal step for the non-idempotent ops. Both decisions are locked; the phase PRs
  implement them, they do not re-open them.
- **P-7 deletes nothing until everything is migrated.** The legacy path's deletion depends on the
  full P-3/P-4/P-5/P-6 set; a dead-code sweep precedes the delete.

**Cross-cutting tests (apply to every migrated command — NOT a per-phase `must_pass`).** This
list is for the *cross-cutting* tests only — those that re-run for **every** command migration
and therefore gate the migration as a whole, not one phase (this mirrors the test-plan companion
§5 "Cross-cutting" note). The master §29 `P-n` blocks (reproduced verbatim above) deliberately
keep *these cross-cutting* tests out of any single phase's `must_pass`, and the expanded playbook
blocks match that. Do not read this as "every test below is excluded from all phase
`must_pass`": **phase-specific** gates are a separate category and DO appear in their owning
phase's `must_pass`. Two such phase gates exist — `T-batch-quota-no-double-decrement` is in
**P-5**'s `must_pass`, and `T-quota-failover` is in **P-1**'s `must_pass`. `T-quota-failover` is
the one test that is *both*: a P-1 phase gate (it must pass for P-1 to land) and a standing
cross-cutting re-run on every later change that touches quota — which is why it is also named in
the list below. Every other entry below is purely cross-cutting (no phase `must_pass` claims it):
- `T-flag-routing-both-paths` (D-14) — per-command on/off byte-identical parity; run per op as
  each command migrates (heaviest in P-6's Set-A sweep, but applicable everywhere).
- `T-determinism-follower-byte-identical` and `T-apply-failure-resync` (D-10) — the determinism +
  crash-and-resync contract every migrated command must uphold.
- `T-rolling-upgrade-mixed-binary` and `T-mixed-mode-no-collision` (D-11/D-12) — mixed-mode
  safety across the whole migration window, until finalization retires them.
- `T-quota-failover` / `T-quota-exact-tlc` — the quota path's standing crash-safety and exactness
  checks, re-run on every change touching quota (their verdict tracks D-OPEN-quota-enforcement).
