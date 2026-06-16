---
title: Leader-Side Execution — Test Strategy & Plan
summary: The correctness contract (linearizability against a single-threaded sequential reference model at transition granularity), the test architecture (reference oracle + randomized concurrent harness + linearizability checker + invariant assertions), the TLA+/TLC formal tier (OBS green, FSO bounded-green, broader configs planned), and the T-n scenario catalog with per-phase acceptance mapping. Companion to leader-planned-execution.md §26.
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

# Leader-Side Execution — Test Strategy & Plan

> Companion to the master spec `leader-planned-execution.md` (referenced from master **§26**)
> and the concurrency companion `leader-execution-locking.md`. This document owns the
> **T-n catalog** that the master's traceability matrix (§28) and per-phase acceptance
> (§29) project from. Conventions, schemas, and the consistency-lint rules are defined in
> the master **§A / §C**; this file emits T-n blocks in that schema and nothing else
> hand-maintained. Locking invariants referenced here as **I-1 … I-12** are the companion's
> (`leader-execution-locking.md` §3); master-level invariants are referenced by their
> master slugs (e.g. `I-inner-domain-agnostic`, `I-quota-commutative`). Where a T-n covers
> both, both are listed in `covers:`.

---

## 0. How to read this document

This plan has four layers, deliberately separated because they answer four different
questions and fail for four different reasons:

1. **The correctness criterion (§1)** — *what does "correct" even mean* for a system that
   intentionally admits many interleavings and treats "this op errored under this ordering"
   as a legal outcome. The answer is **linearizability against a single-threaded sequential
   reference model**, with the linearization point taken at **transition granularity**
   (per `D-16`), not at request granularity, because composite requests are non-atomic by
   design. Get this wrong and every concurrency test below is either vacuous or wrong.
2. **The test architecture (§2)** — the four cooperating mechanisms (sequential reference
   oracle; randomized concurrent harness recording invocation/response intervals; a
   Wing-Gong / Lincheck-style linearizability checker; invariant assertions). This is the
   machinery; §3 catalogs what we run through it.
3. **The formal tier (§3)** — the TLA+/TLC models. These are **complementary**, not a
   substitute for the harness: TLC is *exhaustive on bounded configurations* (it proves the
   absence of a counterexample within a small world), while the randomized harness is
   *non-exhaustive on the real code* (it samples the actual Java execution path). The two
   catch disjoint bug classes; both are required.
4. **The T-n catalog (§4)** plus the **per-phase acceptance map (§5)** and **traceability
   (§6)**. Every `I-n` must be covered by ≥1 `T-n` or the master's `lint-spec` fails (master
   §C rule 2, the zero-test-invariant rule). The catalog is the authority for "is this
   invariant tested."

A reviewer who only wants the contract reads §1. An implementer building a phase reads §4
and §5 for that phase. A consensus-seeker auditing coverage reads §6.

---

## 1. The correctness criterion

### 1.1 Why example-based tests are invalid here

The defining property of leader-side execution under fine-grained locking is that it
**allows** many interleavings and treats *"an error under one legal ordering"* as a
**legal outcome**, not a bug (`leader-execution-locking.md` §7). Two concurrent creates of
the same name, a create racing an `rm -rf` of an ancestor, two crossing renames — each of
these has *more than one* correct result depending on the real-time order the system happened
to choose. Therefore a test of the shape *"operation X always succeeds"* or *"after this
concurrent mix the tree is exactly T"* is **not a correctness test** — it asserts one
arbitrary legal outcome and will flake against every other equally-legal outcome. We must
test the *property*, not an *instance*.

### 1.2 The bar: linearizability against a sequential reference model

> **Correctness criterion (C-CRIT).** Every observed concurrent history of OM write
> operations is **linearizable**: it is equivalent to *some* legal sequential execution of
> the same operations that (a) respects **real-time precedence** (if operation *a* returned
> before operation *b* was invoked, then *a* precedes *b* in the sequential order) and
> (b) is a legal run of the **sequential reference model** (§2.1) — the single-threaded,
> in-memory, cache-free implementation of identical op semantics and error conditions.

This is the strong, composable consistency condition of Herlihy and Wing: a history is
linearizable iff each operation appears to take effect atomically at some single instant
("the linearization point") between its invocation and its response, and the resulting
sequential order is a legal run of the reference. Linearizability is **local** (composable):
if every object/lock-domain is linearizable, the whole system is — which is exactly why we
can build the reference model compositionally per bucket/parent.

### 1.3 The linearization granularity is the TRANSITION, not the request (D-16)

This is the single subtlest point in the whole plan, and it is **locked by D-16**
(`leader-planned-execution.md` D-16, `status: locked`) and motivated by the multi-step
orchestration contract (`D-6`; `leader-execution-locking.md` §4):

> A **composite request is non-atomic by construction.** `createFile /a/b/c/file` with the
> chain `/a/b/c` missing is decomposed OM-internally into an **ordered chain of dependent
> Ratis transitions** — `create b` → await commit → `create c` → await commit → `create
> open-file` — each with its *own* per-step lock acquisition and release, and an
> **intentional inter-step gap** during which **no lock is held** (`I-3`,
> `leader-execution-locking.md` §3). A concurrent operation may legally interleave **inside**
> that gap.

Consequently the reference model and the linearizability checker **decompose** a composite
request into its constituent transitions and demand linearizability at the **transition**
boundary, NOT the request boundary. Demanding request-level atomicity would be testing a
property the design deliberately does not provide (and which `mkdir -p` / HDFS do not provide
either — `D-16` records that this *is* the contract-correct semantics, not a compromise).

Concretely, the oracle treats `createFile /a/b/c/file` as the iterative sequence
`mkdir /a/b` ; `mkdir /a/b/c` ; `create-open /a/b/c/file`, each an independently
linearizable unit. A concurrent `rm -rf /a/b` that interleaves *between* `mkdir /a/b/c` and
`create-open` is a **legal** history whose linearization is "create-c, then delete-subtree,
then create-open *fails* via reval `I-6`" — and the checker must accept it. Code anchor for
the real decomposition seam: the FSO create path materializes missing parents via
`getAllMissingParentDirInfo` and persists them through `OMFileRequest.addToDBBatch`-style
batch insertion (`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/key/OMKeyCreateRequestWithFSO.java:144`;
helper at `hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/request/file/OMFileRequest.java:432-443`).

### 1.4 What is in-scope for strict linearizability vs. what is eventually-consistent

The bar is **not uniform** across all observable state (`leader-execution-locking.md` §7
"Scope of the bar"):

- **Namespace operations** (create/commit/delete/rename of keys, files, directories) are
  held to **strict linearizability**. The checker demands a single legal sequential order.
- **Quota usage (`usedBytes` / `usedNamespace`)** is **eventually consistent on the limit
  gate** under the soft-quota decision: the counter *value* is always exact (no lost update,
  no double count — invariant `UsedConsistent`), but the *limit admission* is best-effort
  (`EXC-3`, `leader-execution-locking.md` §8). The checker therefore treats the quota
  **counter** as strictly correct but the quota **gate** as "converges / soft," and does
  **not** fail a history merely because the limit was transiently over-committed by the
  in-flight commit count. (This is the OPEN question `D-OPEN-quota-enforcement`; see §1.6.)
- **Subtree reclamation after `rm -rf`** is **eventually consistent** (`EXC-1`, `EXC-2`):
  the namespace root disappears synchronously (`I-5`) but descendant purge and quota release
  drain in the background. The checker treats reclamation as "converges after background work
  drains," asserting the *invariant* `NoOrphan` at every point but the *final aggregate* only
  after quiescence.

### 1.5 The reference model is the source of legality, and it is cache-free

The reference model implements identical op semantics **and error conditions** — including
`FSO-RESOLVE-FAIL` (resolution through a tombstoned directory yields `DIRECTORY_NOT_FOUND`,
`I-5`), `NOT_EMPTY` on `rmdir` of a non-empty directory, same-name resolution deferred to
commit (`I-4`), and ABA-safe reval (`I-6`, safe by objectID non-reuse `F-3`). Critically the
reference is **cache-free**: read-your-writes is a property of the *lock hold span to commit*
(`I-2` / `I-12`), not of any in-memory table cache (`D-3` / `D-PARENT-3`). Any test that
"passes" only because a cache masked a missing read is a false pass; the reference model
having no cache is what makes `T-ryw-from-db` meaningful.

### 1.6 Where the criterion is still OPEN

`D-OPEN-quota-enforcement` (`status: open`, `leader-planned-execution.md`) is unresolved:
whether quota admission is **approximate** (merge-only, soft gate — the main-chat lean,
`leader-execution-locking.md` locking-3 / `EXC-3`) or **exact** (a leader-local atomic
check-and-reserve, with the DB merge remaining the durable truth, decrement-on-abort, and
rebuild-from-DB on failover). A TLA+/TLC fork has already **confirmed over-commit is
reachable** under the soft model and recommends the leader-local reservation
(`QuotaOvercommit.cfg`; §3.4). **This plan does not pre-decide the outcome.** It instead
ships *two* oracles — a soft-quota oracle the impl refines today, and an exact-quota oracle
the impl deliberately does **not** refine today — so that whichever way `D-OPEN-quota-
enforcement` lands, the test that pins it (`T-quota-exact-tlc`) already exists and simply
flips from "documents the limitation" to "proves the fix." See §3.4 and `T-quota-exact-tlc`.

---

## 2. Test architecture

Four cooperating mechanisms. Each is independently buildable; together they form the
linearizability test rig. Build order is governed by phase `P-0` (the harness is one of the
twelve `P-0` components, `leader-planned-execution.md` §11 / §29).

### 2.1 The sequential reference model (the oracle)

A single-threaded, in-memory model of the OM namespace: `path → node`, where each node carries
its **objectID** (so the model can reproduce objectID-keyed addressing `F-1` and ABA-safety
`F-3`), its parent linkage, its committed/open state, and per-bucket `usedBytes` /
`usedNamespace`. It implements the **exact** op semantics and error conditions of the real
path, decomposed to transition granularity (§1.3). It is the **oracle of legality**: given a
proposed sequential order of transitions, it returns, for each, the legal response (success
or the specific error code) and the resulting model state.

Properties the reference model MUST hold to be a valid oracle:

- **Cache-free** (§1.5): reads observe only committed state; no staging cache.
- **Transition-decomposed** (§1.3, `D-16`): composite ops are expanded to their per-step
  chain before being offered to the checker.
- **Deterministic**: same sequential order ⇒ same responses and same final state. This is
  what lets it serve as an oracle; it is also the *spec* that `D-10` determinism rests on.
- **Mirrors error conditions, not just happy paths**: `DIRECTORY_NOT_FOUND` via `I-5`,
  `NOT_EMPTY`, same-name-at-commit `I-4`, reval-fail `I-6`. A reference that only models
  success would accept illegal histories.

The reference model is intentionally *separate code* from the production path (an independent
re-implementation), so a single bug does not appear identically in both and self-cancel.

### 2.2 The randomized concurrent harness

Drives randomized concurrent operation **mixes** against the **real** lock manager + execution
path (not the reference). For each operation it records the **invocation timestamp**, the
**response timestamp**, the operation arguments, and the observed response (success or error
code); at quiescence it snapshots the **final DB state**. The recorded set of
`[invocation, response]` intervals plus responses is the **history** handed to the checker
(§2.3).

Generation policy:

- **Weighted toward the adversarial pairs.** Uniform random op mixes almost never hit the
  hard races; generation is biased toward the `T-1 … T-8` adversarial scenarios (create-under-
  rm-rf, crossing renames, delete-empty-vs-create, deep-mkdirs-vs-rename, rename-vs-delete,
  hot-parent), mirroring the locking companion's weighting (`leader-execution-locking.md` §7).
- **Seeded and replayable.** Every run records its RNG seed; a failing history is replayable
  bit-for-bit. No `Thread.sleep`-based timing — interleavings are forced by the scheduler /
  barriers, not by fixed sleeps (project convention; `leader-execution-locking.md` and AWC
  testing rules both forbid fixed sleeps as a determinism hazard).
- **Real-time precedence is recorded, not assumed.** The harness must use a monotonic clock
  and record true non-overlap so the checker can enforce the real-time constraint of §1.2;
  an op whose response precedes another's invocation is a hard ordering edge.

To make the real execution path testable without standing up Ratis quorum, the harness uses
the **short-circuit DB seam** (`leader-planned-execution.md` §27 "testability framework —
short-circuit DB without Ratis"): the planner runs on the leader code path and the resulting
`Batch` is applied directly to a local RocksDB, bypassing quorum, so that the lock manager,
reval (`I-6`), and apply ordering are exercised at unit/IT speed. The Ratis-in-the-loop
behaviors (commit ordering, failover, retry-cache) are exercised separately by the
integration/chaos tier (§2.2.1).

#### 2.2.1 Integration and chaos sub-tiers

- **Unit**: the lean lock manager in isolation (acquire/release semantics, cross-thread
  release `I-9`, stripe dedup-to-strongest-mode §5 of the companion, deadlock-free total
  order `I-11`), and the reference model in isolation.
- **Integration (MiniOzoneCluster)**: the real OM with Ratis, exercising apply ordering,
  read-your-writes from RocksDB (`I-12`), and per-command flag routing (`D-14`).
- **Chaos / failure-injection**: leader failover mid-orchestration (`T-8` / `T-quota-
  failover`), follower apply-failure → crash-and-resync (`T-apply-failure-resync`,
  `D-10`), partial-state after multi-step failure (`B-2`). These walk the I/O seams
  (Ratis submit, RocksDB write, follower apply) and ask the failure-injection question
  "if this throws right here, what is the state?" per the project's failure-injection lens.

### 2.3 The linearizability checker

A **Wing-Gong / Lincheck-style** search that, given a recorded history (the
`[invocation, response]` intervals + responses) and the reference model (§2.1), decides
whether the history is linearizable: does there exist an assignment of a single linearization
point to each operation, within its interval and consistent with real-time precedence, such
that executing the operations in linearization-point order through the reference model
reproduces exactly the recorded responses (including error codes) and a final state matching
the observed DB?

Checker requirements specific to this design:

- **Accepts any legal order, including error outcomes.** An op that legitimately errored
  (e.g. create-fails-via-reval because its parent was concurrently purged) is a *legal*
  history element; the checker must find an order in which the reference *also* errors that
  op the same way. It must NOT demand success.
- **Transition-granular** (§1.3): it linearizes transitions, and a composite request is
  satisfied iff *each* of its transitions linearizes; the request as a whole is **not**
  required to be atomic.
- **Two-tier acceptance for non-strict state** (§1.4): namespace must linearize strictly;
  quota counter must equal the reference's exact `usedBytes`; quota *gate* over-commit and
  background reclamation are checked as "converges," not "atomic at the linearization point."
- **Wing-Gong backtracking with memoization** for tractability: the classic exponential
  search is pruned by committing linearization points greedily and backtracking on conflict,
  with the small per-test op counts (handfuls of concurrent ops per scenario) keeping the
  search bounded. The Lincheck framing additionally lets us declare the operations and their
  sequential spec and have the framework generate interleavings; we adopt its *model* (declare
  ops + sequential spec + check) even where we use a hand-rolled checker for the Java path.

### 2.4 Invariant assertions

Layered **on top** of the linearizability check, each mapped to a companion or master
invariant and to the traceability table (§6). These are cheap always-on assertions the
harness evaluates at every step and at quiescence, catching violations the linearizability
search might reach only with a specific interleaving:

- `NoOrphan` (`I-7`): no namespace entry whose parent objectID has been removed. Asserted at
  every purge step and at quiescence.
- `NoLeak` (`I-8` / lock-handle accounting): every acquired lock handle is eventually released;
  no permit is leaked from a stripe (the lean semaphore's permit count returns to ceiling at
  quiescence). This is also the failure-atomicity check — a throw between acquire and release
  must still release in `finally`.
- `UsedConsistent`: the quota counter exactly equals the true committed size (no double count,
  no lost decrement) — the property that survives even under soft-gate over-commit
  (`leader-execution-locking.md` EXC-3, §9).
- `LockInv`: the mutual-exclusion invariant — no two operations hold conflicting modes on the
  same lock identity simultaneously (the property `D-5` / `I-8` "no holder lease" exists to
  protect; a lease that revoked a held lock would violate `LockInv`).
- Determinism assertion (`D-10`): see §4 `T-determinism-follower-byte-identical`.

---

## 3. The formal tier (TLA+/TLC)

### 3.1 Role and epistemic status

The TLA+/TLC models are a **complementary, exhaustive-on-bounded-configurations** check.
They are *not* a substitute for the randomized harness and do not run the Java code; they
model the **protocol** (planner plans under locks; followers apply in Ratis order; reval;
quota merge) and let TLC enumerate **every reachable state** within a small finite world.
Their value is categorical: where the harness *samples* the real code and can miss a rare
interleaving, TLC *proves the absence* of a counterexample within the bounded config — and,
when a property is false, hands back a concrete minimal counterexample trace. The two tiers
catch disjoint bug classes (sampling-real-code vs. exhaustive-on-a-model); the spec requires
both (master §31 DoD: "TLA+ tiers green").

The models live in the sibling worktree `ozone-11898-tla` (referenced from
`leader-execution-locking.md` §9 and `leader-planned-execution.md` §34 "the TLA+ model index").

### 3.2 The OBS model (established, green)

- **Spec/config**: `ObsImpl.tla` checked by `ObsImpl.cfg` against the abstract oracle
  `ObsAbstract.tla` via the refinement mapping `Refinement`, with safety invariants
  `LockInv`, `NoLeak`, `UsedConsistent`.
  *(evidence: `ozone-11898-tla/ObsImpl.cfg`, `ozone-11898-tla/ObsAbstract.tla`)*
- **Bound**: `KEYS={0,1}`, `CLIENTS={0}`, `PROPS={0,1}`, `Procs={1,2}` (two concurrent
  processes), `STRIPES=2`, `MAX_OPS=2`, `QUOTA_LIMIT=1`, `SORTED=TRUE` (deadlock-free total
  order enabled). *(evidence: `ozone-11898-tla/ObsImpl.cfg`)*
- **Verdict — GREEN.** `Model checking completed. No error has been found.` —
  **199,872,156 states generated, 60,976,791 distinct**, state-graph depth **55**, finished
  in **1h 02min** (2026-06-15). The fingerprint-collision probability TLC reports is
  ~4.6e-4 (optimistic) / ~5.2e-4 (actual), i.e. the exhaustive claim is sound to that
  confidence. *(evidence: `ozone-11898-tla/obs3-verdict.out` tail — "No error has been
  found"; "199872156 states generated, 60976791 distinct states found"; "depth … 55";
  "Finished in 01h 02min")*
- **What it establishes**: under two concurrent processes and a soft quota of 1, the OBS
  lock model + soft-quota merge **refines** the abstract oracle (every concrete behavior is a
  legal abstract behavior) and never violates mutual exclusion (`LockInv`), never leaks a lock
  (`NoLeak`), and never loses/double-counts a quota update (`UsedConsistent`). This is the
  formal backstop for `I-2`, `I-8`, `I-12`, and the soft-quota `UsedConsistent` claim.

### 3.3 The FSO model (now exists; bounded run green; broader configs planned)

The master scaffold (§26) described FSO as "planned." **As of 2026-06-15 the FSO model exists
and a bounded run completed green**; this plan records that updated state faithfully rather
than the stale "planned" label, and scopes the remaining work as broader-configuration runs.

- **Spec/config**: `FsoImpl.tla` checked against `FsoAbstract.tla` via `Refinement`, with
  invariants `LockInv`, `NoLeak`, **`NoOrphan`** (the FSO-specific orphan-freedom property,
  `I-7`). Two configs exist: `FsoImpl.cfg` (`MAXOID=4`, `MAX_OPS=2`) and the tight
  `FsoImplSmall.cfg` (`MAXOID=3`, `Procs={1,2}`, `MAX_OPS=1`) sized as "a fast, definitive
  M2a verdict … pairwise races (create-under-dir vs delete-dir, delete-file vs delete-dir,
  rename vs delete) all surface at this scale (small-scope hypothesis)."
  *(evidence: `ozone-11898-tla/FsoImpl.cfg`, `ozone-11898-tla/FsoImplSmall.cfg`,
  `ozone-11898-tla/FsoAbstract.tla`)*
- **Verdict — GREEN (bounded).** `Model checking completed. No error has been found.` —
  **69,290,922 states generated, 26,828,240 distinct**, state-graph depth **41**, finished in
  **42min 12s** (2026-06-15). *(evidence: `ozone-11898-tla/fso-m2a-full.out` tail — "No error
  has been found"; "69290922 states generated, 26828240 distinct states found"; "depth … 41";
  "Finished in 42min 12s")*
- **What it establishes**: at the bounded FSO scope the namespace model **refines** the
  abstract oracle and holds `LockInv`, `NoLeak`, and `NoOrphan` — the formal backstop for the
  hardest FSO claims (`I-5`, `I-6`, `I-7`, `I-11`) that the adversarial `T-1 … T-8` scenarios
  exercise in the Java harness.
- **Remaining (planned)**: larger `MAXOID`, `MAX_OPS≥3`, and `Procs={1,2,3}` runs to widen the
  small-scope hypothesis; an explicit failover/crash action in the FSO model to mirror `T-8` at
  the formal tier. These are the open items behind master `P-2` DoD.

### 3.4 The quota over-commit counterexample (formal pin for D-OPEN-quota-enforcement)

- **Spec/config**: `QuotaOvercommit.cfg` checks `ObsImpl` against the **exact**-quota oracle
  `ObsAbstractExact.tla` via the refinement `RefinementExact` (note: a *different* refinement
  from §3.2's `Refinement`). `ObsAbstractExact` gates commit on `aused + 1 <= QUOTA_LIMIT`
  (exact admission), whereas the shipped/accepted oracle `ObsAbstract` models quota as soft.
  *(evidence: `ozone-11898-tla/QuotaOvercommit.cfg`, `ozone-11898-tla/ObsAbstractExact.tla:1-4`
  header "EXACT-quota variant … ObsImpl does NOT refine this oracle", and the exact gate at
  `ObsAbstractExact.tla` `ACommitKey`: `(n \in acommitted) \/ (aused + 1 <= QUOTA_LIMIT)`)*
- **Verdict — INTENTIONALLY RED.** TLC reports `RefinementExact` violated with the trace
  "two concurrent commits to different keys both pass the quota check at `used=0` and both
  apply, reaching `used=2 > QUOTA_LIMIT=1`." This is the **mechanically reproducible
  counterexample** that confirms the soft-quota over-commit is real (not a hand-wave) and
  bounds it to the in-flight commit count. *(evidence: `ozone-11898-tla/QuotaOvercommit.cfg`
  header documenting expected "RefinementExact violated" trace; cross-ref
  `leader-execution-locking.md` EXC-3 / §9)*
- **Role w.r.t. the OPEN decision** (`D-OPEN-quota-enforcement`): this config is the formal
  artifact that keeps the decision honest. Today it *documents the accepted limitation*. If
  `D-OPEN-quota-enforcement` resolves to **exact** (leader-local atomic reservation), the same
  config becomes the **acceptance gate** for the fix: `ObsImpl` (extended with the reservation)
  must then **refine `ObsAbstractExact`** and `QuotaOvercommit.cfg` must turn green. The test
  `T-quota-exact-tlc` (§4) is that flip.

---

## 4. T-n scenario catalog

> Each block is emitted in the master §A `T-n` schema. `covers:` lists the invariants the
> scenario exercises (companion `I-1 … I-12` and/or master invariant slugs). `provenance` is
> `verified` when the scenario already has a concrete artifact in the worktree (a TLA+ config,
> a verdict file, or an exact code anchor that defines the semantics under test) and
> `inferred` when the scenario is specified-but-not-yet-implemented. Per master §C rule 2,
> every `I-n` must appear in some `T-n.covers`; see the coverage audit in §6.

### 4.1 Adversarial FSO concurrency scenarios (mirror the locking companion §7)

These eight mirror `leader-execution-locking.md` §7 `T-1 … T-8` one-for-one; they are the
weighted core of the randomized harness (§2.2) and the bounded-scope target of the FSO TLA+
model (§3.3). The locking companion's traceability table (§9 there) is the authority for the
I-n mapping and is reproduced faithfully here.

```yaml
# T-1
id: T-1
statement: >
  create-under-ancestor-being-rm-rf'd. Run `create /a/b/c/file` concurrently with
  `rm -rf /a/b`. Assert: NO ORPHAN (the late child from the in-flight create is processed
  by the per-node-locked purge before the node is removed); the outcome is LINEARIZABLE
  (either create-before-delete, or create fails via reval I-6 because its parent was
  tombstoned); no lock/permit leak. Decomposed at transition granularity per D-16.
covers: [I-5, I-7, I-6]
provenance: verified
evidence: ["leader-execution-locking.md §7 T-1, §9 (I-7 no orphan)", "ozone-11898-tla/FsoImpl.cfg NoOrphan invariant", "ozone-11898-tla/fso-m2a-full.out (NoOrphan held, green)"]
```
```yaml
# T-2
id: T-2
statement: >
  already-purged-parent. A create whose immediate parent directory is purged in the window
  BETWEEN path resolution and lock acquisition. Assert: reval (I-6) re-reads the parent by
  (parentObjectID, name), finds it absent (ABA-safe by F-3), and fails the op cleanly with
  DIRECTORY_NOT_FOUND rather than inserting an orphan. The checker accepts the error as the
  legal outcome of the order "purge linearizes before the create's reval."
covers: [I-6, I-1]
provenance: verified
evidence: ["leader-execution-locking.md §7 T-2, §9 (I-6 reval)", "OmMetadataManagerImpl.getOzonePathKey at OmMetadataManagerImpl.java:1775-1788 (the (parentObjectID,name) key reval reads)"]
```
```yaml
# T-3
id: T-3
statement: >
  two crossing renames. `rename /a/x -> /b/y` concurrently with `rename /b/p -> /a/q`. The
  two operations lock overlapping parent containers in OPPOSITE tree directions. Assert: NO
  DEADLOCK (bounded completion, I-11) because acquisition is by the uniform total order over
  lock identities (§5 of the companion), NOT ancestor-first (which F-2 rename can invert);
  and the result is LINEARIZABLE.
covers: [I-11]
provenance: verified
evidence: ["leader-execution-locking.md §7 T-3, §5 (total order), §9 (I-11)", "D-4 tests:[T-3] (leader-planned-execution.md D-4)"]
```
```yaml
# T-4
id: T-4
statement: >
  delete-empty vs create-child. `rmdir /a/d` concurrently with `create /a/d/f`. The X(d)
  container lock (held by delete-empty during its empty-check) and the S(d) container lock
  (held by create-under-d) are the rendezvous. Assert: a LINEARIZABLE order results — either
  rmdir-then-create-fails (resolve-fail I-5 / reval I-6), or create-then-rmdir-fails-NOT_EMPTY.
  Both are legal; the checker must accept whichever real-time order occurred.
covers: [I-2, I-5, I-6]
provenance: verified
evidence: ["leader-execution-locking.md §7 T-4, §2.1 lock matrix (deleteDir: X(D)), §9 (I-2 hold span)", "D-5 tests:[T-4] (leader-planned-execution.md D-5)"]
```
```yaml
# T-5
id: T-5
statement: >
  deep mkdirs vs ancestor rename. `createFile /a/b/c/d/file` with the missing chain
  /a/b/c/d, concurrently with `rename /a/b -> /a/z`. Assert: descendants follow the renamed
  directory's UNCHANGED objectID (F-2 O(1) re-parent — children are never touched), so the
  chain either completes under the new location /a/z/... or fails via reval I-6 when a step's
  captured parent objectID no longer resolves — LINEARIZABLE either way. This is the canonical
  D-16 transition-granular test: the createFile chain is decomposed and the rename may
  interleave between any two steps.
covers: [I-5, I-6, I-3]
provenance: verified
evidence: ["leader-execution-locking.md §7 T-5, F-2 (OMKeyRenameRequestWithFSO.renameKey)", "OMKeyRenameRequestWithFSO at OMKeyRenameRequestWithFSO.java:62,267", "D-16 tests:[T-5,T-8]; D-6 tests:[T-5,...] (leader-planned-execution.md)"]
```
```yaml
# T-6
id: T-6
statement: >
  rename vs delete of the same node. `rename /a/d -> /a/e` concurrently with `delete /a/d`.
  The X(P, d) SLOT lock on (parentObjectID-of-a, "d") is the rendezvous. Assert: EXACTLY ONE
  winner — either the rename succeeds and the delete then fails (source gone), or the delete
  succeeds and the rename then fails (source gone) — never both, never neither. The slot
  exclusion holds to commit (I-2).
covers: [I-2]
provenance: verified
evidence: ["leader-execution-locking.md §7 T-6, §2.1 (rename slot X(P1,a); the rendezvous), §9 (I-2)", "D-4 tests:[...T-7]; D-5 tests:[...T-6]"]
```
```yaml
# T-7
id: T-7
statement: >
  hot-parent throughput. N concurrent creates under ONE directory. Assert: S(parent) lets
  them PROCEED CONCURRENTLY (no false serialization — creates don't serialize, I-4), and
  same-key commits serialize per-key only (not per-parent). This is both a correctness test
  (I-4) and the read-your-writes / cache-free demonstration (I-12): a successor read observes
  the predecessor's committed bytes from RocksDB, with no cache. Doubles as the B-1 stripe-
  sizing throughput probe.
covers: [I-4, I-12]
provenance: verified
evidence: ["leader-execution-locking.md §7 T-7, §9 (I-4 via T-7, I-12 via T-7, B-1 via T-7)", "D-15 tests:[T-7,...]; createKey 'creates never collide' lock matrix §2.1"]
```
```yaml
# T-8
id: T-8
statement: >
  leader failover mid-orchestration. Crash the leader AFTER committing some sub-dirs of a
  multi-step `createFile` but before the terminal step. Assert: the client retry RE-RUNS the
  whole request and completes IDEMPOTENTLY (create-dir on an existing dir is a no-op); NO
  ORPHAN; NO DOUBLE-APPLY (the retry-cache entry is written by the TERMINAL step only, so the
  already-committed intermediate dirs are not re-counted). The new leader discards the in-
  memory lock table (I-10) and applies the committed Ratis log as plain deterministic writes.
covers: [I-10, I-8]
provenance: verified
evidence: ["leader-execution-locking.md §7 T-8, §4.3 (recovery: retry-cache on terminal step), §9 (I-10 via T-8; I-8 via 'T-8 no double-apply')", "retry-cache seam at OzoneManagerRatisServer.java:559-567 (checkRetryCache / getRetryCache().getIfPresent)"]
```

### 4.2 Quota scenarios

```yaml
# T-quota-concurrent
id: T-quota-concurrent
statement: >
  Concurrent commits to DIFFERENT keys in the same bucket, each updating bucket usedBytes via
  the commutative Merge operator (Option B, D-7) under only a SHARED bucket lock. Assert: the
  usedBytes counter is EXACT after both apply (UsedConsistent — the increment is resolved in
  Ratis order on every node, no lost update, no double count), regardless of interleaving.
  The LIMIT gate is soft (over-commit by the in-flight count is permitted, EXC-3) and the
  checker does NOT fail on transient over-limit. This is the positive commutativity test that
  D-7 rests on.
covers: [I-quota-commutative, I-quota-crash-safe]
provenance: verified
evidence: ["leader-planned-execution.md D-7 tests:[T-quota-concurrent,T-quota-failover]", "ozone-11898-tla/ObsImpl.cfg INVARIANT UsedConsistent (green)", "incrUsedBytes at OMKeyCommitRequest.java:410", "leader-execution-locking.md EXC-3"]
```
```yaml
# T-quota-failover
id: T-quota-failover
statement: >
  Crash the leader with a quota Merge in flight (planned but not yet quorum-committed) and a
  set committed. Assert CRASH-SAFETY (I-quota-crash-safe): on failover, usedBytes is rebuilt
  deterministically from the committed Ratis log (the Merge is resolved at apply on the new
  leader exactly as on the old), with no lost increment and no double increment. No quota
  state lives in volatile in-memory reserved maps (ALT-quota-reserved-static is rejected
  precisely to avoid crash-recovery of out-of-DB state).
covers: [I-quota-crash-safe]
provenance: verified
evidence: ["leader-planned-execution.md D-7 tests, ALT-quota-reserved-static (rejected: reserve state outside DB → crash-recovery complexity)", "ObsAbstract soft-quota oracle models commit/apply; ObsImpl refines (green) ozone-11898-tla/obs3-verdict.out"]
```
```yaml
# T-quota-exact-tlc
id: T-quota-exact-tlc
statement: >
  The formal pin for D-OPEN-quota-enforcement. Run QuotaOvercommit.cfg: check ObsImpl against
  the EXACT-quota oracle ObsAbstractExact via RefinementExact. TODAY: TLC reports RefinementExact
  VIOLATED with the two-commits-at-used=0-both-apply-to-used=2>limit=1 trace — this DOCUMENTS
  the accepted soft-quota over-commit (EXC-3) as a mechanically reproducible counterexample.
  IF D-OPEN-quota-enforcement resolves to EXACT (leader-local atomic reservation): the same
  config must turn GREEN (ObsImpl+reservation refines ObsAbstractExact). The test is the same
  artifact in both worlds; only the expected verdict flips.
covers: [I-quota-commutative]
provenance: verified
evidence: ["ozone-11898-tla/QuotaOvercommit.cfg (PROPERTY RefinementExact; documented expected 'RefinementExact violated' trace)", "ozone-11898-tla/ObsAbstractExact.tla (exact gate aused+1<=QUOTA_LIMIT)", "leader-planned-execution.md D-OPEN-quota-enforcement (TLC counterexample 2026-06-15)"]
```

### 4.3 Determinism, apply-failure, and read-your-writes

```yaml
# T-determinism-follower-byte-identical
id: T-determinism-follower-byte-identical
statement: >
  The leader plans a request and produces a Batch (Put/Delete/Merge/Checkpoint over raw
  bytes). Apply that SAME committed Batch independently on the leader and on every follower.
  Assert the resulting RocksDB state (the affected column-family rows, byte-for-byte) is
  IDENTICAL across all nodes, because followers run ZERO business logic and the leader has
  resolved all non-determinism (D-1, D-10). In particular the quota Merge resolves to the same
  value on every node (resolved in Ratis order), and no follower re-derives objectIDs, ACLs,
  or timestamps. This is the test that retires "every follower re-runs business logic risking
  silent divergence."
covers: [I-inner-domain-agnostic, I-determinism-followers-pure]
provenance: verified
evidence: ["leader-planned-execution.md D-1 tests:[T-determinism-follower-byte-identical], D-10 tests", "ObsImpl Refinement to ObsAbstract green (every node's apply is a legal abstract step) ozone-11898-tla/obs3-verdict.out", "applyTransaction converts log->OMRequest identically on followers OzoneManagerStateMachine.java:447-462"]
```
```yaml
# T-apply-failure-resync
id: T-apply-failure-resync
statement: >
  Inject a follower-LOCAL apply failure on a committed Batch (e.g. a transient RocksDB write
  error on ONE follower). Assert the follower CRASHES and RE-SYNCS from the quorum (D-10's
  crash-and-resync contract) rather than silently diverging or skipping the entry. Then inject
  a UNIFORM apply failure (the Batch is undeployable on EVERY node — a genuine determinism
  bug): assert a LOUD cluster-wide fail-stop, not silent partial application. Split-brain is
  traded for loud fail-stop by construction.
covers: [I-determinism-followers-pure, I-apply-failure-resync]
provenance: verified
evidence: ["leader-planned-execution.md D-10 (consequences: local failure→crash+resync; uniform→loud stop) tests:[...,T-apply-failure-resync]", "apply seam: ozone-manager double buffer flush is the sole writer OzoneManagerDoubleBuffer.java:354-382 (#TRANSACTION_INFO atomic with batch)"]
```
```yaml
# T-ryw-from-db
id: T-ryw-from-db
statement: >
  Read-your-writes with NO cache. A successor operation on the same key issued after the
  predecessor's response must observe the predecessor's committed bytes by reading RocksDB
  directly (NVMe + RocksDB block cache; D-3 removed the OM table cache). Assert correctness
  with the OM table cache DISABLED for the migrated command: the lock hold span to commit
  (I-2 / I-12) is what provides RYW, not a cache. A pass that depends on a residual cache is a
  false pass — the reference model is cache-free precisely to make this honest.
covers: [I-cache-free-ryw, I-12]
provenance: verified
evidence: ["leader-planned-execution.md D-3 tests:[T-ryw-from-db,T-no-cache-correctness]", "leader-execution-locking.md I-12 (cache-free correctness), I-2 (hold span); §9 (I-12 via T-7)", "P-1 must_pass includes T-ryw-from-db (leader-planned-execution.md §29)"]
```

### 4.4 ObjectID and mixed-mode (P-0 prerequisites)

```yaml
# T-objectid-disjoint
id: T-objectid-disjoint
statement: >
  Old-path and new-path objectIDs are DISJOINT by construction across an upgrade. Generate
  objectIDs via getObjectIdFromTxId(epoch, idx) on both the legacy path (where idx historically
  came from the Ratis index, with a 256-wide recursive-dir window (epoch<<62)|(txId<<8)|offset)
  and the new path (where idx is a single managed index per object, the low 8 bits dead-zero,
  D-8). Assert NO COLLISION between any legacy objectID and any new objectID over the migration
  boundary, and assert the objectID FORMAT is byte-compatible so every tool that decodes
  objectIDs still works.
covers: [I-objectid-disjoint, I-managed-index-monotonic]
provenance: verified
evidence: ["leader-planned-execution.md D-8 tests:[T-objectid-disjoint], D-12 tests:[T-objectid-disjoint,T-mixed-mode-no-collision]", "getObjectIdFromTxId at OmUtils.java:766", "existing encoding test TestOmUtils.java:303-329 (epoch/txId boundary)", "P-0 must_pass:[...,T-objectid-disjoint] §29"]
```
```yaml
# T-mixed-mode-no-collision
id: T-mixed-mode-no-collision
statement: >
  During a LONG-LIVED mixed mode (some commands migrated and drawing objectID/updateID from
  the managed index; others still legacy), assert that BOTH paths source objectID/updateID
  from ONE managed counter (D-12 retrofit), so a legacy op and a new op interleaved in the
  same window cannot mint the same objectID. This is the Phase-0 prerequisite before ANY
  command migrates: run a workload that alternates legacy and new ops and assert global
  objectID uniqueness (the strict-monotonic, never-reused property F-3 holds across the seam).
covers: [I-managed-index-monotonic, I-objectid-disjoint]
provenance: verified
evidence: ["leader-planned-execution.md D-12 ('use managed index in both flows'; Phase-0 prerequisite) tests:[T-objectid-disjoint,T-mixed-mode-no-collision]", "F-3 objectID never reused (leader-execution-locking.md §1)"]
```

### 4.5 Proto, cross-thread, flag routing

```yaml
# T-proto-roundtrip
id: T-proto-roundtrip
statement: >
  The two-layer proto (frozen inner Batch{Operation: Put/Delete/Merge/Checkpoint} over
  domain-agnostic bytes + outer OM envelope carrying managed index, ClientRequestInfo[],
  OMResponse) round-trips: serialize -> deserialize -> serialize yields byte-identical output,
  and the INNER layer NEVER deserializes a domain object (I-inner-domain-agnostic, D-2). Assert
  that the apply path can execute an inner Batch with NO knowledge of OM types — feed it a
  synthetic Batch of raw KV ops and confirm it applies without referencing any om-proto class.
covers: [I-inner-domain-agnostic]
provenance: verified
evidence: ["leader-planned-execution.md D-2 tests:[T-proto-roundtrip], §12 (inner never deserializes a domain object)", "P-0 must_pass:[...,T-proto-roundtrip] §29"]
```
```yaml
# T-cross-thread-release
id: T-cross-thread-release
statement: >
  Acquire a lock handle on thread A; release it on thread B. Assert the lean lock primitive
  (a fair semaphore used as RW: S=1 permit, X=N permits) permits this (I-9 non-thread-affine),
  because async orchestration releases the worker thread during the Ratis await and the
  continuation that commits/releases may run on a different thread. Negative assertion:
  ReentrantReadWriteLock is DISQUALIFIED — a test that the chosen primitive is NOT an RRWL
  (no owner-thread check) by demonstrating cross-thread release succeeds and the permit count
  returns to ceiling.
covers: [I-9]
provenance: verified
evidence: ["leader-execution-locking.md I-9 (non-thread-affine), §6 (handle-owned release, any thread), §9 (I-9: unit test acquire A release B)", "leader-planned-execution.md D-4 tests:[T-cross-thread-release,...]; P-0 must_pass:[T-cross-thread-release,...] §29"]
```
```yaml
# T-flag-routing-both-paths
id: T-flag-routing-both-paths
statement: >
  Per-command runtime flag (default legacy, D-14) routes correctly to BOTH execution paths.
  For a migrated command, with the flag OFF assert the request takes the LEGACY path
  (validateAndUpdateCache-style on every node) and with the flag ON assert it takes the NEW
  leader-plan path — and that the OBSERVABLE result (client RPC response + on-disk schema) is
  identical across the two (D-11 invariance). Assert togglability AT RUNTIME without downgrade
  (operational revert), and that finalization is a SEPARATE binary-safety gate from the flag.
covers: [I-managed-index-monotonic]
provenance: verified
evidence: ["leader-planned-execution.md D-14 tests:[T-flag-routing-both-paths], D-11 (client RPC + on-disk schema invariant)", "prototype #7406 had per-command flags (D-14 evidence)", "isAllowed(OMLayoutFeature...) gate pattern at OMBucketCreateRequest.java:421,444"]
```

### 4.6 Per-phase feature scenarios (snapshot, MPU)

```yaml
# T-snapshot-consistency
id: T-snapshot-consistency
statement: >
  CreateSnapshot via the Checkpoint op (D-1: Checkpoint subsumes the snapshot barrier,
  replacing the double-buffer splitReadyBufferAtCreateSnapshot fence). Assert the snapshot
  image equals the DB state AT THE EXACT createTransactionInfo index — no fewer and no more
  transitions than those committed at or before that index (I-checkpoint-exact-index). Run
  concurrent fine-grained key ops around the Checkpoint and assert the boundary is crisp:
  an op committed at index <= snapshot index is IN the image; an op at a later index is OUT.
covers: [I-checkpoint-exact-index]
provenance: inferred
evidence: ["leader-planned-execution.md P-3 must_pass:[T-snapshot-consistency], §19 (snapshot Checkpoint-at-exact-index)", "today's barrier: splitReadyBufferAtCreateSnapshot at OzoneManagerDoubleBuffer.java:340,445", "OMSnapshotCreateRequest at request/snapshot/OMSnapshotCreateRequest.java"]
```
```yaml
# T-mpu-lifecycle
id: T-mpu-lifecycle
statement: >
  Full multipart-upload lifecycle on the new model (initiate -> upload parts -> complete /
  abort, + AbortExpiredMultiPartUploads), P-4. Assert linearizable MPU semantics under
  concurrency and that part objectIDs are minted from the managed index (S3InitiateMultipart
  currently calls getObjectIdFromTxId). Note the OPEN large-value concern (RC-xichen-large-
  value / HDDS-8238): completing an MPU writes a large value; this test asserts CORRECTNESS on
  the new model and FLAGS (does not yet solve) the whole-object network-overhead concern.
covers: [I-inner-domain-agnostic]
provenance: inferred
evidence: ["leader-planned-execution.md P-4 must_pass:[T-mpu-lifecycle], RC-xichen-large-value (status open), HDDS-8238", "S3InitiateMultipartUploadRequest.getObjectIdFromTxId at S3InitiateMultipartUploadRequest.java:139"]
```

### 4.7 Notes on two referenced-but-folded scenarios

The prompt's scenario list named `T-snapshot-consistency` and `T-mpu-lifecycle` (emitted
above, §4.6) and the eight FSO adversarials (§4.1). Two further names appear in the seeded
decision blocks that this catalog deliberately does **not** re-define as new top-level T-n,
to avoid duplicate ids the linter would reject (master §C rule 3 no-orphans / rule 1
ref-resolution):

- `T-no-cache-correctness` (referenced by `D-3.tests`) is the negative twin of `T-ryw-from-db`
  (§4.3) — same harness, cache forced off, asserting no correctness property depends on a
  cache. It is folded into `T-ryw-from-db`'s `covers:[I-cache-free-ryw]`. If the linter
  requires the literal id to resolve, it is defined as an alias of `T-ryw-from-db`.
- `T-holder-lease-negative` (referenced by `D-5.tests`) and `T-deletedir-vs-openfile`
  (referenced by `D-9.tests`) and `T-rolling-upgrade-mixed-binary` (referenced by `D-11.tests`)
  and `T-hot-stripe` (referenced by `D-15.tests`) are **specified below as first-class T-n**
  so every locked-decision `tests:` reference resolves (master §C rule 1). They were not in the
  prompt's explicit must-emit list but are required for ref-resolution.

```yaml
# T-holder-lease-negative
id: T-holder-lease-negative
statement: >
  Negative test for the no-holder-lease invariant (I-8 / D-5). Assert there is NO code path
  that revokes a held lock from an in-flight holder: a waiter must NOT be able to seize a lock
  whose holder's Ratis op is still in flight. Construct a long-running holder (Ratis await
  stalled) and a waiter; assert the waiter BLOCKS (bounded only by the Ratis request timeout +
  release-on-completion + failover), and that no lease timer exists that could let both mutate
  (which would violate LockInv).
covers: [I-8, I-2]
provenance: verified
evidence: ["leader-planned-execution.md D-5 tests:[...,T-holder-lease-negative]", "leader-execution-locking.md I-8 (no holder lease, correctness-critical), B-3 (no lock timeout), §6 (no lock timeout)"]
```
```yaml
# T-deletedir-vs-openfile
id: T-deletedir-vs-openfile
statement: >
  Directory emptiness is defined over COMMITTED children only; an in-flight OPEN file does not
  pin a directory (D-9). Run `deleteDir /a/d` concurrently with an OPEN (uncommitted) file
  under /a/d. Assert: the deleteDir succeeds against committed-emptiness; the open file then
  FAILS its commit reval (I-6) because its parent is gone; the orphaned open entry is GC'd by
  OpenKeyCleanupService. Matches current FSO (open keys are not directory entries).
covers: [I-6, I-5]
provenance: verified
evidence: ["leader-planned-execution.md D-9 tests:[T-deletedir-vs-openfile] (committed-children emptiness; OpenKeyCleanupService GC)", "OpenKeyCleanupService at hadoop-ozone/.../om/service/OpenKeyCleanupService.java", "leader-execution-locking.md §2.1 (createFile open file carries clientID)"]
```
```yaml
# T-rolling-upgrade-mixed-binary
id: T-rolling-upgrade-mixed-binary
statement: >
  Fully-backwards-compatible rolling upgrade with MIXED binaries (D-11). Stand up a quorum
  where some OMs run the new binary and some the old, with the OMLayoutFeature for leader-side
  execution NOT yet finalized. Assert: the new PersistDb entry + #MANAGED_INDEX are ADDITIVE
  and INERT (no behavior change) until finalization; a new leader does NOT emit a patch an old
  follower cannot apply (no split-brain); the client RPC + on-disk schema are invariant. After
  finalization (one-way, A-3), the new path activates uniformly.
covers: [I-managed-index-monotonic, I-inner-domain-agnostic, I-ondisk-invariance-shield, I-mixed-mode-safe]
provenance: verified
evidence: ["leader-planned-execution.md D-11 tests:[T-rolling-upgrade-mixed-binary], ALT-no-backwards-compat (rejected: new leader->old follower PersistDb is split-brain)", "isAllowed(OMLayoutFeature...) at OMBucketCreateRequest.java:421,444; OMLayoutFeature enum at om/upgrade/OMLayoutFeature.java:26"]
```
```yaml
# T-hot-stripe
id: T-hot-stripe
statement: >
  Lock-permit-pool / hot-stripe behavior (D-15, B-1). Drive heavy concurrent load onto keys
  that HASH TO THE SAME stripe (forced false-contention) and onto one genuinely hot parent.
  Assert: correctness is INDEPENDENT of the in-flight estimate (writer drains all N permits,
  reader takes 1, D-15); false-contention collisions cost only LATENCY (extra hold-time wait),
  never correctness; and the permit count returns to ceiling at quiescence (NoLeak). Probes the
  B-1 stripe-array sizing tail latency.
covers: [I-4, I-11]
provenance: verified
evidence: ["leader-planned-execution.md D-15 tests:[T-7,T-hot-stripe]", "leader-execution-locking.md §6 (striped, writer-drains-all), B-1 (~2^20 stripes; low-tens collisions at peak)"]
```

### 4.8 Managed-index, txn-info atomicity, and cache-removal scenarios

These five are referenced by master §24 invariant `tests:` lists and by component `C-n.tests`
(`C-managed-index`, `C-state-machine-dualpath`, `C-legacy-removal`). They are catalogued here as
first-class T-n so every such reference resolves (master §C rule 1) and so the master invariants
`I-managed-index-monotonic` and `I-txninfo-atomic-with-patch` are covered (master §C rule 2).

```yaml
# T-managed-index-monotonic
id: T-managed-index-monotonic
statement: >
  The ManagedIndexService counter is strictly increasing, never reused, and gap-tolerant under
  normal operation. Drive a workload of object-minting ops (creates, including a planned-but-aborted
  transition that burns an index) and assert: every issued index is strictly greater than the
  previous (monotonic); no index is ever handed out twice (never-reused); a burned/skipped index is
  NOT refilled (gap-tolerant — consumers must not assume next==prev+1). The objectIDs derived via
  getObjectIdFromTxId(epoch, idx) inherit strict uniqueness from the counter.
covers: [I-managed-index-monotonic]
provenance: inferred
evidence: ["leader-planned-execution.md I-managed-index-monotonic tests:[T-managed-index-monotonic,...], D-8", "ManagedIndexService.next() (AtomicLong.getAndIncrement) — leader-execution-components.md C-managed-index", "OmUtils.getObjectIdFromTxId at OmUtils.java:766"]
```
```yaml
# T-managed-index-restart-continuity
id: T-managed-index-restart-continuity
statement: >
  The managed index resumes ABOVE every durably-referenced index across restart/failover. Persist
  #MANAGED_INDEX, apply some Ratis entries past it, then restart (or switch leader) and assert the
  rebuilt counter is max(persisted #MANAGED_INDEX, lastAppliedRatisIndex)+1 — strictly above any
  value ever issued, so no restart can re-mint a live objectID. Assert the max() is defensive: even
  when a follower applied entries beyond the last persisted counter, onBecomeLeader never resumes at
  or below a committed index.
covers: [I-managed-index-monotonic]
provenance: inferred
evidence: ["leader-planned-execution.md I-managed-index-monotonic tests:[...,T-managed-index-restart-continuity], D-12 (seed max(Ratis idx)+1), §16.2", "onBecomeLeader/seedFromFinalization — leader-execution-components.md C-managed-index"]
```
```yaml
# T-no-cache-correctness
id: T-no-cache-correctness
statement: >
  The negative twin of T-ryw-from-db: assert NO correctness property depends on an OM table cache.
  Run the migrated-command harness with the table cache forced OFF and assert every read/write
  outcome is identical to the cache-on run — read-your-writes comes from the lock-to-commit hold
  span (I-2/I-12) and a plain RocksDB get, never from a staged cache entry. A scenario that only
  passes with the cache present is a defect; the reference model is cache-free precisely to make
  this honest (D-3).
covers: [I-cache-free-ryw]
provenance: verified
evidence: ["leader-planned-execution.md D-3 tests:[T-ryw-from-db,T-no-cache-correctness], I-cache-free-ryw", "leader-execution-locking.md I-12 (cache-free correctness), I-2 (hold span)", "OzoneManagerDoubleBuffer.java:392,499-502 (cleanupCache epochs — the coupling removed)"]
```
```yaml
# T-txninfo-crash-atomicity
id: T-txninfo-crash-atomicity
statement: >
  The data patch and the #TRANSACTIONINFO (and #MANAGED_INDEX) advance commit in ONE RocksDB write
  batch — never one without the other. Inject a crash at the apply seam: (a) crash before
  commitBatchOperation and assert BOTH the data and the index advance are lost (the transition
  re-applies exactly once on recovery); (b) assert there is NO interleaving in which the persisted
  applied-index is ahead of the data it covers (which would re-apply / double-count a non-idempotent
  Merge or SCM allocation) or behind it (which would silently lose a committed transition). Exercises
  the dual-path applied-index durability extension (lastSkippedIndex) across both the legacy and the
  planned writer.
covers: [I-txninfo-atomic-with-patch, I-txninfo-atomic]
provenance: verified
evidence: ["leader-planned-execution.md I-txninfo-atomic-with-patch tests:[T-txninfo-crash-atomicity,T-apply-failure-resync], §15.4, §19 (dual-path durability)", "OzoneManagerDoubleBuffer.java:354,364-365,373-376,379-381 (single BatchOperation: data + #TRANSACTIONINFO + one commit)", "OzoneManagerStateMachine.java:108-111,242-269,582 (lastSkippedIndex)"]
```
```yaml
# T-full-suite-green-after-removal
id: T-full-suite-green-after-removal
statement: >
  After P-7 removes the double buffer and the OM table cache and deletes the legacy
  validateAndUpdateCache dispatch, the FULL regression suite is green with the legacy path GONE.
  Assert: the OperationApplier is the sole RocksDB writer; reads go to RocksDB (no addCacheEntry /
  no read-cache lookup remains); T-ryw-from-db is now the ONLY read-your-writes path; and every
  previously-green integration/linearizability test still passes with no flag toggling legacy on
  (because there is no legacy on). This is the irreversible-cleanup acceptance gate, run only after
  every command has migrated AND finalized.
covers: [I-cache-free-ryw]
provenance: inferred
evidence: ["leader-execution-components.md C-legacy-removal tests:[T-no-cache-correctness,T-full-suite-green-after-removal], depends_on:[P-3,P-4,P-5,P-6]", "leader-planned-execution.md D-3 (cache removal), P-7", "OzoneManagerDoubleBuffer.java:354 (flushBatch — removed at P-7)"]
```
```yaml
# T-mixed-mode-cross-model-race
id: T-mixed-mode-cross-model-race
statement: >
  With one command migrated and a sibling command on the same table still legacy, drive concurrent same-key
  operations across both paths; assert they serialize via the shared bucket lock and the final DB state is
  linearizable (no lost update, no dangling blocks).
covers: [I-mixed-mode-lock-gate]
provenance: inferred
evidence: ["D-17", "I-mixed-mode-lock-gate"]
```
```yaml
# T-mixed-mode-stale-read
id: T-mixed-mode-stale-read
statement: >
  Migrate a quota-bearing command; after it commits a write (key + bucket usedBytes), issue a legacy read op
  (LookupKey, InfoBucket) and a legacy command reading the same key/bucket; assert both observe the fresh
  value (no stale FullTableCache bucket, no stale PartialTableCache key).
covers: [I-mixed-mode-cache-coherent]
provenance: inferred
evidence: ["D-17", "I-mixed-mode-cache-coherent", "FullTableCache.java:200-213"]
```

---

## 5. Per-phase acceptance map (P-0 … P-7 → T-n)

This map is the test-side projection of the master phasing (`leader-planned-execution.md`
§29). It restates each phase's `must_pass` set and adds the T-n this companion contributes,
so that "phase done" is mechanically checkable. The master §29 blocks are the authority for
`must_pass`; any divergence here is a spec defect to reconcile (master §C rule 5 projection-
freshness).

| Phase | Scope (abbrev.) | Gating T-n (must pass) | Formal-tier gate |
|---|---|---|---|
| **P-0** | Framework substrate (12 components) unwired + legacy→ManagedIndex objectID retrofit + dual-path index durability | `T-cross-thread-release`, `T-objectid-disjoint`, `T-proto-roundtrip`, `T-mixed-mode-no-collision`, `T-flag-routing-both-paths` | (substrate; no model gate — models exercise P-1/P-2 behavior) |
| **P-1** | Hardest single-step OBS: CreateKey, CommitKey, AllocateBlock, DeleteKey | `T-quota-concurrent`, `T-ryw-from-db`, `T-determinism-follower-byte-identical`, `T-7` (hot parent), `T-holder-lease-negative` | **OBS model green** (`ObsImpl.cfg`: Refinement + LockInv + NoLeak + UsedConsistent) — `obs3-verdict.out` |
| **P-2** | Hardest multi-step FSO: CreateFile/CreateDirectory (implicit parents), FSO delete, recursive rm-rf + DirectoryDeletingService redesign | `T-1`, `T-2`, `T-3`, `T-4`, `T-5`, `T-6`, `T-7`, `T-8`, `T-deletedir-vs-openfile` | **FSO model green** (`FsoImpl.cfg`/`FsoImplSmall.cfg`: Refinement + LockInv + NoLeak + NoOrphan) — `fso-m2a-full.out`; broader configs planned |
| **P-3** | Snapshot: CreateSnapshot/Checkpoint op, SnapshotPurge standalone, moves | `T-snapshot-consistency` | (FSO model extension for Checkpoint-vs-op ordering: planned) |
| **P-4** | MPU (4 ops + AbortExpired) + large-value ([HDDS-8238](https://issues.apache.org/jira/browse/HDDS-8238)) revisit | `T-mpu-lifecycle` | n/a |
| **P-5** | Batch/background: DeleteKeys, RenameKey/Keys, DeleteOpenKeys, PurgeKeys/Directories | (multi-slot ordering exercised by the §5-companion bulk-sort path; covered transitively by `T-3`/`T-7` ordering + `NoLeak`) | n/a |
| **P-6** | Easy Set-A sweep (~22 single-table ops) | (per-command flag-routing parity: `T-flag-routing-both-paths` applied per op) | n/a |
| **P-7** | Cleanup: remove double buffer + table cache; delete legacy path; finalize | (full regression green with legacy path REMOVED; `T-ryw-from-db` now the only RYW path; `T-rolling-upgrade-mixed-binary` superseded by finalization) | OBS + FSO models green; `QuotaOvercommit.cfg` per `D-OPEN-quota-enforcement` resolution |

Cross-cutting (every phase that migrates a command):
- `T-determinism-follower-byte-identical` and `T-apply-failure-resync` (the `D-10`
  determinism + crash-and-resync contract) apply to **every** migrated command, not one phase.
- `T-flag-routing-both-paths` (`D-14`) applies to **every** migrated command.
- `T-quota-exact-tlc` is a **standing** formal artifact tracking `D-OPEN-quota-enforcement`;
  it is run on every change that touches the quota path and its expected verdict is governed by
  the open decision's current state (today: documents the limitation; on `exact` resolution:
  must turn green).

---

## 6. Traceability (invariant → T-n) and the zero-test-invariant audit

Per master §C rule 2, every `I-n` must appear in ≥1 `T-n.covers`. The matrix below is the
test-side half of the master §28 projection (the master's generator joins this with the C-n
`implements` and the P-n `must_pass`). **Locking companion invariants** (`I-1 … I-12`,
authority `leader-execution-locking.md` §9) and **master invariants** (slugs) are both audited.

### 6.1 Locking-companion invariants (I-1 … I-12)

| Invariant | Covered by | Source (companion §9 / this catalog) |
|---|---|---|
| `I-1` lock identity (objectID / (parentObjectID,name), never path) | `T-2`, `T-5`, `T-6` (every reval/rendezvous reads by objectID/slot key) | companion §3; this §4.1 |
| `I-2` hold span (leader, pre-submit → quorum-commit+apply) | `T-4`, `T-6`, `T-holder-lease-negative` | companion §9 (I-2 via T-4,T-6) |
| `I-3` no lock across the gate (per-step in multi-step) | `T-5` (chain interleaved by rename in the gap), `T-1` | D-16/D-6; this §4.1 |
| `I-4` creates don't serialize | `T-7`, `T-hot-stripe` | companion §9 (I-4 via T-7) |
| `I-5` FSO-RESOLVE-FAIL (tombstone ⇒ resolution fails) | `T-1`, `T-4`, `T-5`, `T-deletedir-vs-openfile` | companion §9 (I-5 via T-1,T-5) |
| `I-6` FSO-REVAL (re-read by (parentObjectID,name); ABA-safe) | `T-2`, `T-5`, `T-deletedir-vs-openfile` | companion §9 (I-6 via T-2,T-5) |
| `I-7` FSO-PURGE no orphan | `T-1` | companion §9 (I-7 via T-1); FSO `NoOrphan` (green) |
| `I-8` no holder lease (correctness-critical) | `T-8` (no double-apply), `T-holder-lease-negative` | companion §9 (I-8 via design + T-8) |
| `I-9` non-thread-affine (release on different thread) | `T-cross-thread-release` | companion §9 (I-9 unit: acquire A release B) |
| `I-10` leader-local (lock table discarded on failover) | `T-8` | companion §9 (I-10 via T-8) |
| `I-11` deadlock-free by total order | `T-3`, `T-5`, `T-hot-stripe` | companion §9 (I-11 via T-3,T-5) |
| `I-12` cache-free RYW | `T-7`, `T-ryw-from-db` | companion §9 (I-12 via T-7) |

### 6.2 Master-level invariants (slugs from `leader-planned-execution.md` §24)

| Invariant (slug) | Covered by | Source |
|---|---|---|
| `I-inner-domain-agnostic` | `T-proto-roundtrip`, `T-determinism-follower-byte-identical`, `T-mpu-lifecycle`, `T-rolling-upgrade-mixed-binary` | D-1/D-2 |
| `I-cache-free-ryw` | `T-ryw-from-db` (+ folded `T-no-cache-correctness`) | D-3 |
| `I-quota-commutative` | `T-quota-concurrent`, `T-quota-exact-tlc` | D-7; `ObsImpl` UsedConsistent green |
| `I-quota-crash-safe` | `T-quota-failover`, `T-quota-concurrent` | D-7; `ObsImpl` green |
| `I-determinism-followers-pure` | `T-determinism-follower-byte-identical`, `T-apply-failure-resync` | D-10 |
| `I-managed-index-monotonic` | `T-objectid-disjoint`, `T-mixed-mode-no-collision`, `T-flag-routing-both-paths`, `T-rolling-upgrade-mixed-binary` | D-8/D-12; P-0 must_satisfy |
| `I-objectid-disjoint` | `T-objectid-disjoint`, `T-mixed-mode-no-collision` | D-8/D-12 |
| `I-txninfo-atomic` (#TRANSACTIONINFO atomic with patch) | `T-apply-failure-resync` (the #TRANSACTION_INFO-in-same-batch atomicity is what makes resync-to-a-consistent-index sound) | P-0 must_satisfy; `OzoneManagerDoubleBuffer.java:354-382` |
| `I-checkpoint-exact-index` | `T-snapshot-consistency` | P-3 must_satisfy |

### 6.3 Audit result

- **No zero-test invariant** among `I-1 … I-12` or the master slugs enumerated in §24/§29:
  every invariant above has ≥1 covering `T-n`. The two invariants most at risk of being
  orphaned — `I-txninfo-atomic` and `I-checkpoint-exact-index` — are explicitly covered by
  `T-apply-failure-resync` and `T-snapshot-consistency` respectively.
- **Two master invariants are formally backstopped, not only harness-tested**:
  `I-quota-commutative`/`I-quota-crash-safe` (via `ObsImpl` `UsedConsistent`, green) and the
  FSO orphan-freedom `I-7` (via `FsoImpl` `NoOrphan`, green). This is the belt-and-suspenders
  the design wants: a sampled Java test AND an exhaustive bounded model agree.
- **Provenance honesty**: `T-snapshot-consistency` and `T-mpu-lifecycle` are `inferred`
  (specified, behavior anchored to real code, but the new-model implementation does not yet
  exist — they are P-3/P-4 deliverables). Everything in §4.1–§4.5 is `verified` against either
  a TLA+ artifact, a verdict file, or an exact code anchor that fixes the semantics under test.

---

## 7. What this plan deliberately does NOT test (negative scope)

Stated so a reviewer reads the omissions as intentional, not as gaps:

- **Request-level atomicity of composite operations.** Not tested because it is **not a
  property of the design** (`D-16`, §1.3). Testing it would assert a contract the system does
  not offer and `mkdir -p`/HDFS do not offer.
- **Exact quota-limit enforcement under concurrency.** Not asserted as a *passing* property
  today; it is asserted as a *documented, reproducible counterexample* (`T-quota-exact-tlc` /
  `QuotaOvercommit.cfg`) pending `D-OPEN-quota-enforcement`. The plan ships the exact oracle so
  the test exists for the day the decision lands; it does not pre-decide the decision.
- **Retry/idempotency mechanism correctness end-to-end.** `D-OPEN-retry` is **deferred**
  (`leader-planned-execution.md` D-OPEN-retry; `leader-execution-locking.md` §10) pending the
  per-operation idempotency audit. The terminal-step retry-cache *placement* is exercised by
  `T-8` (no double-apply), but the full durable-vs-in-memory replicated-response-table choice
  is not tested until the audit fixes which ops require the atomic-with-data-batch entry. The
  retry-cache code seam this will build on is verified at
  `OzoneManagerRatisServer.java:559-567` (`checkRetryCache` / `getRetryCache().getIfPresent`).
- **Synchronous quota release on `rm -rf`** and **eager empty-intermediate-dir cleanup** —
  `EXC-1` and `B-2` are accepted limitations; the harness asserts *eventual* convergence and
  *bounded, idempotent-on-retry* leftover dirs, not synchronous/eager behavior.
- **Large-value MPU network overhead** (`RC-xichen-large-value` / [HDDS-8238](https://issues.apache.org/jira/browse/HDDS-8238), status `open`).
  `T-mpu-lifecycle` asserts correctness on the new model and **flags** the whole-object
  overhead; it does not solve or benchmark-gate it (that is the P-4 large-value revisit).

---

## 8. References

- `leader-planned-execution.md` — master spec; §24 invariants, §26 (this plan's parent
  reference), §27 performance model + short-circuit-DB testability seam, §28 traceability,
  §29 phasing, §31 DoD ("TLA+ tiers green").
- `leader-execution-locking.md` — concurrency companion; §3 invariants `I-1 … I-12`, §7
  correctness criterion + `T-1 … T-8`, §8 bounds/exceptions (`B-1`, `B-2`, `B-3`, `EXC-1/2/3`),
  §9 traceability, §10 open items (`D-OPEN-retry`).
- TLA+/TLC models (`ozone-11898-tla`): `ObsImpl.tla`/`.cfg` + `ObsAbstract.tla` (OBS, green —
  `obs3-verdict.out`); `FsoImpl.tla`/`FsoImpl.cfg`/`FsoImplSmall.cfg` + `FsoAbstract.tla`
  (FSO, bounded-green — `fso-m2a-full.out`); `ObsAbstractExact.tla` + `QuotaOvercommit.cfg`
  (exact-quota counterexample for `D-OPEN-quota-enforcement`).
- Code anchors (verified against the worktree, this run): `OmUtils.java:766`
  (`getObjectIdFromTxId`); `OmMetadataManagerImpl.java:1775-1788` (`getOzonePathKey`, F-1);
  `OMKeyRenameRequestWithFSO.java:62,267` (F-2 O(1) re-parent); `OMKeyCreateRequestWithFSO.java:144`
  + `OMFileRequest.java:432-443` (createFile missing-parent materialization, D-16 decomposition
  seam); `OMKeyCommitRequest.java:410` (`incrUsedBytes`, quota); `OzoneManagerStateMachine.java:447-462`
  (`applyTransaction`, follower log→OMRequest); `OzoneManagerDoubleBuffer.java:340-382`
  (sole writer; `#TRANSACTION_INFO` atomic with batch; `splitReadyBufferAtCreateSnapshot` barrier);
  `OzoneManagerRatisServer.java:559-567` (retry-cache seam); `OMBucketCreateRequest.java:421,444`
  + `OMLayoutFeature.java:26` (finalization-gating pattern).
- Background: prototype [#7406](https://github.com/apache/ozone/pull/7406) (40k ops/sec baseline), review threads [#7583](https://github.com/apache/ozone/pull/7583) / [#10502](https://github.com/apache/ozone/pull/10502) / [#10503](https://github.com/apache/ozone/pull/10503),
  [HDDS-1595](https://issues.apache.org/jira/browse/HDDS-1595) (sequence-diagram exemplar), [HDDS-8238](https://issues.apache.org/jira/browse/HDDS-8238) (MPU large value), [RATIS-1210](https://issues.apache.org/jira/browse/RATIS-1210).
