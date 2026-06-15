---
title: Leader-Side Execution — Master Design Specification
summary: Leader computes each OM write once and replicates a deterministic DB patch; followers apply bytes with zero business logic. Master spec with rationale record, correctness contract, and a hard-first phased plan.
date: 2026-06-15
jira: HDDS-11898
status: draft (scaffold — sections marked TO-GENERATE are not yet written)
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

<!-- ===================================================================
  SCAFFOLD NOTICE (delete before publishing)

  This file is the MASTER TEMPLATE for the Leader-Side Execution spec.
  It is a hybrid-audience document: human prose for reviewers, plus a
  machine-readable structured block under each actionable unit so agents
  and a CI linter can act on it and keep it honest.

  Section bodies marked `> TO-GENERATE:` carry authoring guidance, not
  content — they are filled by the generation workflow (one agent per
  section/component/decision-cluster), each producing prose + block,
  cited to real code in the worktree, then a synthesis + lint pass.

  Part IV (the rationale spine) is SEEDED below with the real decisions,
  rejected alternatives, and review concerns from the design grill and
  the #7583 / #10502 / #10503 review threads. Generation expands each
  seed's prose; the YAML block is the frozen contract.
==================================================================== -->

# Leader-Side Execution — Master Design Specification

## §A. Conventions (normative — read first)

This spec uses numbered, cross-referenced, evidence-bearing tokens. Every load-bearing
claim cites evidence and is tagged `verified` (with `file:line` / PR# / reviewer) or
`inferred`. Any **`I-n` with zero `T-n` mapped is a spec defect** (enforced by the linter, §C).

| Token | Meaning |
|---|---|
| `F-n`  | Verified fact about existing code (carries `file:line`) |
| `A-n`  | Assumption the design relies on |
| `D-n`  | Decision (ADR): context, alternatives, rationale, status |
| `ALT-n`| Rejected alternative (standing entry; the "do-not-re-tread" wall) |
| `RC-n` | Review concern (reviewer + concern + resolution) |
| `I-n`  | Invariant (must-always / must-never) + rationale + failure-prevented |
| `B-n`  | Bound (numeric/capacity) + justification |
| `C-n`  | Component (a unit of buildable design) |
| `P-n`  | Phase (a unit of delivery) |
| `T-n`  | Test scenario |
| `R-n`  | Risk |
| `Q-n`  | Open question |
| `EXC-n`| Explicit exception (a deliberate, named deviation) |

### Structured-block schemas (the machine layer)

Each actionable unit carries a fenced ```` ```yaml ```` block beside its prose. Schemas:

```yaml
# D-n  (Decision / ADR)
id:            # D-<slug>
title:
status:        # locked | open | deferred | superseded
depends_on:    # [D-/F-/I- ...]   decisions/facts this rests on
enables:       # [I-/D- ...]      what this makes possible
rejects:       # [ALT-...]        alternatives this kills
addresses:     # [RC-...]         review concerns this settles
raised_by:     # [people]
deciders:      # [people]
consequences:  # [short bullets]
tests:         # [T-...]          evidence the decision holds
phase:         # P-n the decision first lands in
provenance:    # verified | inferred
evidence:      # [file:line / PR# / reviewer]
```
```yaml
# ALT-n  (Rejected alternative)
id:
title:
killed_by:     # D-n  (or `deferred_by: D-n` if shelved not dead)
reason:        # why it is dead, in one breath
proposed_by:   # [people]  (so it reads as 'heard', not dismissed)
evidence:      # [file:line / PR#]
```
```yaml
# RC-n  (Review concern)
id:
raised_by:
concern:
raised_on:     # PR#/date
status:        # adopted | addressed | deferred | open | rejected
resolved_by:   # [D-...]
endorsed_by:   # [people]
```
```yaml
# C-n  (Component)
id:
target_files:        # [paths to create/modify]
interface:           # the key signatures
depends_on:          # [D-/C-/F-...]
implements:          # [I-...]
tests:               # [T-...]
anti_patterns:       # [things this MUST NOT do]
phase:               # P-n
provenance: ...
evidence: ...
```
```yaml
# P-n  (Phase)
id:
scope:               # commands / work in this phase
depends_on_phases:   # [P-...]
must_satisfy:        # [I-...]
must_pass:           # [T-...]
config_flag:
acceptance:          # machine-checkable "done"
```
```yaml
# I-n / B-n / T-n
id:
statement:           # I-n: the must/never ; B-n: the number+unit ; T-n: the scenario
rationale:           # I-n: failure prevented ; B-n: why this value
covers:              # T-n: [I-...] invariants this exercises
provenance: ...
evidence: ...
```

> Projections — the **traceability matrix (§28), decision-dependency graph (§22),
> per-phase task list (§29), and invariant-coverage report (§28)** — are GENERATED from
> these blocks. Never hand-maintain them.

## §B. Document map

| Doc | Role | Status |
|---|---|---|
| `leader-planned-execution.md` (this) | Master: orientation, why, design overview, **rationale spine**, correctness-contract index, delivery plan | scaffold |
| `leader-execution-locking.md` | Companion: concurrency & locking model (container/slot, linearizability) | drafted |
| `leader-execution-components.md` (or per-component set) | Companion: the 12 component deep-dives | TO-GENERATE |
| `leader-execution-test-plan.md` | Companion: full test strategy + T-n catalog | TO-GENERATE |
| `leader-execution-phasing.md` | Companion: per-command migration playbook | TO-GENERATE |

## §C. Consistency lint (CI gate — mechanical, no agent)

The docs build runs `lint-spec` over all blocks in master + companions and FAILS on:
1. **ref-resolution** — every `depends_on/enables/rejects/addresses/tests/implements/covers/resolved_by` id exists.
2. **coverage** — every `I-n` appears in ≥1 `C-n.implements` AND ≥1 `T-n.covers` (the zero-test-invariant rule).
3. **no-orphans** — every `ALT-n` has a `killed_by`/`deferred_by`; every `RC-n` a `status`; no unreferenced `D-n`.
4. **status-sanity** — no `locked` `D-n` `depends_on` an `open` `D-n`.
5. **projection-freshness** — regenerate matrix/graph/task-list and diff == 0.
6. **anchor-existence** — every `evidence`/`target_files` `file:line` or symbol resolves in the source tree (`rg`/`sg`).
Semantic coherence (prose↔block, design soundness, "anchor still means what's claimed") is a separate **on-demand agent pass**, not in CI.

---

# PART I — Orientation

## 1. How to read this spec
> TO-GENERATE: audience reading paths — Reviewer (read §2 summary + Part IV rationale), Implementer (Part III components + Part V tests + Part VI phasing), Newcomer (§2 + §5 history + glossary), Consensus-seeker (Part IV RC-n ledger). One short paragraph each.

## 2. Executive summary (the human narrative)
> TO-GENERATE — LONG, prose, phrased-to-read, readable standalone. Tell the story: the problem (OM caps at ~12k ops/s, double buffer batches 1.2x, every follower re-runs business logic risking silent divergence, objectID welded to Ratis, bucket-lock contention) → the idea (leader executes once, replicates a deterministic DB patch, followers apply bytes) → the load-bearing decisions and WHY (Ethan's module, commutative quota merge, no cache on NVMe, fine-grained objectID locking, fully-backwards-compatible finalization gating, multi-step orchestration) → what changes and what deliberately does NOT (client RPC and on-disk schema invariant) → the hard-first phased plan and how mixed-mode is safe. Several pages is fine. No bullets-only; write paragraphs a human will actually read.

## 3. Nomenclature / glossary
> TO-GENERATE: define every term. Must disambiguate the ones the reviews tripped on: leader-side execution; planned request; transition vs request; replicated-DB module; managed index; container lock vs slot lock; merge operator (Option B); retry cache (NOT "replay cache" — RC-ivandika-terminology); finalization vs prepare; OBS vs FSO.

# PART II — Why

## 4. Background & problem statement
> TO-GENERATE: current pain, quantified. Source: §9 current architecture + perf numbers (12k/40k, 25k Ratis cap, 1.2x batching). Cite F-n.

## 5. Project history & prior art
> TO-GENERATE: honest archaeology so newcomers inherit context. #7583 (sumit, deep, stalled to auto-close Nov 2025), #10502 (closed in minutes), #10503 (abhishek, condensed — regressed on quota/retry/batching/upgrade), prototype #7406 (proved 40k). The abandonment risk and what is different now (hard-first, formal TLA+ validation, rationale spine).

## 6. Goals & non-goals
> TO-GENERATE: explicit out-of-scope (multi-Ratis, UUID/global objectIDs, a new caching architecture, FSO in-memory inode trees, MPU large-value redesign HDDS-8238). Non-goals pre-empt tangents.

## 7. Guiding principles
> TO-GENERATE: leader-executes-once; followers-apply-blind; on-disk + RPC invariance; fail-loud-not-silently-divergent; the reference model is defined at transition granularity (composite requests inherit decomposed semantics).

## 8. Assumptions (A-n)
> TO-GENERATE: A-1 NVMe + RocksDB block cache make OM table cache marginal; A-2 Ratis applies a committed entry exactly once; A-3 finalization is one-way; A-4 no volume-rename op exists; A-5 merge operator registered on every node before any node can receive a Merge. Each with "if this breaks, X breaks."

# PART III — What (the design)

## 9. Current architecture (the "before")
> TO-GENERATE from verified map: double buffer is the sole RocksDB writer (flushBatch + #TRANSACTIONINFO atomic; splitReadyBufferAtCreateSnapshot barrier); validateAndUpdateCache runs on EVERY node (OzoneManagerStateMachine.java:446→668); table cache epoch = Ratis index; objectID = getObjectIdFromTxId(epoch, ratisIndex) (OmUtils.java:766-783); bucket write lock serializes key ops. Cite F-n throughout.

## 10. Proposed architecture overview
> TO-GENERATE: the "after" — leader plans → replicated-DB Batch over Ratis → followers apply. Diagram. Points to §11 components.

## 11. Component designs (C-n)  →  companion
> TO-GENERATE (companion): the 12 components, each a C-n block + prose + worked seam to existing code. See §29 phasing for build order. (replicated-DB module; ManagedIndexService; lean lock manager; orchestrator/LeaderPlanner; PlannedRequest + change recorder; dual-path state machine; OMLayoutFeature gate; quota merge operator (Option B); retry [deferred]; per-command subclasses; test harness; late-removal of double buffer + cache.)

## 12. Proto & API contracts
> TO-GENERATE: frozen inner `Batch{Operation: Put/Delete/Merge/Checkpoint}` (domain-agnostic bytes) + outer OM envelope (managed index, ClientRequestInfo[], OMResponse). The "inner never deserializes a domain object" invariant (I-, from D-2).

## 13. Worked examples & sequence diagrams
> TO-GENERATE: end-to-end walk-throughs as executable oracles — createKey; commitKey (quota merge + overwrite soft-delete); createFile /a/b/c/file with missing parents (multi-step chain); rm -rf with concurrent create (no orphan); leader failover mid-orchestration. Sequence diagrams (ivandika3 asked for these, HDDS-1595 style — RC-ivandika-seqdiagram).

## 14. Concurrency & locking model  →  companion `leader-execution-locking.md`
> Reference only. The companion is the contract; do not re-derive here.

## 15. Failure modes & recovery
> TO-GENERATE: failure-injection per I/O seam (RPC, RocksDB write, DNS); the crash-and-resync follower contract (D-10); partial-state semantics of multi-step; atomic-replace patterns; #TRANSACTIONINFO-with-patch atomicity.

## 16. Upgrade, compatibility & mixed-mode
> TO-GENERATE from D-11/D-12/D-14: fully backwards-compatible; finalization gate; per-command runtime flag; index handoff (managed index seeded max(Ratis idx)+1); the legacy→ManagedIndex objectID retrofit (mixed-mode collision prevention); downgrade stance.

## 17. Security considerations
> TO-GENERATE: ACL/authorization runs on leader (authorize step); followers apply trusted bytes (no re-check); audit becomes leader-only; tokens/secrets ops.

## 18. Observability
> TO-GENERATE: metrics that move to leader-only; new metrics; audit (leader-only — RC-ivandika-audit); logging; tracing.

## 19. Cross-cutting / downstream impact (blast radius)
> TO-GENERATE from verified sweep: AFFECTED (7) — DirectoryDeletingService recursive-delete redesign; purge flush-fences off the double buffer; snapshot Checkpoint-at-exact-index; SnapshotPurge standalone; dual-path applied-index durability (extend lastSkippedIndex); #TRANSACTIONINFO atomicity; audit/metrics leader-only. UNAFFECTED (on-disk invariance shields) — Recon (WAL getUpdatesSince), SnapshotDiff, S3 Gateway, deletion-service scan sides, SnapshotDiffCleanupService. Cite file:line.

# PART IV — Why these choices (RATIONALE SPINE — seeded; expand prose, freeze blocks)

## 20. Design decisions (D-n)

> Each entry below is SEEDED from the grill + review threads. Generation expands the prose
> (context / forces / alternatives-walked / consequences / consensus) under each; the YAML
> block is frozen.

### D-1 — Anchor on Ethan Rose's replicated-DB module
```yaml
id: D-1
title: Replicate a domain-agnostic DB patch (Put/Delete/Merge/Checkpoint), not abstract commands
status: locked
depends_on: [F-currentarch]
enables: [D-2, D-7, I-inner-domain-agnostic]
rejects: [ALT-raw-putdelete-only, ALT-journal-not-dbchanges]
addresses: [RC-ethan-merge-module, RC-xichen-journal-vs-dbchanges]
raised_by: [ethan-rose]
deciders: [kerneltime]
consequences: ["followers do zero business logic", "Recon/SCM could reuse the module", "Checkpoint op subsumes the snapshot barrier"]
tests: [T-determinism-follower-byte-identical]
phase: P-0
provenance: verified
evidence: ["PR#7583 review (errose28)", "nandakumar131 +1"]
```

### D-2 — Module in `hadoop-hdds/framework`; two-layer proto; inner stays domain-agnostic
```yaml
id: D-2
title: Package in framework (no premature extraction); inner Batch domain-agnostic, outer OM envelope
status: locked
depends_on: [D-1]
enables: [I-inner-domain-agnostic]
rejects: [ALT-module-extraction-now]
raised_by: [ethan-rose, kerneltime]
deciders: [kerneltime]
consequences: ["Recon-as-listener, follower-reads, SCM reuse fall out for free", "extraction is a later no-behavior-change PR"]
tests: [T-proto-roundtrip]
phase: P-0
provenance: verified
evidence: ["PR#7583 review", "grill 2026-06-12"]
```

### D-3 — Eliminate all OM-level table caching for migrated commands
```yaml
id: D-3
title: Remove write-staging AND read caches; reads go to RocksDB (NVMe + block cache)
status: locked
depends_on: [A-nvme, D-5]
enables: [I-cache-free-ryw]
rejects: [ALT-keep-cache, ALT-cf-callback-cache]
raised_by: [kerneltime, ethan-rose]
deciders: [kerneltime]
consequences: ["removes the cache-epoch↔Ratis-index coupling (a known OM bug class)", "future caching = separate post-refactor effort"]
tests: [T-ryw-from-db, T-no-cache-correctness]
phase: P-0
provenance: verified
evidence: ["grill 2026-06-13 (kerneltime: OM on NVMe)"]
```

### D-4 — Build a lean lock manager; do not reuse `OzoneManagerLock`
```yaml
id: D-4
title: Striped non-thread-affine semaphore-RW lock manager keyed by objectID/slot
status: locked
depends_on: [F-ozonemanagerlock-thread-affine]
rejects: [ALT-reuse-ozonemanagerlock]
addresses: [RC-szetszwo-split-locking-doc]
raised_by: [kerneltime]
deciders: [kerneltime]
consequences: ["releasable on a different thread than acquired (async orchestration)", "no per-type maps/trackers/reentrancy"]
tests: [T-cross-thread-release, T-3, T-7]
phase: P-0
provenance: verified
evidence: ["leader-execution-locking.md D-PARENT-4, I-9"]
```

### D-5 — Lock hold span = leader, pre-submit→post-commit; NO lock timeout
```yaml
id: D-5
title: Hold locks on leader from before Ratis submit until quorum-commit+apply; no holder lease
status: locked
depends_on: [D-4]
enables: [I-cache-free-ryw]
rejects: [ALT-lock-timeout]
raised_by: [ethan-rose, xichen01, kerneltime]
deciders: [kerneltime]
consequences: ["read-your-writes without a cache", "a holder lease would break mutual exclusion — Ratis req timeout + release-on-completion + failover suffice"]
tests: [T-4, T-6, T-holder-lease-negative]
phase: P-0
provenance: verified
evidence: ["leader-execution-locking.md I-2, I-8", "grill: user caught the lease flaw"]
```

### D-6 — Multi-step framework contract: dynamic step-iterator owned by the request
```yaml
id: D-6
title: One request → an ordered chain of per-step-locked transitions; request owns the iterator
status: locked
depends_on: [D-5]
enables: [D-16]
rejects: [ALT-stateless-request-orchestrator, ALT-static-step-decomposition]
raised_by: [kerneltime]
deciders: [kerneltime]
consequences: ["implicit mkdir-p + rm-rf expressible", "reval can change the plan between steps", "retry-cache entry on terminal step only", "single-step ops are the degenerate N=1 case"]
tests: [T-5, T-8, T-1]
phase: P-0
provenance: verified
evidence: ["leader-execution-locking.md §4", "grill 2026-06-14"]
```

### D-7 — Quota via merge operator, Option B (module-applied at apply)
```yaml
id: D-7
title: Commutative quota via a Merge op resolved by a registered operator at apply (whole-row write)
status: locked
depends_on: [D-1, D-3]
enables: [I-quota-commutative, I-quota-crash-safe]
rejects: [ALT-quota-reserved-static, ALT-quota-wholerow-put]
deferred_alternatives: [ALT-quota-rocksdb-native]
addresses: [RC-ethan-merge-module, RC-kerneltime-minimal-apply]
raised_by: [ethan-rose, nandakumar131]
deciders: [kerneltime]
consequences: ["no on-disk representation change (whole-row Put, no operands)", "no RocksDB-native-merge dependency", "increment resolved in Ratis order on every node"]
tests: [T-quota-concurrent, T-quota-failover]
phase: P-1
provenance: verified
evidence: ["PR#7583 review (errose28 merge operator)", "Option B chosen grill 2026-06-14"]
```

### D-8 — objectID = getObjectIdFromTxId(epoch, managedIndex); retire the 256-window
```yaml
id: D-8
title: Keep the objectID encoding; each object gets one managed index; retire the per-request 256 window
status: locked
depends_on: [D-12]
raised_by: [kerneltime]
deciders: [kerneltime]
consequences: ["old/new objectID disjoint by construction on upgrade", "low 8 bits go dead-zero", "format continuity for every tool that decodes objectIDs"]
tests: [T-objectid-disjoint]
phase: P-0
provenance: verified
evidence: ["OmUtils.java:766-783", "leader-execution-locking.md §4.1, locking-1"]
```

### D-9 — deleteDir emptiness = committed children only (open files never block delete)
```yaml
id: D-9
title: Directory emptiness defined over committed children; in-flight open files do not pin a directory
status: locked
raised_by: [kerneltime]
deciders: [kerneltime]
consequences: ["open file under a deleted dir fails reval at commit; open entry GC'd by OpenKeyCleanupService", "matches current FSO (open keys aren't dir entries)"]
tests: [T-deletedir-vs-openfile]
phase: P-2
provenance: verified
evidence: ["grill locking-2 2026-06-14"]
```

### D-10 — Determinism + crash-and-resync follower-apply-failure contract
```yaml
id: D-10
title: Leader resolves all non-determinism; a follower that cannot apply a committed patch crashes and re-syncs
status: locked
depends_on: [D-1]
consequences: ["split-brain (silent divergence) eliminated, traded for loud fail-stop", "local failure → crash+resync; uniform failure → loud cluster-wide stop"]
raised_by: [kerneltime]
deciders: [kerneltime]
tests: [T-determinism-follower-byte-identical, T-apply-failure-resync]
phase: P-0
provenance: verified
evidence: ["grill Q7 2026-06-14"]
```

### D-11 — Fully backwards-compatible; finalization-gated
```yaml
id: D-11
title: No backwards-compat break; client RPC + logical on-disk schema invariant; gated by OMLayoutFeature
status: locked
depends_on: [D-7]
rejects: [ALT-no-backwards-compat]
addresses: [RC-kerneltime-minimal-apply]
raised_by: [kerneltime, szetszwo]
deciders: [kerneltime]
consequences: ["new PersistDb entry + #MANAGED_INDEX additive, inert until finalization", "Option B keeps physical representation unchanged too"]
tests: [T-rolling-upgrade-mixed-binary]
phase: P-0
provenance: verified
evidence: ["OMLayoutFeature.java", "isAllowed usage OMBucketCreateRequest.java:421,444", "grill Q8"]
```

### D-12 — Retrofit the legacy path to source objectID/updateID from ManagedIndex
```yaml
id: D-12
title: Both legacy and new paths draw objectID/updateID from one managed counter (mixed-mode collision prevention)
status: locked
depends_on: [D-11]
enables: [D-8]
raised_by: [kerneltime]
deciders: [kerneltime]
consequences: ["prevents legacy(Ratis-idx) vs new(managed-idx) objectID collision during long-lived mixed mode", "Phase-0 prerequisite before ANY command migrates"]
tests: [T-objectid-disjoint, T-mixed-mode-no-collision]
phase: P-0
provenance: verified
evidence: ["PR#7583 ('use managed index in both flows')", "grill catch 2026-06-15"]
```

### D-13 — Hard-first phasing
```yaml
id: D-13
title: Migrate hard commands/scenarios first; defer easy leg-work; land incrementally in master
status: locked
depends_on: [D-6, D-14]
rejects: [ALT-value-first-phasing]
raised_by: [kerneltime]
deciders: [kerneltime]
consequences: ["showstoppers (multi-step FSO, recursive delete, quota, snapshot) retired by end P2-P3", "mitigates abandonment of an intrusive refactor", "matches Sumit's createKey beachhead"]
tests: []
phase: P-0
provenance: verified
evidence: ["grill 2026-06-14 (user overruled value-first)"]
```

### D-14 — Per-command runtime config flag (default legacy) for revert
```yaml
id: D-14
title: Each migrated command behind a runtime flag defaulting to legacy; finalization is the separate binary-safety gate
status: locked
depends_on: [D-11]
raised_by: [kerneltime, sumitagrawl]
deciders: [kerneltime]
consequences: ["operational revert without downgrade", "master always stable; long-lived mixed mode is first-class"]
tests: [T-flag-routing-both-paths]
phase: P-0
provenance: verified
evidence: ["prototype #7406 had per-command flags", "grill Q8"]
```

### D-15 — Lock permit pool = large constant (writer-drains-all)
```yaml
id: D-15
title: RW semaphore per stripe with a large fixed permit pool; reader takes 1, writer drains all; admission control is separate
status: locked
depends_on: [D-4]
raised_by: [kerneltime]
deciders: [kerneltime]
consequences: ["decouples correctness from any in-flight estimate", "write-admission backpressure is a separate memory concern, not the lock ceiling"]
tests: [T-7, T-hot-stripe]
phase: P-0
provenance: verified
evidence: ["grill locking-4 2026-06-14"]
```

### D-16 — createFile-with-missing-parents reference model = iterative mkdir -p
```yaml
id: D-16
title: Non-atomic multi-dir create is the DEFINED, contract-correct semantics (matches mkdir -p / HDFS), not a compromise
status: locked
depends_on: [D-6]
raised_by: [kerneltime]
deciders: [kerneltime]
consequences: ["atomicity is unobservable without cross-op isolation (justifies I-3 no-lock-across-gap)", "linearizability oracle decomposes createFile into the iterative chain"]
tests: [T-5, T-8]
provenance: verified
phase: P-2
evidence: ["grill locking-5 2026-06-15"]
```

### D-OPEN-quota-enforcement — exact vs approximate (OPEN, leaning leader-local reservation)
```yaml
id: D-OPEN-quota-enforcement
title: Whether quota admission is exact (leader-local atomic reservation) or approximate (merge-only)
status: open
depends_on: [D-7]
raised_by: [kerneltime, ivandika3]
consequences: ["main-chat grill leaned approximate/eventually-consistent (locking-3)", "TLA+ fork CONFIRMED over-commit via TLC counterexample and recommends leader-local reservation (exact; DB merge stays durable truth; decrement-on-abort; rebuild-from-DB on failover)"]
tests: [T-quota-concurrent, T-quota-exact-tlc]
provenance: verified
evidence: ["TLC counterexample 2026-06-15 (ozone-11898-tla)", "grill locking-3"]
```

### D-OPEN-retry — idempotency / retry-cache mechanism (DEFERRED)
```yaml
id: D-OPEN-retry
title: In-flight registry + durable replicated response table vs in-memory-only; pending per-op idempotency audit
status: deferred
depends_on: [D-7]
raised_by: [ivandika3, kerneltime]
addresses: [RC-ivandika-retry-cache-semantics]
consequences: ["DB batch already idempotent (whole-object puts); only RE-EXECUTION is non-idempotent (SCM alloc + quota Merge) → audit scope ~10 ops, not 47", "atomic-with-data-batch is the likely invariant for non-idempotent ops"]
tests: []
provenance: verified
evidence: ["per-command inventory 2026-06-15", "leader-execution-locking.md §10"]
```

### Spec-process decisions (about this document, not the design)
```yaml
- {id: D-SPEC-1, title: "Master spec + companions; rationale spine in the master", status: locked, deciders: [kerneltime], consequences: ["house-dialect master is the canonical website entry", "locking/per-component/test-plan/phasing are linked companions"], provenance: verified, evidence: ["grill 2026-06-15"]}
- {id: D-SPEC-2, title: "Dual-representation + generated projections + mechanical CI lint (no agent) + on-demand agent semantic pass", status: locked, deciders: [kerneltime], consequences: ["spec is a consistency-checkable graph", "projections never hand-maintained", "CI requires no agent"], provenance: verified, evidence: ["grill 2026-06-15"]}
- {id: D-SPEC-3, title: "Decision record = D-n/ALT-n/RC-n, cross-linked, back-filled from grill + review threads", status: locked, rejects: [ALT-forward-only-record], deciders: [kerneltime], consequences: ["the rationale is the asset; re-litigation hits a documented wall"], provenance: verified, evidence: ["grill 2026-06-15"]}
```

## 21. Rejected alternatives (ALT-n) — the do-not-re-tread wall
```yaml
- {id: ALT-quota-reserved-static,  title: "In-memory reserved-quota static AtomicLong maps", killed_by: D-7, reason: "commutative but reserve state lives OUTSIDE the DB in static maps → crash-recovery + reset-on-failure complexity", proposed_by: [sumitagrawl], evidence: ["QuotaResource.java (PR#7406)"]}
- {id: ALT-quota-wholerow-put,     title: "Whole-row bucket PUT under bucket write lock", killed_by: D-7, reason: "does not commute → forces bucket serialization → kills the parallelism the feature exists for", proposed_by: [], evidence: []}
- {id: ALT-quota-rocksdb-native,   title: "RocksDB-native merge operator (operands on disk)", deferred_by: D-7, reason: "changes bucketTable physical representation + needs DB-layer merge support; Option B avoids both", proposed_by: [ethan-rose], evidence: []}
- {id: ALT-keep-cache,             title: "Keep OM table caches", killed_by: D-3, reason: "marginal on NVMe (RocksDB block cache); retains the cache-epoch↔Ratis-index bug class", proposed_by: [], evidence: []}
- {id: ALT-cf-callback-cache,      title: "Column-family async cache-update callbacks", killed_by: D-3, reason: "superseded by removing the cache entirely", proposed_by: [ethan-rose], evidence: ["PR#7583 review"]}
- {id: ALT-mgl-ancestor-locking,   title: "Multiple-granularity (ancestor) locking", killed_by: D-4, reason: "over-strict for Ozone FSO: rename is O(1) re-parent (F-2), descendants untouched, so ancestors need no lock", proposed_by: [szetszwo], evidence: ["leader-execution-locking.md F-2"]}
- {id: ALT-reuse-ozonemanagerlock, title: "Reuse existing OzoneManagerLock", killed_by: D-4, reason: "thread-affine (breaks async hold) + 8 leveled resources/trackers/reentrancy this design does not need", proposed_by: [], evidence: ["leader-execution-locking.md I-9"]}
- {id: ALT-lock-timeout,           title: "Per-lock acquisition timeout", killed_by: D-5, reason: "a holder lease that expires a held lock mid-flight lets waiter + holder both mutate → mutual-exclusion violation", proposed_by: [sumitagrawl], evidence: ["obs-locking.md timeout note"]}
- {id: ALT-no-backwards-compat,    title: "Clean-cut, no backwards compatibility", killed_by: D-11, reason: "rolling upgrade runs mixed binaries; new leader → old follower PersistDb is split-brain; finalization gating gives compat for free", proposed_by: [], evidence: ["PR#10503 §3"]}
- {id: ALT-value-first-phasing,    title: "Migrate easy/high-value commands first", killed_by: D-13, reason: "intrusive refactor risks abandonment in permanent mixed-mode if hard bits stall after easy bits 'prove value'", proposed_by: [], evidence: []}
- {id: ALT-static-step-decomposition, title: "Pre-compute the full multi-step chain up front", killed_by: D-6, reason: "concurrent deletes can invalidate a statically-planned chain; reval must re-resolve per step", proposed_by: [], evidence: []}
- {id: ALT-stateless-request-orchestrator, title: "Hold chain state in the orchestrator, stateless requests", killed_by: D-6, reason: "request owns resolution context across the gap; chosen for locality of the decomposition logic", proposed_by: [], evidence: []}
- {id: ALT-module-extraction-now,  title: "Extract the replicated-DB module to its own Maven module in V1", killed_by: D-2, reason: "premature; lock the API surface in framework, extract later as a no-behavior-change PR", proposed_by: [], evidence: []}
- {id: ALT-raw-putdelete-only,     title: "Raw Put/Delete bytes only (no Merge/Checkpoint)", killed_by: D-1, reason: "cannot express commutative quota or the snapshot barrier; #10503 regressed to this", proposed_by: [], evidence: ["PR#10503"]}
- {id: ALT-journal-not-dbchanges,  title: "Replicate a journal of commands, not DB changes", killed_by: D-1, reason: "abstract-command apply is the per-command-unique step that causes today's divergence; DB-changes is how consensus normally works", proposed_by: [xichen01], evidence: ["PR#7583 review (errose28 reply)"]}
- {id: ALT-forward-only-record,    title: "Forward-only decision record", killed_by: D-SPEC-3, reason: "discards the most expensive asset (the rationale); a transcript is not addressable or CI-checkable", proposed_by: [], evidence: []}
```

## 22. Decision-dependency graph
> GENERATED from D-n `depends_on`/`enables`. Do not hand-write. (Shows e.g. D-7 merge → enables no-bucket-lock parallelism; D-3 no-cache ← D-5 lock-hold-to-commit; D-12 → D-8 objectID disjointness.)

## 23. Review & consensus ledger (RC-n)
```yaml
- {id: RC-ethan-merge-module, raised_by: ethan-rose, concern: "Make replication an Ozone-agnostic module: Put/Delete/Merge/Checkpoint; quota via merge operator", raised_on: "PR#7583", status: adopted, resolved_by: [D-1, D-7], endorsed_by: [nandakumar131]}
- {id: RC-ethan-caching, raised_by: ethan-rose, concern: "Serialized apply makes write-through caching hard; CF-level callbacks, non-fatal updates", raised_on: "PR#7583", status: superseded, resolved_by: [D-3], endorsed_by: []}
- {id: RC-szetszwo-mgl, raised_by: szetszwo, concern: "Lock tree should lock ancestors (MGL); why no volume/root lock; volume rename?", raised_on: "PR#7583", status: addressed, resolved_by: [D-4], endorsed_by: []}
- {id: RC-szetszwo-split-locking-doc, raised_by: szetszwo, concern: "Split OBS locking into its own design doc", raised_on: "PR#7583", status: adopted, resolved_by: [D-4], endorsed_by: []}
- {id: RC-szetszwo-target3, raised_by: szetszwo, concern: "Compatibility may not matter if target is Ozone 3.0.0", raised_on: "PR#7583", status: addressed, resolved_by: [D-11], endorsed_by: []}
- {id: RC-ivandika-retry-cache-semantics, raised_by: ivandika3, concern: "Batched Ratis txn answers many clients; how do retry/reply caches work?", raised_on: "PR#7583", status: deferred, resolved_by: [D-OPEN-retry], endorsed_by: []}
- {id: RC-ivandika-terminology, raised_by: ivandika3, concern: "Use 'retryCache' not 'replayCache' (align with Ratis)", raised_on: "PR#7583", status: adopted, resolved_by: [], endorsed_by: []}
- {id: RC-ivandika-audit, raised_by: ivandika3, concern: "Write audit logs will be leader-only now", raised_on: "PR#7583", status: addressed, resolved_by: [D-10], endorsed_by: []}
- {id: RC-ivandika-seqdiagram, raised_by: ivandika3, concern: "Add detailed sequence diagrams (HDDS-1595 style)", raised_on: "PR#7583", status: open, resolved_by: [], endorsed_by: []}
- {id: RC-xichen-large-value, raised_by: xichen01, concern: "Large DB values (MPU, HDDS-8238) → big network overhead sending whole objects", raised_on: "PR#7583", status: open, resolved_by: [], endorsed_by: [ivandika3]}
- {id: RC-xichen-journal-vs-dbchanges, raised_by: xichen01, concern: "Syncing DB changes (vs a journal) may limit future features (FSO inode trees)", raised_on: "PR#7583", status: addressed, resolved_by: [D-1], endorsed_by: []}
- {id: RC-kerneltime-minimal-apply, raised_by: kerneltime, concern: "Keep apply minimal/idempotent; no read-modify-write in apply (rolling-upgrade); migrate incrementally; benchmark before complexity", raised_on: "PR#7583", status: adopted, resolved_by: [D-7, D-10, D-11, D-13], endorsed_by: []}
```

# PART V — Correctness contract

## 24. Invariants (I-n)
> TO-GENERATE: pull the parent-level invariants here (inner-domain-agnostic; cache-free-RYW; quota-commutative/crash-safe; determinism; #TRANSACTIONINFO-atomic-with-patch; managed-index monotonic; objectID-disjoint). Locking invariants live in the companion (I-1..I-12 there). Each I-n needs ≥1 T-n (CI-enforced).

## 25. Bounds (B-n)
> TO-GENERATE: B-managed-index-max; B-lock-stripe-size; B-permit-pool; B-batch-size; B-retry-expiry. Each with justification.

## 26. Test strategy & plan (T-n)  →  companion
> TO-GENERATE (companion): the linearizability harness + sequential reference model (transition-granularity, per D-16); unit/integration/chaos; determinism (leader vs follower byte-identical); failure-injection; the TLA+ models (OBS green; FSO planned) as a formal tier. Per-phase acceptance maps here.

## 27. Performance model & validation
> TO-GENERATE: prototype 40k baseline + methodology; per-phase benchmark gates; regression guards; the testability framework (short-circuit DB without Ratis).

## 28. Traceability matrix
> GENERATED from blocks: I/B ↔ T ↔ C ↔ P ↔ JIRA. Zero-test invariants flagged.

# PART VI — How & when (delivery)

## 29. Implementation plan / phasing (P-n)
```yaml
- {id: P-0, scope: "framework substrate (12 components) unwired + legacy→ManagedIndex objectID retrofit + dual-path index durability", depends_on_phases: [], must_satisfy: [I-inner-domain-agnostic, I-txninfo-atomic, I-managed-index-monotonic], must_pass: [T-cross-thread-release, T-objectid-disjoint, T-proto-roundtrip], config_flag: "n/a (inert)", acceptance: "zero behavior change; all unit tests green; lint-spec passes"}
- {id: P-1, scope: "hardest single-step OBS: CreateKey, CommitKey, AllocateBlock, DeleteKey", depends_on_phases: [P-0], must_satisfy: [I-quota-commutative, I-cache-free-ryw], must_pass: [T-quota-concurrent, T-ryw-from-db], config_flag: "ozone.om.leader.execution.obs.key.enabled", acceptance: "OBS key path on new model; perf ≥ baseline; quota correct"}
- {id: P-2, scope: "hardest multi-step FSO: CreateFile/CreateDirectory (implicit parents), FSO delete, recursive rm-rf + DirectoryDeletingService redesign", depends_on_phases: [P-1], must_satisfy: [I-3, I-5, I-6, I-7, I-11], must_pass: [T-1, T-2, T-3, T-4, T-5, T-6, T-7, T-8], config_flag: "ozone.om.leader.execution.fso.enabled", acceptance: "FSO linearizable under T-1..T-8; no orphan; showstoppers retired"}
- {id: P-3, scope: "snapshot: CreateSnapshot/Checkpoint op, SnapshotPurge standalone, moves", depends_on_phases: [P-2], must_satisfy: [I-checkpoint-exact-index], must_pass: [T-snapshot-consistency], config_flag: "ozone.om.leader.execution.snapshot.enabled", acceptance: "snapshot image == DB@createTransactionInfo index"}
- {id: P-4, scope: "MPU (4 ops + AbortExpired) + large-value (HDDS-8238) revisit", depends_on_phases: [P-3], must_satisfy: [], must_pass: [T-mpu-lifecycle], config_flag: "ozone.om.leader.execution.mpu.enabled", acceptance: "MPU on new model"}
- {id: P-5, scope: "batch/background: DeleteKeys, RenameKey/Keys, DeleteOpenKeys, PurgeKeys/Directories", depends_on_phases: [P-2], must_satisfy: [], must_pass: [], config_flag: "per-command", acceptance: "leg work complete"}
- {id: P-6, scope: "easy Set-A sweep (~22 single-table ops: ACLs, tagging, SetTimes, secrets, tokens, tenant, snapshot props, vol/bucket props, prepare)", depends_on_phases: [P-1], must_satisfy: [], must_pass: [], config_flag: "per-command", acceptance: "legacy path retired for simple ops"}
- {id: P-7, scope: "cleanup: remove double buffer + table cache; delete legacy path; finalize", depends_on_phases: [P-3, P-4, P-5, P-6], must_satisfy: [], must_pass: [], config_flag: "n/a", acceptance: "double buffer gone; single execution model"}
```
> Per-phase command detail + JIRA breakdown → companion `leader-execution-phasing.md`.

## 30. Risks & open questions (R-n, Q-n)
> TO-GENERATE: R-quota-enforcement (D-OPEN-quota-enforcement); R-retry (D-OPEN-retry); R-mpu-large-value (RC-xichen-large-value); R-snapshot-checkpoint-ordering; R-abandonment (mitigated by D-13). Plus Q-n from locking §10 (multipart lock placement, hsync/lease, SetAcl/SetTimes).

## 31. Definition of done
> TO-GENERATE: per-phase + overall (perf target hit; all I-n tested; legacy path removed; lint-spec green; TLA+ tiers green).

## 32. Operational runbook & rollback
> TO-GENERATE: per-command flag revert; detection metrics; mixed-mode operations.

# PART VII — Appendices

## 33. References
> #7583, #10502, #10503, #7406 (prototype), RATIS-1210, HDDS-1595 (seq-diagram exemplar), HDDS-8238 (MPU large-value), companion docs, prototype performance PDF.

## 34. Appendices
> Full proto listings; raw prototype performance data; the TLA+ model index (ObsAbstract/ObsImpl, FSO planned).
