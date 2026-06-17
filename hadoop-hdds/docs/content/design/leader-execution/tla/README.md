---
title: Leader-Side Execution — TLA+ Formal Models
summary: TLC-checked refinement models for the OBS/FSO locking and quota design (HDDS-11898), with reproduction instructions and captured verdicts
date: 2026-06-16
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

# Leader-Side Execution — TLA+ Formal Models

These models formally validate the **concurrency-control and quota design** for the
HDDS-11898 leader-side-execution work. They are the formal tier behind the prose specs in
the parent directory — `leader-execution-locking.md` (the lock model) and
`leader-execution-retry.md` (retry/idempotency). They validate the **design/algorithm**, not
the Java code (see [Scope and limitations](#scope-and-limitations)).

## What is modeled

Two independent subsystems, each as an **abstract oracle** + a **faithful implementation**
that is proven to **refine** it (refinement here = linearizability: every implementation
behavior maps to an atomic per-operation behavior of the oracle).

| File | Role |
|------|------|
| `ObsAbstract.tla` | OBS (flat namespace) oracle — atomic create/commit/delete + **soft** quota (the accepted over-commit limitation, EXC-3). |
| `ObsImpl.tla` | OBS implementation — striped lock manager (fixed array, sorted dedup-to-strongest acquisition, RW-per-stripe, held across commit), `CommitPlan`/`CommitApply` split for the commutative quota merge. Refines `ObsAbstract`. |
| `ObsAbstractExact.tla` | An **exact**-quota oracle, used only to *reproduce* the over-commit counterexample under shared-bucket locks (the negative test that motivates EXC-3). |
| `FsoAbstract.tla` | FSO (hierarchical namespace) oracle — atomic tree operations over objectID-keyed nodes. |
| `FsoImpl.tla` | FSO implementation — container locks keyed by objectID, slot locks by `(parentObjectID, name)`, objectID-total-order acquisition (deadlock freedom), recursive delete as tombstone + per-node-locked async purge. Refines `FsoAbstract`. |

Invariants checked (defined inside the specs): `Refinement` (linearizability), `LockInv`
(mutual exclusion / lock discipline), `NoLeak` (no permit/lock leak), `UsedConsistent` (OBS
quota accounting), and for recursive delete `Accounted` (no *permanent* unaccounted orphan —
the EXC-2-weakened form of strict no-orphan, since transient orphans exist mid-purge by
design).

## How to run

TLC is driven by `tla2tools.jar`, which is **not committed** (it is a ~2 MB binary; download
it from the [TLA+ tools releases](https://github.com/tlaplus/tlaplus/releases) into this
directory). Then, on a JDK 21 toolchain:

```bash
# Spec whose config file shares its name (e.g. ObsImpl.cfg for ObsImpl.tla):
./run.sh ObsImpl

# Alternate configs for the same spec — invoke TLC directly with -config:
JH="$(/usr/libexec/java_home -v 21)"
"$JH/bin/java" -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
  -config ObsImpl3.cfg -workers auto ObsImpl.tla
```

`run.sh` auto-runs the PlusCal translator first if the spec contains an `--algorithm` block,
then runs TLC with all cores. The OBS/FSO impl specs are large; the 3-proc and FSO runs take
tens of minutes to hours and can be memory/disk-hungry (see the M3 note below).

## Configurations and captured verdicts

Verdicts below are quoted **verbatim** from TLC runs captured during development on
2026-06-15 (the raw logs are reproducible via the commands above and are intentionally not
committed). State counts are exact.

| Config | Spec | Scope | Verdict |
|--------|------|-------|---------|
| `ObsImpl.cfg` | `ObsImpl` | 2-proc lifecycle + quota | GREEN (smaller bound; refines + `LockInv` + `NoLeak` + `UsedConsistent`) |
| `ObsImpl3.cfg` | `ObsImpl` | **3-proc** lifecycle + quota | **GREEN** — `60,976,791 distinct states`, depth 55 |
| `ObsImplCollide.cfg` | `ObsImpl` | `STRIPES=1` forced stripe collision | GREEN (collision-safe: dedup-to-strongest holds) |
| `ObsImplBug.cfg` | `ObsImpl` | `SORTED=FALSE` (argument-order acquisition) | **DEADLOCK found (expected)** — negative test proving the total-order acquisition is load-bearing |
| `QuotaOvercommit.cfg` | `ObsAbstractExact` | exact-quota precondition under shared-bucket lock | **OVER-COMMIT found (expected)** — negative test; motivates the soft-quota decision (EXC-3) |
| `FsoImpl.cfg` / `FsoImplSmall.cfg` | `FsoImpl` | M2a: create/commit/delete + file rename | **GREEN** — `26,828,240 distinct states`, depth 41 |
| `FsoImpl.cfg` (dir-rename mode) | `FsoImpl` | M2b: directory rename (rename-stability F-2) | **GREEN** — `32,400,283 distinct states`, depth 41 |
| `FsoM3.cfg` | `FsoImpl` | M3: recursive `rm -rf`, tight bound `MAX_OPS=1` | **GREEN (tight bound)** — `341,610 distinct states`, depth 23, "No error has been found" (captured 2026-06-17) |
| `FsoM3Full.cfg` | `FsoImpl` | M3 broad, `MAX_OPS=2` | **ABORTED — no verdict.** Ran out of disk at ~220M distinct states with ~14M still queued (`Error: ... No space left on device`) |

The two OBS negative tests and the two FSO green tiers (M2a/M2b) are the load-bearing
results: linearizability + deadlock-freedom + quota accounting for the non-recursive
operations are established by exhaustive refinement, and the negative tests prove the model
has teeth (it *can* find the deadlock and the over-commit).

### M3 (recursive delete): tight bound green, broad bound still exceeds disk

Be precise about what is and isn't established here, because the two M3 bounds land
differently. The **tight** M3 run (`FsoM3.cfg`, `MAX_OPS=1`) checks the `Accounted` invariant
(no *permanent* unaccounted orphan — the EXC-2-weakened form) and now **passes with a captured
verdict**: `341,610 distinct states`, search depth 23, "No error has been found" (captured
2026-06-17). The **broad** M3 run (`FsoM3Full.cfg`, `MAX_OPS=2`) still **aborts on disk
exhaustion** before completing and so produces **no verdict**. So recursive-delete
orphan-freedom is `verified` at the formal tier *within the tight bound* (`MAX_OPS=1`), and
remains `inferred` beyond it (anchored by the lock/linearizability semantics and the M2a/M2b
refinement) until the broad run can complete on larger disk.

The `Accounted` invariant is also the one wired into `FsoImpl.cfg` / `FsoImplSmall.cfg`: those
configs previously named an older `NoOrphan` invariant and so were not runnable as cited;
they now use `INVARIANT Accounted` (reconciled), so the M2a, M2b, and M3-tight verdicts all
reproduce from the committed cfgs via the commands above.

## Scope and limitations

These models validate the **design**, not the **code**. Three gaps, stated so they are not
overclaimed:

1. **Model faithfulness** — the spec is a hand model of the algorithm; mitigated by the
   negative tests and review, not eliminated.
2. **Code conformance** (the big one) — nothing here proves the Java matches the model. The
   intended bridge is Lincheck-style linearizability tests on the real Java against the *same*
   oracle (test-plan §7), plus the invariant set as a review contract.
3. **Abstraction** — Ratis is modeled as atomic commit; RocksDB as a set, not bytes; bounds
   are finite (proc counts, op counts, tree shape); the op set is a subset. TLC is exhaustive
   *within* the bound, not unbounded (this is TLC model-checking, not a TLAPS proof).

## Notes for an Apache contribution

- `tla2tools.jar` is deliberately uncommitted (binary; download as above).
- At PR time the `.tla` / `.cfg` / `.sh` files will need either Apache license headers (TLA+
  comment syntax `\* ...`, shell `#`) or a RAT (release-audit) exclusion entry — they carry
  none today. This is a packaging follow-up, not a design gap.
- Cross-references: parent `../leader-execution-locking.md` (lock model, invariants I-1..I-13),
  `../leader-execution-retry.md` (retry/WAL terminal write path), `../leader-planned-execution.md`
  (master design; §31.2 DoD requires the FSO model green against the linearizability oracle).
