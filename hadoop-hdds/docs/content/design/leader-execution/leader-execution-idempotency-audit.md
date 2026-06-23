---
title: Leader-Side Execution — Per-Operation Idempotency Audit
summary: Classification of every OM write operation as idempotent or non-idempotent under client retry in the leader-side-execution (re-plan) model; sizes the retry-protection set that D-OPEN-retry requires
date: 2026-06-15
jira: HDDS-11898
status: draft (working — produced to close the D-OPEN-retry audit gap)
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

# Leader-Side Execution — Per-Operation Idempotency Audit

## 0. Why this document exists

The parent design defers the client retry/replay mechanism (`D-OPEN-retry`) **pending a per-operation
idempotency audit** that classifies every OM write op as idempotent or not under client retry. The
master notes that audit "has not been performed" and estimates "~10 non-idempotent ops, not 47" with
no in-tree inventory. **This document is that audit.** Its finding supersedes the "~10" estimate: the
client-facing non-idempotent set is **~26 operations**, in two tiers of severity.

All classifications are verified against master code (`file:line` cited). The *re-plan* reasoning on
top of those facts is design-level (provenance: verified facts → inferred conclusion, marked where it
matters).

## 1. The framing: idempotency under *re-plan*, not *replay*

The classification hinges on one model difference (this is the trap that flips several verdicts):

- **Classic OM HA — replay.** A retried write replays a *fixed* Ratis log entry (or the Ratis retry
  cache returns the prior response). Identities minted in `preExecute` are frozen into that entry, so a
  replay re-applies identical bytes — idempotent by construction. Dedup is 100% Ratis's in-memory
  retry cache keyed on the *user* `(clientId, callId)` (`OzoneManagerRatisServer.java:512-578`).
- **Leader-Side Execution — re-plan.** Only a leader-produced `PersistDb` patch goes through Ratis
  (different identity; one batch may carry many clients). A client retry that escapes dedup triggers a
  **fresh re-plan on the leader**: `preExecute` runs again and a *new* transaction index is minted from
  the OM-managed `AtomicLong`. The hazard is **leader re-execution**, not follower replay (followers
  apply fixed bytes; Ratis applies a committed entry once — both unchanged).

Two identity-generation mechanisms decide most verdicts (both verified):

- **`UniqueId.next()`** (`UniqueId.java:59-65`) = `getTime() << 16 | offset++` — **wall-clock +
  mutable counter → NON-deterministic.** A re-plan returns a *different* value. Source of `clientID`
  (CreateKey/CreateFile) and part of the MPU `uploadID`.
- **`getObjectIdFromTxId(txId)`** (`OmUtils.java:766-770`) = `(epoch<<N)|txId` — **deterministic in the
  transaction index.** Churns under re-plan *only* if the re-plan is assigned a different index — which
  it is, since the index is minted fresh at plan time. (`objectID`/`updateID` are therefore *also*
  re-plan-non-deterministic in LSE, but only because the index advances, not because of `UniqueId`.)
- Several namespace ops mint a **random secret/token** in `preExecute` (`OmUtils.getSHADigest()` =
  `SRAND.nextBytes(...)` then SHA-256, `OmUtils.java:474-483`; delegation-token sequence#). These
  re-mint on re-plan — the GetS3Secret / GetDelegationToken / AssignUserAccessId trap.

## 2. Taxonomy

An op is **non-idempotent under client retry** iff a second independent re-plan changes the final DB
state **or** the client-visible result vs a single execution. Tags (an op may carry several):

| Tag | Cause | Re-plan effect |
|---|---|---|
| **N1** | mints identity from a fresh source (`UniqueId.next()`, random secret/token, the advancing managed index) | distinct object / id-or-secret churn |
| **N2** | allocates an external/irreversible resource (SCM blocks, RocksDB checkpoint dir) | leak / double-allocate |
| **N3** | relative/commutative mutation (quota `usedBytes`/`usedNamespace` `+=Δ`, ref counts) | double-apply |
| **N4** | consumes a one-shot precondition (open→committed, MPU session) or moves/soft-deletes state | spurious error (`KEY_NOT_FOUND`/`ALREADY_EXISTS`) or dangling/duplicate state |
| **I** | absolute whole-object Put/Delete fully determined by request args (incl. set-union/set-difference ACLs) | identical final state and result |

## 3. Classification (master code; `Type` ids from `OmClientProtocol.proto`)

### 3.1 Data-path ops

| Op | Type | Class (`om/request/…`) | Tags | Evidence + retry hazard |
|---|---|---|---|---|
| CreateKey | 31 | `key/OMKeyCreateRequest` | N1,N2 | clientID `UniqueId.next()` `OMKeyCreateRequest.java:204`; SCM `allocateBlock` `:165`; objectID `:306`. Re-plan churns clientID + leaks blocks. **AMBIGUOUS** (handle reuse). |
| CreateFile | 72 | `file/OMFileCreateRequest` | N1,N2,N3 | clientID `:157`; `allocateBlock` `:133-141`; objectID `:250`; `incrUsedNamespace` `:276`. Same as CreateKey. **AMBIGUOUS**. |
| AllocateBlock | 37 | `key/OMAllocateBlockRequest` | N2,N3 | fresh SCM block, `appendNewBlocks`. Re-plan double-allocates (bounded; surplus GC'd at commit/expiry). |
| CommitKey | 36 | `key/OMKeyCommitRequest` | N3,N4 | reads/tombstones open key `:257,:391`; `incrUsedBytes` `:410`. Re-plan → `KEY_NOT_FOUND` + quota double-count. |
| DeleteKey | 34 | `key/OMKeyDeleteRequest` | N4,N3 | `KEY_NOT_FOUND` `:146`; `decrUsedBytes/Namespace` `:166-167`. Re-plan → spurious error. |
| DeleteKeys | 38 | `key/OMKeysDeleteRequest` | N4,N3,batch | partial `unDeletedKeys` `:297-299`. Re-plan deletes a *different subset*. **AMBIGUOUS** (batch atomicity). |
| RenameKey | 33 | `key/OMKeyRenameRequest` | N4 | source consumed → `KEY_NOT_FOUND` `:176`; objectID preserved (not minted). |
| RenameKeys | 39 | `key/OMKeysRenameRequest` | N4,batch | partial `unRenamedKeys` `:219-225`. **AMBIGUOUS** (batch atomicity). |
| InitiateMPU | 45 | `s3/multipart/S3InitiateMultipartUploadRequest` | N1,N3 | uploadID = `UUIDv7 + UniqueId.next()` `OMMultipartUploadUtils.java:43`, no dedup on (vol,bkt,key). Re-plan → **second MPU session**. |
| CommitMPUPart | 46 | `…/S3MultipartUploadCommitPartRequest` | N3,N4 | open part key consumed `:151-162`; `incrUsedBytes` `:257`. |
| CompleteMPU | 47 | `…/S3MultipartUploadCompleteRequest` | N3,N4 | session consumed → `NO_SUCH_MULTIPART_UPLOAD` `:266-269`; quota diffs. |
| AbortMPU | 48 | `…/S3MultipartUploadAbortRequest` | N3,N4 | session consumed `:162-165`; `incrUsedBytes(-released)` `:174-180`. |
| **CreateDirectory** | 71 | `file/OMDirectoryCreateRequest` | (self-heal) | returns `DIRECTORY_ALREADY_EXISTS` + skips quota `:202-206`. **Idempotent** under re-plan¹. |
| **RecoverLease** | 119 | `file/OMRecoverLeaseRequest` | (self-heal) | `LEASE_RECOVERY` flag skip-write `:222-238`. **Idempotent**¹. |
| **DeleteOpenKeys** | 40 | `key/OMOpenKeysDeleteRequest` | (self-heal) | skips absent/stale `:189,:191-195`; no quota. **Idempotent**. |
| **PurgeKeys** | 81 | `key/OMKeyPurgeRequest` | (self-heal) | absolute deletes + absolute `purgeSnapshot*` quota `:202-203`. **Idempotent** (snapshot-chain-guarded). |

*Background/service ops (not client retry, listed for completeness):* PurgeDirectories (95, N3 relative quota), SnapshotMoveDeletedKeys (116, N4), SnapshotMoveTableKeys (137, N4), AbortExpiredMPU (127, N3 relative quota).

### 3.2 Namespace / admin ops

| Op | Type | Class | Tags | Evidence + retry hazard |
|---|---|---|---|---|
| CreateVolume | — | `volume/OMVolumeCreateRequest` | N1 | objectID `:138`; guarded `VOLUME_ALREADY_EXISTS`. Re-plan: spurious error + id churn. |
| SetVolumeProperty/quota | — | `volume/OMVolumeSetQuotaRequest` | I | absolute setters `:143-160`. |
| SetVolumeProperty/owner | — | `volume/OMVolumeSetOwnerRequest` | N4 | relative owner-list move; `delVolumeFromOwnerList` → `USER_NOT_FOUND` on re-plan `OMVolumeRequest.java:63-64`. |
| DeleteVolume | — | `volume/OMVolumeDeleteRequest` | N4 | `VOLUME_NOT_FOUND` on re-plan. |
| CreateBucket | — | `bucket/OMBucketCreateRequest` | N1,N3 | objectID `:260-262`; **volume `incrUsedNamespace(1)` `:272-274`** (precondition-gated). |
| SetBucketProperty (quota/owner) | — | `bucket/OMBucketSetProperty/SetOwnerRequest` | I | absolute. |
| DeleteBucket | — | `bucket/OMBucketDeleteRequest` | N3,N4 | **volume `incrUsedNamespace(-1)` `:182-184`** (precondition-gated); `BUCKET_NOT_FOUND` on re-plan. |
| AddAcl / RemoveAcl | — | `*/acl/*AddAcl/RemoveAcl*` | **I** | **set-union / set-difference, verified** (`OzoneAclUtil.java:222-279`: bitwise OR / AND-NOT, no-error if absent). |
| SetAcl | — | `*/acl/*SetAcl*` | I | full replace (`AclListBuilder.java:94-100`). |
| CreateSnapshot | 112 | `snapshot/OMSnapshotCreateRequest` | N1,N2 | `preExecute` **unconditionally** mints `setSnapshotId(UUID.randomUUID())` `:127`; checkpoint dir keyed by snapshotId → **lost-window re-plan leaks an orphan checkpoint** (N2, not overwrite-healable). |
| DeleteSnapshot | 115 | `snapshot/OMSnapshotDeleteRequest` | N4 | soft-delete → `FILE_NOT_FOUND` "already deleted" `:169-171`. |
| RenameSnapshot | 131 | `snapshot/OMSnapshotRenameRequest` | N4 | move → `FILE_NOT_FOUND` on re-plan. |
| SnapshotPurge | 118 | `snapshot/OMSnapshotPurgeRequest` | I | per-key no-op if already purged `:104-111` (service-driven). |
| SetSnapshotProperty | 128 | `snapshot/OMSnapshotSetPropertyRequest` | I | absolute setters `:78-104`. |
| CreateTenant | 96 | `s3/tenant/OMTenantCreateRequest` | N1,N4 | volume objectID `:292` + `incRefCount` `:294`; `TENANT_ALREADY_EXISTS` on re-plan. |
| DeleteTenant | 97 | `s3/tenant/OMTenantDeleteRequest` | N4 | `TENANT_NOT_FOUND` on re-plan. |
| TenantAssignUserAccessId | 100 | `s3/tenant/OMTenantAssignUserAccessIdRequest` | N1,N4 | **random secret minted `preExecute:175`**; lost-window re-plan **rotates the client's secret**; `…_ALREADY_EXISTS` on success-retry. |
| TenantRevokeUserAccessId | 101 | `s3/tenant/OMTenantRevokeUserAccessIdRequest` | N4 | `ACCESS_ID_NOT_FOUND` on re-plan. |
| TenantAssignAdmin / RevokeAdmin | 102/103 | `s3/tenant/OMTenantAssign/RevokeAdminRequest` | I | absolute `setIsAdmin(...)` set. |
| **GetDelegationToken** | 61 | `security/OMGetDelegationTokenRequest` | **N1 (corrected)** | `preExecute` mints a fresh token; identifier (with seq#) is the **table key** `:179-181` → re-plan inserts a **second token** (distinct key), not an overwrite. *Naive "replay" analysis wrongly calls this idempotent.* |
| RenewDelegationToken | 62 | `security/OMRenewDelegationTokenRequest` | I (≈) | stable key, absolute new expiry `:160-167`. |
| CancelDelegationToken | 63 | `security/OMCancelDelegationTokenRequest` | I | remove-if-present `:105-112`. |
| GetS3Secret | 49 | `s3/security/S3GetSecretRequest` | **I / N1 (windowed)** | store-if-absent `:151-178`; idempotent once present, but a **lost-first-write re-plan persists a different random secret**. |
| SetS3Secret | 106 | `s3/security/OMSetSecretRequest` | I | absolute set of caller-supplied secret. |
| RevokeS3Secret | 93 | `s3/security/S3RevokeSecretRequest` | I | delete-if-exists. |
| FinalizeUpgrade / Prepare / CancelPrepare | 54/56/58 | `upgrade/OM*Request` | I | absolute version/marker. |
| QuotaRepair | 133 | `volume/OMQuotaRepairRequest` | **N3** | **`incrUsedBytes/Namespace(diff)` `:125-126`, NO precondition guard** → re-plan **double-counts quota**. The only *unguarded* additive delta. |
| SetSafeMode | 124 | (no request class; `OzoneManager.java:5160`) | I | read-only; not a replicated write (`OmUtils.java:256`). |

¹ Self-heal carries a **durability-ordering caveat**: the internal guard protects against re-fire only if the first execution's mutation is durable *before the client is acked*. If LSE acks before commit, even these can re-fire (harmlessly for deletes; a redundant write for RecoverLease/CreateDirectory).

## 4. The non-idempotent set, in two tiers

**Tier A — durable `(clientId,callId)→response` entry STRICTLY REQUIRED** (no overwrite/absolute-set
self-heals; re-plan corrupts state or hands the client a wrong/rotated value):
CommitKey, AllocateBlock, DeleteKey, RenameKey, InitiateMPU, CommitMPUPart, CompleteMPU, AbortMPU,
**QuotaRepair** (unguarded double-count), **CreateSnapshot** (orphan checkpoint), **GetDelegationToken**
(distinct-key token churn), **TenantAssignUserAccessId** + **GetS3Secret** (lost-window secret rotation).

**Tier B — durable entry needed to suppress a SPURIOUS ERROR + id churn** (the state self-heals via an
existence precondition, but a success-then-retry returns `ALREADY_EXISTS`/`NOT_FOUND` and any returned
objectID differs from a lost first attempt):
CreateVolume, CreateBucket, CreateTenant, DeleteVolume, DeleteBucket, DeleteTenant,
SetVolumeProperty/owner, DeleteSnapshot, RenameSnapshot, TenantRevokeUserAccessId.

**RESOLVED → Tier A (the design choices are now made):**
- **CreateKey / CreateFile / InitiateMPU** — `R-5` (retry doc) rejected the deterministic-id lever as
  collision-unsafe, so the `UniqueId.next()` clientID/uploadID churn (and block leak) stands → these need
  the durable entry (**Tier A**). With R-4 uniform caching the entry is written regardless; the point is
  that they are *non-idempotent* and the entry is *load-bearing*, not merely spurious-error suppression.
- **DeleteKeys / RenameKeys** — batch atomicity is fixed as **one `(clientId,callId)` retry unit**: the
  whole batch is one client request → one completion record capturing the (possibly partial,
  `PARTIAL_DELETE`/`PARTIAL_RENAME`) response, replayed verbatim on retry. The subset-divergence
  disappears and a re-planned `Merge` cannot double-count (**Tier A**). (Not best-effort per-entry — a
  per-element cache was the rejected alternative.)

**Idempotent (no retry entry needed):** CreateDirectory, RecoverLease, DeleteOpenKeys, PurgeKeys,
SetVolumeProperty/quota, SetBucketProperty (quota+owner), Add/Remove/SetAcl (all object types),
SnapshotPurge, SetSnapshotProperty, TenantAssign/RevokeAdmin, Renew/CancelDelegationToken,
Set/RevokeS3Secret, FinalizeUpgrade, Prepare, CancelPrepare, SetSafeMode, SetRangerServiceVersion.

## 5. Two design levers (input to the D-OPEN-retry mechanism)

1. **Make `clientID` and `uploadID` deterministic functions of `(callerClientId, callId)`** instead of
   `UniqueId.next()`. The *only* N1 churn on the data path is `UniqueId.next()` — objectID/updateID are
   already deterministic in the index. This lever removes N1 from CreateKey/CreateFile/InitiateMPU, so
   the retry table is needed there only for the N2 block-leak and the N3/N4 cases. (It does **not** fix
   N3/N4 — a relative quota merge double-applies regardless of id determinism.)
2. **A durable, replicated `(clientId,callId)→response` table, written atomically with the data batch.**
   This is the general fix: it replays the first response and *suppresses re-execution* for every Tier-A
   and Tier-B op. Crucially it must be **durable + replicated + atomic-with-the-batch** — because today's
   Ratis retry cache is in-memory and lost on failover (it survives only because the cross-failover-retried
   ops happen to be idempotent-by-outcome). Making it durable is **strictly better than today's behavior**,
   not merely a replacement.

**Conclusion for the mechanism:** deterministic ids alone are insufficient (they don't cover N3/N4); a
durable replicated retry entry is necessary for the Tier-A/Tier-B set; a leader-local in-flight registry
is additionally needed to serialize *concurrent* retries before the first commit lands. The protection
set is ~26 client-facing ops (not ~10) — the create-path subset stays Tier A (R-5 rejected the deterministic-id lever).

## 6. Flagged judgment calls (for design review)

- **GetDelegationToken / GetS3Secret / TenantAssignUserAccessId** — the re-plan-vs-replay correction
  (§1). These are *not* idempotent in LSE; confirm and decide whether the durable entry covers them or
  whether secret/token minting moves out of the re-planned path.
- **CreateSnapshot** — the snapshotId is minted in `preExecute` and names an on-disk checkpoint dir; a
  lost-window re-plan strands a checkpoint. Decide: deterministic snapshotId, or retry-entry coverage.
- **Batch ops (DeleteKeys/RenameKeys)** — RESOLVED: **one `(clientId,callId)` retry unit**; the completion
  record captures the partial-success response (`PARTIAL_DELETE`/`PARTIAL_RENAME`), replayed verbatim (Tier A). Not best-effort per-entry.
- **QuotaRepair** — the one unguarded additive delta; whether it becomes the *exact-quota mitigation*
  path (per `D-OPEN-quota-enforcement`) interacts with its retry handling.
- **Durability ordering** — the self-healing ops (§3.1 footnote) are only safe if the first mutation is
  durable before the client ack. Confirm the LSE ack point is post-commit.

## 7. Provenance

Every op's *behavior* (mints id / allocates / applies delta / consumes precondition) is **verified**
against master code at the cited `file:line`. The *idempotency conclusion under re-plan* is design-level
reasoning on those verified facts (the leader re-executes on retry; the index advances). This audit
closes the "per-op idempotency audit not yet performed" gap behind `D-OPEN-retry` and supersedes the
master's "~10 non-idempotent ops" estimate with the ~26-op, two-tier finding above.
