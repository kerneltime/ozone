---
title: Multipart Upload Parts Table Split - Finalization Runbook
summary: Operator guide for finalizing the MPU_PARTS_TABLE_SPLIT layout feature (HDDS-10611), including the one-way-door warning, pre-checks, and verification.
date: 2026-06-02
jira: HDDS-10611
status: implemented
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

# Overview

Before this feature, the Ozone Manager stored every part of a multipart upload
(MPU) inline inside that upload's single `multipartInfoTable` row. Each
`CommitMultipartUploadPart` appended one part to that row and re-serialized the
**entire** row. For an upload of N parts this rewrites O(N^2) bytes over the
upload's life, which dominates write cost and RocksDB compaction for very large
MPUs.

`MPU_PARTS_TABLE_SPLIT` moves per-part records into a dedicated
`multipartPartsTable`, keyed by `(uploadId, partNumber)` (the `uploadId` is the
scan prefix that groups an upload's parts in part-number order). Each `CommitMultipartUploadPart`
now writes one small part row and leaves the upload's `multipartInfoTable` row
free of the inline part list, so the per-commit write cost is constant in the
number of parts.

This is an **OM layout feature** gated by Ozone's non-rolling upgrade
framework. The change is internal bookkeeping only: the S3 multipart API
(initiate, upload part, list parts, complete, abort) is unchanged, and already
completed keys are unaffected.

# What changes at finalization

Each MPU carries a `schemaVersion` that selects its storage layout:

- **schemaVersion 0** (pre-feature): parts stored inline in the
  `multipartInfoTable` row.
- **schemaVersion 1** (this feature): parts stored in the `multipartPartsTable`;
  the inline list stays empty.

The version is stamped on the upload **at initiate time**, from the OM's
finalized layout version, and is immutable for the life of that upload:

- **Pre-finalization**: every upload is initiated as schemaVersion 0. The OM
  refuses to produce schemaVersion 1 state, so a binary downgrade is always
  safe (all on-disk MPU state is the old format).
- **Post-finalization**: uploads *initiated after* finalization are
  schemaVersion 1 and use the split table. Uploads *initiated before*
  finalization remain schemaVersion 0 for their entire life, even if they are
  completed or aborted after finalization.

So after finalization the two layouts coexist, and complete / abort / list-parts
all handle both.

# Invariants

These hold by construction and are covered by unit and integration tests; they
are listed so operators know what behavior to expect and what would constitute a
defect.

- **I-A (stable version)**: an upload's `schemaVersion` is fixed at initiate and
  never changes. Finalizing the cluster does not migrate or rewrite existing
  uploads.
- **I-B (no stranded uploads)**: an upload initiated before finalization
  completes and aborts through the schemaVersion 0 (inline) path even if the
  cluster finalizes while it is in flight. Finalization never strands or breaks
  an in-progress upload.
- **I-C (API parity)**: initiate, upload-part, list-parts, complete, and abort
  behave identically for schemaVersion 0 and 1. S3 clients observe no
  difference, including ETags and part ordering. A part's ETag is optional at
  both versions: the S3 gateway always supplies one, but a part committed
  through the native client (or legacy pre-HDDS-9680 data) carries none, in
  which case list-parts falls back to the part name -- identically for
  schemaVersion 0 and 1.
- **I-D (accounting parity)**: bucket quota accounting and block garbage
  collection (deleted-table entries on overwrite, complete-with-discards, and
  abort) are byte-for-byte identical between the two layouts.

# ETags and non-S3 clients

The part ETag is an S3 concept, and only the S3 gateway computes its value (the
content MD5); the Ozone Manager and the generic Ozone client never derive it --
they store, validate, and concatenate whatever string they are handed. This
matters because multipart upload is also reachable through the **native Ozone
client**, which does not set an ETag at all. Both schemaVersion 0 and 1 tolerate
such eTag-less parts (this feature deliberately did not change that), so:

- **S3 uploads** get the canonical, content-derived multipart ETag:
  `md5(concat(per-part content MD5s)) + "-<partCount>"`.
- **Native (non-S3) uploads** complete successfully but their final-object ETag
  is **identifier-derived, not content-derived**:
  `md5(concat(part names)) + "-<partCount>"`. It is a stable, valid S3-shaped
  ETag but carries no content-integrity meaning, and it differs from the value
  an S3 upload of byte-identical content would produce.

Two pieces of code make the native path work and are easy to mistake for dead
defensiveness; both carry cross-reference comments so they are not removed:

1. `OmMultipartUploadCompleteList#getPartsList` mirrors the supplied per-part
   identifier into BOTH the proto `partName` and `eTag` fields, so a native
   (eTag-less) Complete still satisfies the OM's `allMatch(Part::hasETag)` gate.
2. `S3MultipartUploadCompleteRequest#eTagBasedValidator` accepts a part when the
   supplied value matches the stored eTag **or** the stored part name; native
   parts (no stored eTag) pass only via the part-name clause.

Making native-client multipart ETags content-derived (computing the MD5 in the
client output stream, the only layer below the gateway that sees the bytes) is a
tracked follow-up (`TODO(HDDS-14661 follow-up)`); it is out of scope here.

# The one-way door

**Finalization cannot be undone, and it removes the ability to downgrade the OM
binary.** Finalizing raises the on-disk OM metadata layout version (MLV) above
the maximum software layout version (SLV) supported by pre-feature binaries. An
OM started against on-disk state whose MLV exceeds its SLV will refuse to start.

Consequences:

- **Before** finalization the cluster is fully downgradable: stop the OMs and
  restart on the old binary. All MPU state is schemaVersion 0 and readable by
  the old version.
- **After** finalization there is no binary downgrade. The data is safe and the
  feature is transparent to clients, but you are committed to the new version.

Therefore: **finalize only after the new version has been validated in your
environment and you are prepared to stay on it.** There is no operational reason
to finalize early; pre-finalized clusters run with full old-version
functionality.

# Pre-finalization checklist

1. All OM instances are running the new binary and are healthy. In an OM HA
   deployment, all members of the quorum.
2. The cluster reports a pre-finalized state:

   ```
   ozone admin om finalizationstatus
   ```

   In OM HA, finalization status reflects the quorum, not individual OMs.
3. You have validated the new version (functional and, for large-MPU
   workloads, the write-cost improvement) and accept the one-way-door
   constraint above.

# Finalize

Follow the standard non-rolling upgrade procedure
([Non-Rolling Upgrades and Downgrades]({{< ref "feature/Nonrolling-Upgrade.md" >}}))
for the full sequence (prepare, restart with `--upgrade`, then finalize). The
OM finalize step is:

```
ozone admin om finalizeupgrade --service-id=<om-service-id>
```

Wait for the finalization status to report completion before validating.

# Post-finalization verification

- **New uploads use the split layout.** Initiate a fresh MPU, upload a part,
  and complete it; it should succeed and the object should read back correctly.
  Internally the part is now recorded in the `multipartPartsTable` and the
  upload's `multipartInfoTable` row no longer grows per part. For very large
  MPUs this is the visible win: per-part commit cost no longer scales with the
  number of parts already committed.
- **In-flight pre-finalization uploads still complete.** Any MPU that was
  initiated before finalization can still be listed, completed, or aborted
  normally (it stays on the inline path).
- **Diagnostics.** `ozone debug om container-key-mapping --in-progress` accounts
  for schemaVersion 1 parts (the split table) in addition to inline parts, so
  container-to-key reporting for in-progress uploads remains complete.

# Rollback and recovery

- **Pre-finalization**: downgrade is supported. Restart the OMs on the previous
  binary per the non-rolling downgrade procedure. All MPU state is the old
  inline format.
- **Post-finalization**: the OM binary cannot be downgraded (the one-way door).
  No layout rollback is available or required -- existing data, including
  in-progress uploads, remains valid and the feature is transparent to clients.

# Scope and known follow-ups

- Only in-progress MPU part bookkeeping changes. Completed keys, regular keys,
  and the S3 API are unaffected.
- Recon's multipart insight task accounts for split-table parts (HDDS-14666,
  landed in this change): its periodic *reprocess* scans the
  `multipartPartsTable` and includes schemaVersion 1 parts in the size and count
  totals. The *incremental* event path still defers a v1 upload's part sizes to
  the next reprocess, because a schemaVersion 1 `multipartInfoTable` event
  carries an empty inline part list. This timing gap matches Recon's
  eventual-consistency model and does not affect OM correctness, quota, or block
  cleanup.
