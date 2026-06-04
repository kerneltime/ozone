/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteList;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests S3 Multipart Upload Complete request.
 */
public class TestS3MultipartUploadCompleteRequest
    extends TestS3MultipartRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    doPreExecuteCompleteMPU(volumeName, bucketName, keyName,
        UUID.randomUUID().toString(), new ArrayList<>());
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put("custom-key1", "custom-value1");
    customMetadata.put("custom-key2", "custom-value2");

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");


    String uploadId = checkValidateAndUpdateCacheSuccess(
        volumeName, bucketName, keyName, customMetadata, tags);
    checkDeleteTableCount(volumeName, bucketName, keyName, 0, uploadId);

    customMetadata.remove("custom-key1");
    customMetadata.remove("custom-key2");
    customMetadata.put("custom-key3", "custom-value3");

    tags.remove("tag-key1");
    tags.remove("tag-key2");
    tags.put("tag-key3", "tag-value3");

    // Do it twice to test overwrite
    uploadId = checkValidateAndUpdateCacheSuccess(volumeName, bucketName,
        keyName, customMetadata, tags);
    // After overwrite, one entry must be in delete table
    checkDeleteTableCount(volumeName, bucketName, keyName, 1, uploadId);
  }

  public void checkDeleteTableCount(String volumeName,
      String bucketName, String keyName, int count, String uploadId)
      throws Exception {
    String dbOzoneKey = getMultipartKey(volumeName, bucketName, keyName,
        uploadId);
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
        = omMetadataManager.getDeletedTable().getRangeKVs(
        null, 100, dbOzoneKey);

    // deleted key entries count is expected to be 0
    if (count == 0) {
      assertEquals(0, rangeKVs.size());
      return;
    }

    assertThat(rangeKVs.size()).isGreaterThanOrEqualTo(1);

    // Count must consider unused parts on commit
    assertEquals(count,
        rangeKVs.get(0).getValue().getOmKeyInfoList().size());
  }

  @Test
  public void testValidateAndUpdateCacheV1CompletesFromPartsTable()
      throws Exception {
    // Post-finalization: parts live in the split table; Complete must assemble
    // the final key from them (with the same ETag hash as the inline path) and
    // delete the part rows.
    when(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .thenReturn(OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    OMRequest initiateMPURequest =
        doPreExecuteInitiateMPU(volumeName, bucketName, keyName);
    String multipartUploadID =
        getS3InitiateMultipartUploadReq(initiateMPURequest)
            .validateAndUpdateCache(ozoneManager, 1L).getOMResponse()
            .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    long clientID = Time.now();
    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);
    S3MultipartUploadCommitPartRequest commitReq =
        getS3MultipartUploadCommitReq(commitMultipartRequest);
    addKeyToTable(volumeName, bucketName, keyName, clientID);
    commitReq.validateAndUpdateCache(ozoneManager, 2L);

    // The part is stored in the split table, not inline.
    assertNotNull(omMetadataManager.getMultipartPartsTable()
        .get(OmMultipartPartKey.of(multipartUploadID, 1)));

    String eTag = commitReq.getOmRequest().getCommitMultiPartUploadRequest()
        .getKeyArgs().getMetadataList().stream()
        .filter(kv -> kv.getKey().equals(OzoneConsts.ETAG))
        .findFirst().get().getValue();
    List<Part> partList = new ArrayList<>();
    partList.add(Part.newBuilder().setETag(eTag).setPartName(eTag)
        .setPartNumber(1).build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);
    S3MultipartUploadCompleteRequest completeReq =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);
    OMClientResponse omClientResponse =
        completeReq.validateAndUpdateCache(ozoneManager, 3L);

    BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation();
    omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        multipartUploadID);
    // Info row gone; final key assembled with the v0-equivalent ETag hash.
    assertNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));
    OmKeyInfo finalKey = omMetadataManager
        .getKeyTable(completeReq.getBucketLayout())
        .get(getOzoneDBKey(volumeName, bucketName, keyName));
    assertNotNull(finalKey);
    assertEquals(DigestUtils.md5Hex(eTag) + "-1",
        finalKey.getMetadata().get(OzoneConsts.ETAG));
    // The split-table part row was deleted on completion.
    assertNull(omMetadataManager.getMultipartPartsTable()
        .get(OmMultipartPartKey.of(multipartUploadID, 1)));
  }

  @Test
  public void testValidateAndUpdateCacheV1NativeETaglessComplete()
      throws Exception {
    // Native (non-S3) multipart upload, schemaVersion 1, end to end. The native
    // Ozone client commits parts with NO eTag (only the S3 gateway computes one)
    // and at Complete supplies each part NAME, which OmMultipartUploadCompleteList
    // mirrors into both the partName and eTag proto fields. This exercises the
    // load-bearing eTag-less contract: the OM must (a) accept the eTag-less
    // commit, (b) validate each part via the "eTag equals the stored part name"
    // fallback in eTagBasedValidator, and (c) derive the final-object ETag from
    // the part NAMES -- md5(concat(partNames))-N -- not from content MD5s.
    when(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .thenReturn(OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    OMRequest initiateMPURequest =
        doPreExecuteInitiateMPU(volumeName, bucketName, keyName);
    String multipartUploadID =
        getS3InitiateMultipartUploadReq(initiateMPURequest)
            .validateAndUpdateCache(ozoneManager, 1L).getOMResponse()
            .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    // Commit one part as the NATIVE client does -- with NO eTag. Build the
    // normal commit request and strip the ETag metadata that the S3 gateway
    // would have set, then preExecute. The schemaVersion 1 commit guard requires
    // only block locations (supplied by the open key), so the eTag-less part
    // still commits (HDDS-14661 relaxation).
    long clientID = Time.now();
    OMRequest rawCommit = OMRequestTestUtils.createCommitPartMPURequest(
        volumeName, bucketName, keyName, clientID, 0L, multipartUploadID, 1,
        Collections.emptyList());
    MultipartCommitUploadPartRequest commitPart =
        rawCommit.getCommitMultiPartUploadRequest();
    OMRequest eTaglessCommit = rawCommit.toBuilder()
        .setCommitMultiPartUploadRequest(commitPart.toBuilder().setKeyArgs(
            commitPart.getKeyArgs().toBuilder().clearMetadata().build()))
        .build();
    OMRequest commitMultipartRequest =
        getS3MultipartUploadCommitReq(eTaglessCommit).preExecute(ozoneManager);
    S3MultipartUploadCommitPartRequest commitReq =
        getS3MultipartUploadCommitReq(commitMultipartRequest);
    addKeyToTable(volumeName, bucketName, keyName, clientID);
    OMClientResponse commitResp =
        commitReq.validateAndUpdateCache(ozoneManager, 2L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        commitResp.getOMResponse().getStatus());

    // The committed split-table part carries no eTag.
    OmMultipartPartInfo storedPart = omMetadataManager.getMultipartPartsTable()
        .get(OmMultipartPartKey.of(multipartUploadID, 1));
    assertNotNull(storedPart);
    assertNull(storedPart.getETag());

    // Complete as the native client does: supply the part NAME, which
    // OmMultipartUploadCompleteList mirrors into both partName and eTag.
    String partName = storedPart.getPartName();
    Map<Integer, String> nativePartsMap = new LinkedHashMap<>();
    nativePartsMap.put(1, partName);
    List<Part> partList =
        new OmMultipartUploadCompleteList(nativePartsMap).getPartsList();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);
    S3MultipartUploadCompleteRequest completeReq =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);
    OMClientResponse omClientResponse =
        completeReq.validateAndUpdateCache(ozoneManager, 3L);
    BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation();
    omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Complete succeeds for the eTag-less native upload (validated via the
    // eTag-equals-stored-part-name fallback)...
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    // ...and the final-object ETag is identifier-derived from the part name,
    // not content-derived.
    OmKeyInfo finalKey = omMetadataManager
        .getKeyTable(completeReq.getBucketLayout())
        .get(getOzoneDBKey(volumeName, bucketName, keyName));
    assertNotNull(finalKey);
    assertEquals(DigestUtils.md5Hex(partName) + "-1",
        finalKey.getMetadata().get(OzoneConsts.ETAG));
  }

  /** Commits one part of a schemaVersion 1 upload and returns its eTag. */
  private String commitV1Part(String volumeName, String bucketName,
      String keyName, String multipartUploadID, int partNumber, long clientID,
      long trxnLogIndex) throws Exception {
    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, partNumber);
    S3MultipartUploadCommitPartRequest commitReq =
        getS3MultipartUploadCommitReq(commitMultipartRequest);
    addKeyToTable(volumeName, bucketName, keyName, clientID);
    commitReq.validateAndUpdateCache(ozoneManager, trxnLogIndex);
    return commitReq.getOmRequest().getCommitMultiPartUploadRequest()
        .getKeyArgs().getMetadataList().stream()
        .filter(kv -> kv.getKey().equals(OzoneConsts.ETAG))
        .findFirst().get().getValue();
  }

  @Test
  public void testValidateAndUpdateCacheV1MultiPartComplete() throws Exception {
    // Three parts committed to the split table, completed in full: the final
    // key's ETag hash must concatenate the part eTags in ascending part-number
    // order (the v0 formula) and every part row must be deleted.
    when(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .thenReturn(OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    OMRequest initiateMPURequest =
        doPreExecuteInitiateMPU(volumeName, bucketName, keyName);
    String multipartUploadID =
        getS3InitiateMultipartUploadReq(initiateMPURequest)
            .validateAndUpdateCache(ozoneManager, 1L).getOMResponse()
            .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    String eTag1 = commitV1Part(volumeName, bucketName, keyName,
        multipartUploadID, 1, Time.now(), 2L);
    String eTag2 = commitV1Part(volumeName, bucketName, keyName,
        multipartUploadID, 2, Time.now() + 1, 3L);
    String eTag3 = commitV1Part(volumeName, bucketName, keyName,
        multipartUploadID, 3, Time.now() + 2, 4L);

    List<Part> partList = new ArrayList<>();
    partList.add(Part.newBuilder().setETag(eTag1).setPartName(eTag1)
        .setPartNumber(1).build());
    partList.add(Part.newBuilder().setETag(eTag2).setPartName(eTag2)
        .setPartNumber(2).build());
    partList.add(Part.newBuilder().setETag(eTag3).setPartName(eTag3)
        .setPartNumber(3).build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);
    S3MultipartUploadCompleteRequest completeReq =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);
    OMClientResponse omClientResponse =
        completeReq.validateAndUpdateCache(ozoneManager, 5L);
    BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation();
    omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    OmKeyInfo finalKey = omMetadataManager
        .getKeyTable(completeReq.getBucketLayout())
        .get(getOzoneDBKey(volumeName, bucketName, keyName));
    assertNotNull(finalKey);
    assertEquals(DigestUtils.md5Hex(eTag1 + eTag2 + eTag3) + "-3",
        finalKey.getMetadata().get(OzoneConsts.ETAG));

    // Every part row is reclaimed.
    for (int partNumber = 1; partNumber <= 3; partNumber++) {
      assertNull(omMetadataManager.getMultipartPartsTable()
          .get(OmMultipartPartKey.of(multipartUploadID, partNumber)));
    }
  }

  @Test
  public void testValidateAndUpdateCacheV1DiscardedPartReclaimed()
      throws Exception {
    // Parts 1, 2, 3 committed; Complete lists only [1, 3]. The discarded part 2
    // must have its row deleted AND its key moved to the deleted table, and the
    // listed parts' rows must also be deleted.
    when(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .thenReturn(OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    OMRequest initiateMPURequest =
        doPreExecuteInitiateMPU(volumeName, bucketName, keyName);
    String multipartUploadID =
        getS3InitiateMultipartUploadReq(initiateMPURequest)
            .validateAndUpdateCache(ozoneManager, 1L).getOMResponse()
            .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    String eTag1 = commitV1Part(volumeName, bucketName, keyName,
        multipartUploadID, 1, Time.now(), 2L);
    commitV1Part(volumeName, bucketName, keyName,
        multipartUploadID, 2, Time.now() + 1, 3L);
    String eTag3 = commitV1Part(volumeName, bucketName, keyName,
        multipartUploadID, 3, Time.now() + 2, 4L);

    List<Part> partList = new ArrayList<>();
    partList.add(Part.newBuilder().setETag(eTag1).setPartName(eTag1)
        .setPartNumber(1).build());
    partList.add(Part.newBuilder().setETag(eTag3).setPartName(eTag3)
        .setPartNumber(3).build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);
    S3MultipartUploadCompleteRequest completeReq =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);
    OMClientResponse omClientResponse =
        completeReq.validateAndUpdateCache(ozoneManager, 5L);
    BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation();
    omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // All three part rows -- including the discarded part 2 -- are deleted.
    for (int partNumber = 1; partNumber <= 3; partNumber++) {
      assertNull(omMetadataManager.getMultipartPartsTable()
          .get(OmMultipartPartKey.of(multipartUploadID, partNumber)));
    }

    // The discarded part's key was moved to the deleted table for block GC.
    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        multipartUploadID);
    assertFalse(omMetadataManager.getDeletedTable()
            .getRangeKVs(null, 100, multipartKey).isEmpty(),
        "discarded part should be moved to the deleted table");
  }

  private String checkValidateAndUpdateCacheSuccess(String volumeName,
      String bucketName, String keyName, Map<String, String> metadata, Map<String, String> tags) throws Exception {

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName, metadata, tags);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    // Add key to open key table.
    addKeyToTable(volumeName, bucketName, keyName, clientID);

    s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    List<Part> partList = new ArrayList<>();

    String eTag = s3MultipartUploadCommitPartRequest.getOmRequest()
        .getCommitMultiPartUploadRequest()
        .getKeyArgs()
        .getMetadataList()
        .stream()
        .filter(keyValue -> keyValue.getKey().equals(OzoneConsts.ETAG))
        .findFirst().get().getValue();
    partList.add(Part.newBuilder().setETag(eTag).setPartName(eTag).setPartNumber(1)
        .build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    BatchOperation batchOperation
        = omMetadataManager.getStore().initBatchOperation();
    omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
            multipartUploadID);

    assertNull(omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCompleteRequest.getBucketLayout())
        .get(multipartKey));
    assertNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));
    OmKeyInfo multipartKeyInfo = omMetadataManager
        .getKeyTable(s3MultipartUploadCompleteRequest.getBucketLayout())
        .get(getOzoneDBKey(volumeName, bucketName, keyName));
    assertNotNull(multipartKeyInfo);
    assertNotNull(multipartKeyInfo.getLatestVersionLocations());
    assertTrue(multipartKeyInfo.getLatestVersionLocations()
        .isMultipartKey());
    if (metadata != null) {
      assertThat(multipartKeyInfo.getMetadata()).containsAllEntriesOf(metadata);
    }
    if (tags != null) {
      assertThat(multipartKeyInfo.getTags()).containsAllEntriesOf(tags);
    }

    OmBucketInfo omBucketInfo = omMetadataManager.getBucketTable()
        .getCacheValue(new CacheKey<>(
            omMetadataManager.getBucketKey(volumeName, bucketName)))
        .getCacheValue();
    assertEquals(getNamespaceCount(),
        omBucketInfo.getUsedNamespace());
    return multipartUploadID;
  }

  @Test
  public void testValidateAndUpdateCacheRejectsSchemaVersionOneBeforeFinalization()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    String multipartUploadID =
        initiateMultipartUploadWithSchemaVersion(volumeName, bucketName,
            keyName, (byte) 1);

    // The request is still rejected before the empty part-list validation.
    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, new ArrayList<>());

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager,
            3L);

    assertEquals(OzoneManagerProtocolProtos.Status
        .NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION,
        omClientResponse.getOMResponse().getStatus());
  }

  protected void addVolumeAndBucket(String volumeName, String bucketName)
      throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
  }

  @Test
  public void testInvalidPartOrderError() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    // Add key to open key table.
    addKeyToTable(volumeName, bucketName, keyName, clientID);

    s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    List<Part> partList = new ArrayList<>();

    String partName = getPartName(volumeName, bucketName, keyName,
        multipartUploadID, 23);

    partList.add(Part.newBuilder().setETag(partName).setPartName(partName).setPartNumber(23).build());

    partName = getPartName(volumeName, bucketName, keyName, multipartUploadID, 1);
    partList.add(Part.newBuilder().setETag(partName).setPartName(partName).setPartNumber(1).build());

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, multipartUploadID, partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(OzoneManagerProtocolProtos.Status
        .INVALID_PART_ORDER, omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheVolumeNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
            getS3MultipartUploadCompleteReq(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheNoSuchMultipartUploadError()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    List<Part> partList = new ArrayList<>();

    OMRequest completeMultipartRequest = doPreExecuteCompleteMPU(volumeName,
        bucketName, keyName, UUID.randomUUID().toString(), partList);

    // Doing  complete multipart upload request with out initiate.
    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(completeMultipartRequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadCompleteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omClientResponse.getOMResponse().getStatus());

  }

  protected void addKeyToTable(String volumeName, String bucketName,
                             String keyName, long clientID) throws Exception {
    OMRequestTestUtils.addKeyToTable(true, true, volumeName, bucketName,
        keyName, clientID, RatisReplicationConfig.getInstance(ONE), omMetadataManager);
  }

  protected String getMultipartKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    return omMetadataManager.getMultipartKey(volumeName,
            bucketName, keyName, multipartUploadID);
  }

  private String getPartName(String volumeName, String bucketName,
      String keyName, String uploadID, int partNumber) {

    String dbOzoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    return S3MultipartUploadCommitPartRequest.getPartName(dbOzoneKey, uploadID,
        partNumber);
  }

  protected String getOzoneDBKey(String volumeName, String bucketName,
                                 String keyName) throws IOException {
    return omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }

  protected long getNamespaceCount() {
    return 1L;
  }
}

