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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.multipart.S3MultipartUploadCommitPartResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Tests S3 Multipart upload commit part request.
 */
public class TestS3MultipartUploadCommitPartRequest
    extends TestS3MultipartRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    doPreExecuteCommitMPU(volumeName, bucketName, keyName, Time.now(),
        UUID.randomUUID().toString(), 1);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

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
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);


    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.OK);

    String multipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));
    assertEquals(1, omMetadataManager.getMultipartInfoTable()
        .get(multipartKey).getPartKeyInfoMap().size());

    OmKeyInfo mpuOpenKeyInfo = omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(multipartOpenKey);
    assertNotNull(mpuOpenKeyInfo);
    assertNotNull(mpuOpenKeyInfo.getLatestVersionLocations());
    assertTrue(mpuOpenKeyInfo.getLatestVersionLocations()
        .isMultipartKey());

    String partKey = getOpenKey(volumeName, bucketName, keyName, clientID);
    assertNull(omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(partKey));
  }

  @Test
  public void testValidateAndUpdateCacheRejectsSchemaVersionOneBeforeFinalization()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    String multipartUploadID =
        initiateMultipartUploadWithSchemaVersion(volumeName, bucketName,
            keyName, (byte) 1);

    long clientID = Time.now();
    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    // Regular part metadata is present; the upgrade gate should still reject
    // this schema version before commit proceeds.
    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
            2L);

    assertEquals(OzoneManagerProtocolProtos.Status
        .NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheMultipartNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    // Add key to open key table.
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    assertNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));

  }

  /**
   * Regression guard for the commit-after-abort path. When the multipart
   * upload was aborted (no multipartInfoTable entry) but the part's open key
   * still exists, the request throws NO_SUCH_MULTIPART_UPLOAD_ERROR and the
   * response moves the orphaned part to the deleted table for GC. The null
   * part-key check MUST run only after the open key (omKeyInfo) is resolved;
   * otherwise the NO_SUCH response carries a null part and
   * S3MultipartUploadCommitPartResponse#checkAndUpdateDB NPEs when the double
   * buffer flushes it, terminating the OM. Unit tests that only assert the
   * response status do not exercise that flush, so this test drives the
   * response through checkAndUpdateDB to lock the ordering.
   */
  @Test
  public void testValidateAndUpdateCacheCommitAfterAbortGarbageCollectsPart()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    long clientID = Time.now();
    // Random uploadId => no multipartInfoTable entry (upload was aborted).
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    // The part's open key still exists (written during the part upload,
    // before the abort).
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
            2L);

    assertSame(OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omClientResponse.getOMResponse().getStatus());

    // Flushing the error response must not NPE, and the orphaned part must be
    // moved to the deleted table for garbage collection.
    BatchOperation batchOperation =
        omMetadataManager.getStore().initBatchOperation();
    omClientResponse.checkAndUpdateDB(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertFalse(omMetadataManager.getDeletedTable()
            .getRangeKVs(null, 100, "").isEmpty(),
        "orphaned part should be moved to the deleted table for GC");
  }

  @Test
  public void testValidateAndUpdateCacheV1WritesPartToPartsTable()
      throws Exception {
    // Post-finalization: initiate creates a schemaVersion 1 upload, so commit
    // must persist the part in the split parts table and leave the multipart
    // info row's inline part list empty.
    when(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .thenReturn(OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    OMRequest initiateMPURequest =
        doPreExecuteInitiateMPU(volumeName, bucketName, keyName);
    OMClientResponse initiateResponse =
        getS3InitiateMultipartUploadReq(initiateMPURequest)
            .validateAndUpdateCache(ozoneManager, 1L);
    String multipartUploadID = initiateResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    long clientID = Time.now();
    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);
    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);

    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
            2L);
    assertSame(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);
    OmMultipartKeyInfo multipartKeyInfo =
        omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(1, multipartKeyInfo.getSchemaVersion());
    // Parts are not inlined for schemaVersion 1.
    assertEquals(0, multipartKeyInfo.getPartKeyInfoMap().size());
    // The committed part lives in the split parts table.
    assertNotNull(omMetadataManager.getMultipartPartsTable()
        .get(OmMultipartPartKey.of(multipartUploadID, 1)));
  }

  @Test
  public void testValidateAndUpdateCacheV1OnOverwriteReclaimsOldPartBlocks()
      throws Exception {
    // Post-finalization v1 upload: re-committing the same part number must
    // reclaim the previously-committed part's blocks (read from the split
    // parts table and reconstructed via toOmKeyInfo) and store the new part,
    // exactly as the v0 inline path reclaims the overwritten inline part.
    when(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .thenReturn(OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    OMRequest initiateMPURequest =
        doPreExecuteInitiateMPU(volumeName, bucketName, keyName);
    String multipartUploadID = getS3InitiateMultipartUploadReq(initiateMPURequest)
        .validateAndUpdateCache(ozoneManager, 1L).getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    // First commit of part 1 (2 blocks).
    long clientID = Time.now();
    List<KeyLocation> originalKeyLocationList = getKeyLocation(5).subList(0, 2);
    List<OmKeyLocationInfo> originalKeyLocationInfos = originalKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID,
        originalKeyLocationInfos);
    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1,
        originalKeyLocationList);
    getS3MultipartUploadCommitReq(commitMultipartRequest)
        .validateAndUpdateCache(ozoneManager, 2L);

    OmMultipartPartKey partKey = OmMultipartPartKey.of(multipartUploadID, 1);
    assertNotNull(omMetadataManager.getMultipartPartsTable().get(partKey));

    // Re-commit part 1 (overwrite, 3 different blocks).
    clientID = Time.now();
    List<KeyLocation> overwriteKeyLocationList = getKeyLocation(5).subList(2, 5);
    List<OmKeyLocationInfo> overwriteKeyLocationInfos = overwriteKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID,
        overwriteKeyLocationInfos);
    OMRequest overwriteOMRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1,
        overwriteKeyLocationList);
    OMClientResponse overwriteResponse =
        getS3MultipartUploadCommitReq(overwriteOMRequest)
            .validateAndUpdateCache(ozoneManager, 3L);
    assertSame(OzoneManagerProtocolProtos.Status.OK,
        overwriteResponse.getOMResponse().getStatus());

    // The part row still exists (overwritten with the new part).
    assertNotNull(omMetadataManager.getMultipartPartsTable().get(partKey));

    // The previously-committed part's blocks must be queued for deletion,
    // matching the v0 inline behaviour (the 2 original blocks). This proves
    // the synthesized OmKeyInfo fed the old part's blocks to the GC path.
    Map<String, RepeatedOmKeyInfo> toDeleteKeyList =
        ((S3MultipartUploadCommitPartResponse) overwriteResponse)
            .getKeyToDelete();
    assertEquals(1, toDeleteKeyList.size());
    assertEquals(originalKeyLocationList.size(), toDeleteKeyList.values()
        .stream().findFirst().get().cloneOmKeyInfoList().get(0)
        .getKeyLocationVersions().get(0).getLocationList().size());
  }

  @Test
  public void testValidateAndUpdateCacheKeyNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);
    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);
    OMClientResponse initiateResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            1L);

    long clientID = Time.now();
    String multipartUploadID = initiateResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    // Don't add key to open table entry, and we are trying to commit this MPU
    // part. It will fail with KEY_NOT_FOUND

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);


    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheBucketFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);


    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    // Don't add key to open table entry, and we are trying to commit this MPU
    // part. It will fail with KEY_NOT_FOUND

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);


    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

  }

  @Test
  public void testValidateAndUpdateCacheOnOverwrite() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    // Create part key to be overwritten
    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    // Take the first 2 blocks for the part key to be overwritten
    List<KeyLocation> originalKeyLocationList = getKeyLocation(5).subList(0, 2);

    List<OmKeyLocationInfo> originalKeyLocationInfos = originalKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    // Add key to open key table.
    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, originalKeyLocationInfos);

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, originalKeyLocationList);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());
    PartKeyInfo partKeyInfo = multipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    OmKeyInfo partOmKeyInfo = OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());

    // Overwrite the key

    // New client ID for the overwritten key
    clientID = Time.now();

    // Take the last 3 blocks for the overwrite key
    List<KeyLocation> overwriteKeyLocationList = getKeyLocation(5).subList(2, 5);

    List<OmKeyLocationInfo> overwriteKeyLocationInfos = overwriteKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequest overwriteOMRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, overwriteKeyLocationList);

    S3MultipartUploadCommitPartRequest overwriteRequest = getS3MultipartUploadCommitReq(overwriteOMRequest);

    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, overwriteKeyLocationInfos);

    omClientResponse =
        overwriteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertSame(OzoneManagerProtocolProtos.Status.OK, omClientResponse.getOMResponse().getStatus());

    OmMultipartKeyInfo newMultipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    // Part key still remains the same
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());

    PartKeyInfo newPartKeyInfo = newMultipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    // Check modification time
    assertEquals(overwriteOMRequest.getCommitMultiPartUploadRequest()
        .getKeyArgs().getModificationTime(), newPartKeyInfo.getPartKeyInfo().getModificationTime());

    OmKeyInfo newPartOmKeyInfo = OmKeyInfo.getFromProtobuf(newPartKeyInfo.getPartKeyInfo());

    assertNotEquals(partOmKeyInfo, newPartOmKeyInfo);

    // Check block location
    List<OmKeyLocationInfo> locationsInfoListFromCommitPartRequest =
        overwriteOMRequest.getCommitMultiPartUploadRequest().getKeyArgs()
            .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());

    assertEquals(overwriteKeyLocationInfos, locationsInfoListFromCommitPartRequest);
    assertEquals(overwriteKeyLocationInfos, newPartOmKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(1, newPartOmKeyInfo.getKeyLocationVersions().size());

    Map<String, RepeatedOmKeyInfo> toDeleteKeyList =
        ((S3MultipartUploadCommitPartResponse) omClientResponse).getKeyToDelete();

    // Since there are no uncommitted blocks, only the overwritten (original) key should be deleted
    assertEquals(1, toDeleteKeyList.size());
    assertEquals(originalKeyLocationList.size(), toDeleteKeyList.values().stream()
        .findFirst().get().cloneOmKeyInfoList().get(0).getKeyLocationVersions()
        .get(0).getLocationList().size());
  }

  @Test
  public void testValidateAndUpdateCacheWithUncommittedBlocks() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    // Allocated block list (5 blocks)
    List<KeyLocation> allocatedKeyLocationList = getKeyLocation(5);

    List<OmKeyLocationInfo> allocatedBlockList = allocatedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    // Put the open key to simulate the part key upload using OMKeyCreateRequest
    String openMpuPartKey = addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, allocatedBlockList);

    OmKeyInfo openMpuPartKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openMpuPartKey);
    assertNotNull(openMpuPartKeyInfo);

    // Commit only the first 3 allocated blocks
    List<KeyLocation> committedKeyLocationList = allocatedKeyLocationList.subList(0, 3);

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, committedKeyLocationList);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    omClientResponse = s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    Map<String, RepeatedOmKeyInfo> toDeleteKeyList =
        ((S3MultipartUploadCommitPartResponse) omClientResponse).getKeyToDelete();

    // Since this part key is not overwritten, only the allocated but uncommitted
    // blocks should be deleted.
    assertEquals(1, toDeleteKeyList.size());
    assertEquals(2, toDeleteKeyList.values().stream()
        .findFirst().get().cloneOmKeyInfoList().get(0).getKeyLocationVersions()
        .get(0).getLocationList().size());

    String multipartOpenKey = getMultipartOpenKey(volumeName, bucketName,
        keyName, multipartUploadID);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));
    assertEquals(1, omMetadataManager.getMultipartInfoTable()
        .get(multipartKey).getPartKeyInfoMap().size());

    OmKeyInfo mpuOpenKeyInfo = omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(multipartOpenKey);
    assertNotNull(mpuOpenKeyInfo);
    assertNotNull(mpuOpenKeyInfo.getLatestVersionLocations());
    assertTrue(mpuOpenKeyInfo.getLatestVersionLocations()
        .isMultipartKey());

    assertNull(omMetadataManager
        .getOpenKeyTable(s3MultipartUploadCommitPartRequest.getBucketLayout())
        .get(openMpuPartKey));
  }

  @Test
  public void testValidateAndUpdateCacheOnOverWriteWithUncommittedBlocks() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    // Create key to be overwritten
    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    List<KeyLocation> originalKeyLocationList = getKeyLocation(5).subList(0, 2);

    List<OmKeyLocationInfo> originalKeyLocationInfos = originalKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, originalKeyLocationList);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, originalKeyLocationInfos);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());
    PartKeyInfo partKeyInfo = multipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    // Overwrite the key, at the same time there are some uncommitted blocks

    // New client ID for the overwritten key
    clientID = Time.now();

    // Allocate 3 blocks for the overwritten key
    List<KeyLocation> overwriteAllocatedKeyLocationList = getKeyLocation(5).subList(2, 5);

    List<OmKeyLocationInfo> overwriteAllocatedBlockList = overwriteAllocatedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    // Put the open key to simulate the part key upload using OMKeyCreateRequest
    String openMpuPartKey = addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID,
        overwriteAllocatedBlockList);

    OmKeyInfo openMpuPartKeyInfo =
        omMetadataManager.getOpenKeyTable(getBucketLayout()).get(openMpuPartKey);
    assertNotNull(openMpuPartKeyInfo);

    // Commit only the first allocated blocks
    List<KeyLocation> overwriteCommittedKeyLocationList = overwriteAllocatedKeyLocationList.subList(0, 1);

    List<OmKeyLocationInfo> overwriteCommittedBlockList = overwriteCommittedKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequest overwriteOMRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, overwriteCommittedKeyLocationList);

    S3MultipartUploadCommitPartRequest overwriteRequest = getS3MultipartUploadCommitReq(overwriteOMRequest);

    omClientResponse =
        overwriteRequest.validateAndUpdateCache(ozoneManager, 3L);

    assertSame(OzoneManagerProtocolProtos.Status.OK, omClientResponse.getOMResponse().getStatus());

    OmMultipartKeyInfo newMultipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());

    PartKeyInfo newPartKeyInfo = newMultipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    // Check modification time
    assertEquals(overwriteOMRequest.getCommitMultiPartUploadRequest()
        .getKeyArgs().getModificationTime(), newPartKeyInfo.getPartKeyInfo().getModificationTime());

    OmKeyInfo newPartOmKeyInfo = OmKeyInfo.getFromProtobuf(newPartKeyInfo.getPartKeyInfo());

    // Check block location
    List<OmKeyLocationInfo> locationsInfoListFromCommitPartRequest =
        overwriteOMRequest.getCommitMultiPartUploadRequest().getKeyArgs()
            .getKeyLocationsList().stream().map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList());

    assertEquals(overwriteCommittedBlockList, locationsInfoListFromCommitPartRequest);
    assertEquals(overwriteCommittedBlockList, newPartOmKeyInfo.getLatestVersionLocations().getLocationList());
    assertEquals(1, newPartOmKeyInfo.getKeyLocationVersions().size());

    Map<String, RepeatedOmKeyInfo> toDeleteKeyMap =
        ((S3MultipartUploadCommitPartResponse) omClientResponse).getKeyToDelete();

    // Since there are both uncommitted blocks and overwritten key blocks, there are two keys to delete
    assertEquals(2, toDeleteKeyMap.size());
  }

  @Test
  public void testValidateAndUpdateCacheWithUncommittedBlockForEmptyPart() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    createParentPath(volumeName, bucketName);

    // Create key to be overwritten
    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 1L);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    List<KeyLocation> emptyKeyLocationInfos = new ArrayList<>();
    List<KeyLocation> originalKeyLocationList = getKeyLocation(1);
    List<OmKeyLocationInfo> originalKeyLocationInfos = originalKeyLocationList
        .stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1, emptyKeyLocationInfos);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(commitMultipartRequest);

    addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID, originalKeyLocationInfos);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertSame(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager.getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(1, multipartKeyInfo.getPartKeyInfoMap().size());
    PartKeyInfo partKeyInfo = multipartKeyInfo.getPartKeyInfo(1);
    assertNotNull(partKeyInfo);

    Map<String, RepeatedOmKeyInfo> toDeleteKeyMap =
        ((S3MultipartUploadCommitPartResponse) omClientResponse).getKeyToDelete();
    assertNull(toDeleteKeyMap);
  }

  /**
   * Demonstrates the write-amplification fix that motivates the parts-table
   * split (HDDS-10611). On every CommitPart the schemaVersion 0 path appends the
   * part to the inline part list and re-serializes the whole multipartInfoTable
   * row, so that row -- rewritten on each of N commits -- grows linearly with
   * the part count (O(N^2) bytes rewritten across the upload). The schemaVersion
   * 1 path stores each part in the split parts table and leaves the inline list
   * empty, so the info row stays at its empty-parts size no matter how many
   * parts commit. Runs for both OBS and FSO via the subclass.
   */
  @Test
  public void testV1BoundsMultipartInfoRowGrowth() throws Exception {
    final int numParts = 20;

    // schemaVersion 0: the default (pre-finalization) layout version.
    int[] v0RowSizes = commitPartsMeasuringInfoRow(numParts);

    // schemaVersion 1: the finalized layout version.
    when(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .thenReturn(OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion());
    int[] v1RowSizes = commitPartsMeasuringInfoRow(numParts);

    for (int i = 1; i < numParts; i++) {
      // v0: the info row grows with every committed part.
      assertTrue(v0RowSizes[i] > v0RowSizes[i - 1],
          "schemaVersion 0 info row should grow on every commit");
      // v1: the info row never grows -- parts live in the split table.
      assertEquals(v1RowSizes[0], v1RowSizes[i],
          "schemaVersion 1 info row size must not grow with part count");
    }

    // After N parts the v1 info row is far smaller than the v0 row that carries
    // all N parts inline.
    assertTrue(v1RowSizes[numParts - 1] < v0RowSizes[numParts - 1],
        "schemaVersion 1 info row must stay smaller than the inline v0 row");
  }

  /**
   * Commit {@code numParts} parts to a fresh upload and return the serialized
   * size of its multipartInfoTable row after each commit.
   */
  private int[] commitPartsMeasuringInfoRow(int numParts) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);

    OMRequest initiateMPURequest =
        doPreExecuteInitiateMPU(volumeName, bucketName, keyName);
    String uploadID = getS3InitiateMultipartUploadReq(initiateMPURequest)
        .validateAndUpdateCache(ozoneManager, 1L).getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();
    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, uploadID);

    int[] rowSizes = new int[numParts];
    long txnIndex = 2L;
    for (int partNumber = 1; partNumber <= numParts; partNumber++) {
      long clientID = 1000L + partNumber;
      addKeyToOpenKeyTable(volumeName, bucketName, keyName, clientID);
      OMRequest commitRequest = doPreExecuteCommitMPU(volumeName, bucketName,
          keyName, clientID, uploadID, partNumber);
      OMClientResponse response = getS3MultipartUploadCommitReq(commitRequest)
          .validateAndUpdateCache(ozoneManager, txnIndex++);
      assertSame(OzoneManagerProtocolProtos.Status.OK,
          response.getOMResponse().getStatus());
      OmMultipartKeyInfo infoRow =
          omMetadataManager.getMultipartInfoTable().get(multipartKey);
      assertNotNull(infoRow);
      rowSizes[partNumber - 1] = infoRow.getProto().getSerializedSize();
    }
    return rowSizes;
  }

  protected void addKeyToOpenKeyTable(String volumeName, String bucketName,
      String keyName, long clientID) throws Exception {
    OMRequestTestUtils.addKeyToTable(true, true, volumeName, bucketName,
        keyName, clientID, RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE), omMetadataManager);
  }

  protected String addKeyToOpenKeyTable(String volumeName, String bucketName,
      String keyName, long clientID, List<OmKeyLocationInfo> locationList) throws Exception {
    OMRequestTestUtils.addKeyToTable(true, true, false,
        volumeName, bucketName, keyName,
        clientID, RatisReplicationConfig.getInstance(ReplicationFactor.ONE), 0L,
        omMetadataManager, locationList, 0L);

    return getOpenKey(volumeName, bucketName, keyName, clientID);
  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }

  protected String getMultipartOpenKey(String volumeName, String bucketName,
      String keyName, String multipartUploadID) throws IOException {
    return omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName, multipartUploadID);
  }

  protected String getOpenKey(String volumeName, String bucketName,
      String keyName, long clientID) throws IOException {
    return omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, clientID);
  }

  protected void createParentPath(String volumeName, String bucketName)
          throws Exception {
    // no parent hierarchy
  }

  /**
   * Create KeyLocation list.
   */
  protected List<KeyLocation> getKeyLocation(int count) {
    List<KeyLocation> keyLocations = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      KeyLocation keyLocation =
          KeyLocation.newBuilder()
              .setBlockID(HddsProtos.BlockID.newBuilder()
                  .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                      .setContainerID(i + 1000).setLocalID(i + 100).build()))
              .setOffset(0).setLength(200).setCreateVersion(0L).build();
      keyLocations.add(keyLocation);
    }
    return keyLocations;
  }
}
