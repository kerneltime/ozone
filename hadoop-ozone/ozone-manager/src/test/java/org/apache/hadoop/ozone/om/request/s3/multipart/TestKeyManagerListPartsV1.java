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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmPartInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link org.apache.hadoop.ozone.om.KeyManagerImpl#listParts} for
 * schemaVersion 1 (split parts table) uploads.
 *
 * <p>After finalization an upload's per-part rows live in the dedicated
 * multipartPartsTable instead of inline in the multipartInfoTable row, so the
 * inline {@code getPartKeyInfoMap()} is empty. listParts must instead page the
 * parts table (cache-aware) and adapt each row back into the same shape the
 * inline path produced. These tests assert that adaptation preserves every
 * displayed field (part number, eTag, size, modification time), keeps ascending
 * part-number order, honors the (partNumberMarker, maxParts) pagination
 * contract, and merges in-cache (not-yet-flushed) parts over the persisted
 * RocksDB rows.</p>
 *
 * <p>The harness builds a real KeyManagerImpl over an in-memory metadata
 * manager (no OM RPC boot), so the read path runs end to end. Parts are seeded
 * directly into the parts table rather than committed through the write path,
 * which keeps the setup layout-agnostic and independent of finalization state.
 * The FSO variant overrides only the layout-specific scaffolding.</p>
 */
public class TestKeyManagerListPartsV1 extends TestS3MultipartRequest {

  private KeyManagerImpl keyManager;

  /**
   * Build a KeyManagerImpl over the base harness's in-memory metadata manager.
   * Only the read path is exercised, so block-token, KMS, SCM, and metrics
   * dependencies are unused and left null/mocked. The base @BeforeEach runs
   * first and wires {@code ozoneManager.getMetadataManager()}.
   */
  @BeforeEach
  public void initKeyManager() {
    keyManager = new KeyManagerImpl(ozoneManager, mock(ScmClient.class),
        omMetadataManager, new OzoneConfiguration(), null, null, null);
  }

  /**
   * Create the volume/bucket (+ FSO parent dirs) and initiate a schemaVersion 1
   * multipart upload. Returns its upload id.
   */
  private String setupV1Upload(String volumeName, String bucketName,
      String keyName) throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    createParentPath(volumeName, bucketName);
    return initiateMultipartUploadWithSchemaVersion(volumeName, bucketName,
        keyName, (byte) 1);
  }

  /**
   * Build a committed part with caller-controlled identity so assertions can
   * verify exact round-trip values. The part stores its eTag in metadata (as a
   * real CommitPart does) so the listParts eTag projection finds it.
   */
  private OmMultipartPartInfo buildV1Part(int partNumber, long modTime,
      long dataSize, String eTag) {
    KeyLocation keyLocation = KeyLocation.newBuilder()
        .setBlockID(HddsProtos.BlockID.newBuilder()
            .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                .setContainerID(partNumber + 1000L)
                .setLocalID(partNumber + 100L).build()))
        .setOffset(0).setLength(200).setCreateVersion(0L).build();
    OmKeyInfo partKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName("vol").setBucketName("bucket").setKeyName("key")
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, Collections.singletonList(
                OmKeyLocationInfo.getFromProtobuf(keyLocation)), true)))
        .setDataSize(dataSize)
        .setCreationTime(modTime)
        .setModificationTime(modTime)
        .setObjectID(1000L + partNumber)
        .setUpdateID(partNumber)
        .addMetadata(OzoneConsts.ETAG, eTag)
        .build();
    return OmMultipartPartInfo.from("part-" + partNumber, partNumber,
        partKeyInfo);
  }

  /** Seed a part into the cache only (an applied-but-not-yet-flushed part). */
  private void cacheV1Part(String uploadID, OmMultipartPartInfo part) {
    omMetadataManager.getMultipartPartsTable().addCacheEntry(
        new CacheKey<>(OmMultipartPartKey.of(uploadID, part.getPartNumber())),
        CacheValue.get(part.getPartNumber(), part));
  }

  /** Seed a part into the persisted RocksDB table only. */
  private void putV1Part(String uploadID, OmMultipartPartInfo part)
      throws IOException {
    omMetadataManager.getMultipartPartsTable().put(
        OmMultipartPartKey.of(uploadID, part.getPartNumber()), part);
  }

  @Test
  public void testListPartsV1ReturnsAllPartsWithFields() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String uploadID = setupV1Upload(volumeName, bucketName, keyName);

    for (int i = 1; i <= 5; i++) {
      cacheV1Part(uploadID, buildV1Part(i, 1000L + i, 100L * i, "etag-" + i));
    }

    OmMultipartUploadListParts result = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 0, 10);

    List<OmPartInfo> parts = result.getPartInfoList();
    assertEquals(5, parts.size());
    assertFalse(result.isTruncated());
    assertEquals(0, result.getNextPartNumberMarker());
    assertNotNull(result.getReplicationConfig());
    for (int i = 0; i < 5; i++) {
      OmPartInfo part = parts.get(i);
      int expected = i + 1;
      assertEquals(expected, part.getPartNumber());
      assertEquals("etag-" + expected, part.getETag());
      assertEquals(100L * expected, part.getSize());
      assertEquals(1000L + expected, part.getModificationTime());
      assertNotNull(part.getPartName());
      assertFalse(part.getPartName().isEmpty());
    }
  }

  @Test
  public void testListPartsV1Pagination() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String uploadID = setupV1Upload(volumeName, bucketName, keyName);

    for (int i = 1; i <= 5; i++) {
      cacheV1Part(uploadID, buildV1Part(i, 1000L + i, 100L * i, "etag-" + i));
    }

    // First page: parts 1,2; more remain.
    OmMultipartUploadListParts page1 = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 0, 2);
    assertEquals(2, page1.getPartInfoList().size());
    assertEquals(1, page1.getPartInfoList().get(0).getPartNumber());
    assertEquals(2, page1.getPartInfoList().get(1).getPartNumber());
    assertTrue(page1.isTruncated());
    assertEquals(2, page1.getNextPartNumberMarker());

    // Middle page from marker 2: parts 3,4; more remain.
    OmMultipartUploadListParts page2 = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 2, 2);
    assertEquals(2, page2.getPartInfoList().size());
    assertEquals(3, page2.getPartInfoList().get(0).getPartNumber());
    assertEquals(4, page2.getPartInfoList().get(1).getPartNumber());
    assertTrue(page2.isTruncated());
    assertEquals(4, page2.getNextPartNumberMarker());

    // Last page from marker 4: part 5; none remain.
    OmMultipartUploadListParts page3 = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 4, 10);
    assertEquals(1, page3.getPartInfoList().size());
    assertEquals(5, page3.getPartInfoList().get(0).getPartNumber());
    assertFalse(page3.isTruncated());
    assertEquals(0, page3.getNextPartNumberMarker());

    // Marker past the last part: empty, not truncated.
    OmMultipartUploadListParts page4 = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 5, 10);
    assertEquals(0, page4.getPartInfoList().size());
    assertFalse(page4.isTruncated());
  }

  @Test
  public void testListPartsV1PaginationPersisted() throws Exception {
    // Same pagination/truncation as testListPartsV1Pagination, but the parts
    // live ONLY in RocksDB (no cache) -- the post-flush cluster state that the
    // full-cluster integration test exercises. Truncation must still be
    // detected when the page is exactly the persisted-part boundary.
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String uploadID = setupV1Upload(volumeName, bucketName, keyName);

    for (int i = 1; i <= 3; i++) {
      putV1Part(uploadID, buildV1Part(i, 1000L + i, 100L * i, "etag-" + i));
    }

    // 3 parts, page size 2: first page returns 2 and MUST report truncated.
    OmMultipartUploadListParts page1 = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 0, 2);
    assertEquals(2, page1.getPartInfoList().size());
    assertTrue(page1.isTruncated());
    assertEquals(2, page1.getNextPartNumberMarker());

    OmMultipartUploadListParts page2 = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 2, 2);
    assertEquals(1, page2.getPartInfoList().size());
    assertEquals(3, page2.getPartInfoList().get(0).getPartNumber());
    assertFalse(page2.isTruncated());
  }

  @Test
  public void testListPartsV1ExactPageBoundary() throws Exception {
    // Fence-post: when the remaining parts EXACTLY equal maxParts, the page is
    // full but must NOT be truncated (the +1 lookahead finds no extra part) and
    // the next-marker resets to 0. Mirrors the persisted (RocksDB) cluster path.
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String uploadID = setupV1Upload(volumeName, bucketName, keyName);

    for (int i = 1; i <= 2; i++) {
      putV1Part(uploadID, buildV1Part(i, 1000L + i, 100L * i, "etag-" + i));
    }

    // remaining == maxParts: full page, NOT truncated, marker reset.
    OmMultipartUploadListParts page = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 0, 2);
    assertEquals(2, page.getPartInfoList().size());
    assertFalse(page.isTruncated());
    assertEquals(0, page.getNextPartNumberMarker());
  }

  @Test
  public void testListPartsV1MergesCacheOverPersisted() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String uploadID = setupV1Upload(volumeName, bucketName, keyName);

    // Odd parts persisted to RocksDB, even parts only in the cache. listParts
    // must return the union, ordered by part number (the I13 merge scan).
    putV1Part(uploadID, buildV1Part(1, 1001L, 100L, "etag-1"));
    cacheV1Part(uploadID, buildV1Part(2, 1002L, 200L, "etag-2"));
    putV1Part(uploadID, buildV1Part(3, 1003L, 300L, "etag-3"));
    cacheV1Part(uploadID, buildV1Part(4, 1004L, 400L, "etag-4"));
    putV1Part(uploadID, buildV1Part(5, 1005L, 500L, "etag-5"));

    OmMultipartUploadListParts result = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 0, 10);

    List<OmPartInfo> parts = result.getPartInfoList();
    assertEquals(5, parts.size());
    for (int i = 0; i < 5; i++) {
      int expected = i + 1;
      assertEquals(expected, parts.get(i).getPartNumber());
      assertEquals("etag-" + expected, parts.get(i).getETag());
      assertEquals(100L * expected, parts.get(i).getSize());
    }
  }

  @Test
  public void testListPartsV1NoParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = getKeyName();
    String uploadID = setupV1Upload(volumeName, bucketName, keyName);

    OmMultipartUploadListParts result = keyManager.listParts(volumeName,
        bucketName, keyName, uploadID, 0, 10);

    assertEquals(0, result.getPartInfoList().size());
    assertFalse(result.isTruncated());
    // With no parts the replication config is resolved from the open key.
    assertNotNull(result.getReplicationConfig());
  }

  protected String getKeyName() {
    return UUID.randomUUID().toString();
  }

  protected void createParentPath(String volumeName, String bucketName)
      throws Exception {
    // OBS has no parent hierarchy.
  }
}
