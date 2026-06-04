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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.waitForFinalization;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.maxLayoutVersion;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * End-to-end upgrade test for the multipart parts-table split
 * (OMLayoutFeature.MPU_PARTS_TABLE_SPLIT). Drives full multipart uploads through
 * the real OzoneClient/RPC/datanode path across the finalization boundary and
 * inspects the OM metadata tables directly to prove the gate switches behavior.
 *
 * <p>Invariants exercised:</p>
 * <ul>
 *   <li>Pre-finalization an upload's parts are stored inline in the
 *       multipartInfoTable row (schemaVersion 0) and the split parts table is
 *       untouched.</li>
 *   <li>Post-finalization an upload's parts are stored in the dedicated
 *       multipartPartsTable (schemaVersion 1) and the inline map is empty;
 *       listParts, complete, and abort all operate on the split table, and
 *       complete/abort drain it.</li>
 *   <li>An upload initiated <em>before</em> finalization keeps schemaVersion 0
 *       even when completed <em>after</em> finalization -- the schema version is
 *       stamped at initiate, so finalizing mid-flight never strands an
 *       in-progress upload.</li>
 * </ul>
 *
 * <p>The cluster boots one layout version below MPU_PARTS_TABLE_SPLIT so the
 * pre-finalization cases run at schemaVersion 0, then finalizes. Test methods
 * are ordered into PRE / FINALIZE / POST groups; the in-flight upload's state is
 * captured in PRE and consumed in POST.</p>
 *
 * <p>Table reads use {@code get(OmMultipartPartKey.of(uploadId, n))} rather than
 * a prefix iterator: the OM apply path updates the table cache synchronously
 * (before the client RPC returns) but flushes to RocksDB asynchronously, and a
 * RocksDB prefix iterator would not see just-committed, not-yet-flushed parts.
 * {@code get} is cache-aware, so it reliably observes both committed parts and
 * the tombstones left by complete/abort.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestMultipartUploadPartsTableSplitUpgrade {

  private static final int PRE_FINALIZE = 100;
  private static final int FINALIZE = 200;
  private static final int POST_FINALIZE = 300;

  private static final int MIN_PART_SIZE = 1024;
  private static final int PART_SIZE = 4096;
  private static final ReplicationConfig RATIS_ONE =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);

  private final int preFinalizeVersion =
      OMLayoutFeature.MPU_PARTS_TABLE_SPLIT.layoutVersion() - 1;

  private MiniOzoneCluster cluster;
  private OzoneManager ozoneManager;
  private OzoneClient client;
  private ObjectStore store;
  private OzoneManagerProtocol omClient;

  // In-flight upload captured pre-finalization and completed post-finalization.
  private String inflightVolume;
  private String inflightBucket;
  private String inflightKey;
  private String inflightUploadId;
  private Map<Integer, String> inflightParts;
  private byte[] inflightExpected;

  @BeforeAll
  void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY, preFinalizeVersion);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
    // Allow tiny non-final parts so multi-part uploads use small data.
    ozoneManager.setMinMultipartUploadPartSize(MIN_PART_SIZE);
    client = cluster.newClient();
    store = client.getObjectStore();
    omClient = store.getClientProxy().getOzoneManagerClient();
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  // ---------------------------------------------------------------------------
  // PRE-FINALIZATION
  // ---------------------------------------------------------------------------

  @Test
  @Order(PRE_FINALIZE)
  void clusterStartsBelowSplitFeature() {
    assertEquals(preFinalizeVersion,
        ozoneManager.getVersionManager().getMetadataLayoutVersion());
  }

  @Test
  @Order(PRE_FINALIZE)
  void uploadBeforeFinalizationUsesInlineParts() throws Exception {
    OzoneBucket bucket = newObjectStoreBucket();
    String key = "key-pre-" + UUID.randomUUID();
    String uploadId = bucket.initiateMultipartUpload(key,
        RATIS_ONE).getUploadID();

    Map<Integer, String> parts = new TreeMap<>();
    byte[] expected = uploadParts(bucket, key, uploadId, 3, parts);

    OmMultipartKeyInfo info = mpuKeyInfo(bucket, key, uploadId);
    assertNotNull(info);
    assertEquals(0, info.getSchemaVersion());
    // Parts inline, split table untouched.
    assertEquals(3, info.getPartKeyInfoMap().size());
    assertEquals(0, countSplitParts(uploadId));

    bucket.completeMultipartUpload(key, uploadId, parts);
    assertArrayEquals(expected, readKey(bucket, key, expected.length));
  }

  @Test
  @Order(PRE_FINALIZE)
  void startUploadBeforeFinalization() throws Exception {
    OzoneBucket bucket = newObjectStoreBucket();
    inflightVolume = bucket.getVolumeName();
    inflightBucket = bucket.getName();
    inflightKey = "key-inflight-" + UUID.randomUUID();
    inflightUploadId = bucket.initiateMultipartUpload(inflightKey,
        RATIS_ONE).getUploadID();
    inflightParts = new TreeMap<>();
    inflightExpected =
        uploadParts(bucket, inflightKey, inflightUploadId, 2, inflightParts);

    // Stamped schemaVersion 0 at initiate; parts written inline.
    OmMultipartKeyInfo info =
        mpuKeyInfo(bucket, inflightKey, inflightUploadId);
    assertEquals(0, info.getSchemaVersion());
    assertEquals(2, info.getPartKeyInfoMap().size());
  }

  // ---------------------------------------------------------------------------
  // FINALIZE
  // ---------------------------------------------------------------------------

  @Test
  @Order(FINALIZE)
  void finalizeUpgrade() throws Exception {
    // The upgrade client id MUST match the one waitForFinalization polls with
    // ("finalize-test"); the OM tracks finalization per client id and rejects a
    // status query from an unknown client. A mismatch makes waitForFinalization
    // fail immediately instead of blocking, so the POST_FINALIZE tests would
    // then race an un-awaited, still-finalizing cluster.
    omClient.finalizeUpgrade("finalize-test");
    waitForFinalization(omClient);
    assertEquals(maxLayoutVersion(),
        ozoneManager.getVersionManager().getMetadataLayoutVersion());
  }

  // ---------------------------------------------------------------------------
  // POST-FINALIZATION
  // ---------------------------------------------------------------------------

  @Test
  @Order(POST_FINALIZE)
  void uploadAfterFinalizationUsesSplitTable() throws Exception {
    OzoneBucket bucket = newObjectStoreBucket();
    String key = "key-post-" + UUID.randomUUID();
    String uploadId = bucket.initiateMultipartUpload(key,
        RATIS_ONE).getUploadID();

    Map<Integer, String> parts = new TreeMap<>();
    byte[] expected = uploadParts(bucket, key, uploadId, 3, parts);

    OmMultipartKeyInfo info = mpuKeyInfo(bucket, key, uploadId);
    assertNotNull(info);
    assertEquals(1, info.getSchemaVersion());
    // Parts live in the split table; inline map is empty.
    assertEquals(0, info.getPartKeyInfoMap().size());
    assertEquals(3, countSplitParts(uploadId));

    // listParts reads through the schemaVersion 1 path.
    OzoneMultipartUploadPartListParts listed =
        bucket.listParts(key, uploadId, 0, 100);
    assertEquals(3, listed.getPartInfoList().size());

    bucket.completeMultipartUpload(key, uploadId, parts);
    assertArrayEquals(expected, readKey(bucket, key, expected.length));
    // Complete drains the split table for this upload.
    assertEquals(0, countSplitParts(uploadId));
  }

  @Test
  @Order(POST_FINALIZE)
  void abortAfterFinalizationCleansSplitTable() throws Exception {
    OzoneBucket bucket = newObjectStoreBucket();
    String key = "key-abort-" + UUID.randomUUID();
    String uploadId = bucket.initiateMultipartUpload(key,
        RATIS_ONE).getUploadID();

    Map<Integer, String> parts = new TreeMap<>();
    uploadParts(bucket, key, uploadId, 2, parts);
    assertEquals(2, countSplitParts(uploadId));

    bucket.abortMultipartUpload(key, uploadId);

    // Both the upload row and every split-table part row are gone.
    assertNull(mpuKeyInfo(bucket, key, uploadId));
    assertEquals(0, countSplitParts(uploadId));
  }

  @Test
  @Order(POST_FINALIZE)
  void uploadStartedBeforeFinalizationCompletesAsV0() throws Exception {
    // The in-flight upload was stamped schemaVersion 0 at initiate. Finalizing
    // mid-flight must not strand it: complete still reads the inline parts and
    // the reassembled object is correct.
    OzoneBucket bucket =
        store.getVolume(inflightVolume).getBucket(inflightBucket);

    OmMultipartKeyInfo info =
        mpuKeyInfo(bucket, inflightKey, inflightUploadId);
    assertEquals(0, info.getSchemaVersion());
    assertEquals(2, info.getPartKeyInfoMap().size());

    bucket.completeMultipartUpload(inflightKey, inflightUploadId, inflightParts);
    assertArrayEquals(inflightExpected,
        readKey(bucket, inflightKey, inflightExpected.length));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private OzoneBucket newObjectStoreBucket() throws IOException {
    String volumeName = "vol" + UUID.randomUUID().toString().substring(0, 12);
    String bucketName = "buck" + UUID.randomUUID().toString().substring(0, 12);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.OBJECT_STORE).build());
    return volume.getBucket(bucketName);
  }

  private byte[] uploadParts(OzoneBucket bucket, String key, String uploadId,
      int numParts, Map<Integer, String> partsOut) throws Exception {
    ByteArrayOutputStream all = new ByteArrayOutputStream();
    for (int i = 1; i <= numParts; i++) {
      byte[] data = new byte[PART_SIZE];
      java.util.Arrays.fill(data, (byte) ('a' + i));
      OzoneOutputStream out =
          bucket.createMultipartKey(key, data.length, i, uploadId);
      out.write(data);
      out.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(data));
      out.close();
      partsOut.put(i, out.getCommitUploadPartInfo().getETag());
      all.write(data);
    }
    return all.toByteArray();
  }

  private byte[] readKey(OzoneBucket bucket, String key, int length)
      throws IOException {
    byte[] content = new byte[length];
    try (OzoneInputStream in = bucket.readKey(key)) {
      int offset = 0;
      while (offset < length) {
        int read = in.read(content, offset, length - offset);
        if (read < 0) {
          break;
        }
        offset += read;
      }
    }
    return content;
  }

  private OmMultipartKeyInfo mpuKeyInfo(OzoneBucket bucket, String key,
      String uploadId) throws IOException {
    String multipartKey = ozoneManager.getMetadataManager().getMultipartKey(
        bucket.getVolumeName(), bucket.getName(), key, uploadId);
    return ozoneManager.getMetadataManager().getMultipartInfoTable()
        .get(multipartKey);
  }

  /**
   * Count the upload's parts in the split table via cache-aware point lookups
   * (get) on contiguous part numbers from 1.
   *
   * <p>The OM apply path updates the table cache synchronously but flushes to
   * RocksDB asynchronously, so when this runs right after the upload RPCs a
   * just-committed part may live only in the cache while an earlier part is
   * mid-flush. A merge scan (MultipartPartScanUtil.scanParts) builds from two
   * non-atomic snapshots -- a RocksDB prefix iterator and a separate cache
   * iterator -- so a part that flushes out of the cache between those two
   * snapshots is absent from both and the count comes up short (observed:
   * expected 3 but was 2). A point get re-checks cache then RocksDB live for
   * each key, so it never misses a committed part and still sees the tombstone
   * left by complete/abort. Tests upload contiguous parts 1..N, so counting
   * upward until the first absent part number yields N.</p>
   */
  private int countSplitParts(String uploadId) throws IOException {
    int count = 0;
    while (ozoneManager.getMetadataManager().getMultipartPartsTable()
        .get(OmMultipartPartKey.of(uploadId, count + 1)) != null) {
      count++;
    }
    return count;
  }
}
