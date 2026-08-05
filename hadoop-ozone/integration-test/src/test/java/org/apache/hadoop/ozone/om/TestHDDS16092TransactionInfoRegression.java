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

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.Test;

/**
 * Reproduction for HDDS-16092. NOT INTENDED FOR MERGE -- this branch exists so the race can be
 * observed directly rather than argued from a code trace.
 *
 * <h2>What it demonstrates</h2>
 *
 * The OM persists how far it has applied in TRANSACTION_INFO_KEY. Two paths write that key and
 * are not ordered against each other:
 * <ul>
 *   <li>{@code OzoneManagerDoubleBuffer.flushBatch} writes it <em>inside</em> the RocksDB batch
 *       holding the transaction data, so data and index commit together, and advances the state
 *       machine's in-memory applied index only <em>after</em> that commit returns.</li>
 *   <li>{@code OzoneManagerStateMachine.takeSnapshotImpl} computes an index from that in-memory
 *       value and writes the key with a direct, unbatched put.</li>
 * </ul>
 * A snapshot landing between the commit and the advance therefore writes an index lower than the
 * one just committed, leaving the DB holding transactions its own index disclaims.
 *
 * <h2>How to run it</h2>
 *
 * <pre>
 *   mvn -pl hadoop-ozone/integration-test -am test \
 *       -Dtest=TestHDDS16092TransactionInfoRegression
 * </pre>
 *
 * On this branch (unmodified master) it FAILS, reporting the persisted index moving backwards --
 * that failure is the bug. Cherry-pick the HDDS-16092 fix commits on top and it PASSES.
 *
 * <h2>Why the threshold is lowered, and why this is not a CI test</h2>
 *
 * The only setting changed from production is
 * {@code ozone.om.ratis.snapshot.auto.trigger.threshold}, dropped from its 400000 default to 50.
 * That does not create the race. It turns one draw per 400000 transactions into a draw every few
 * transactions, so the existing window gets sampled thousands of times in a short run instead of
 * once. This is exactly why the defect is not seen in the field.
 *
 * Because it depends on a non-production setting and on thread timing, it makes a poor CI gate --
 * a green run proves nothing. The committed regression tests on the fix are the deterministic
 * equivalent; this one is for seeing the real thing happen.
 *
 * <h2>The detector</h2>
 *
 * A watcher per OM polls the persisted index with {@code getSkipCache} (as
 * {@code TransactionInfo.readTransactionInfo} does for this key) and records any move backwards.
 * A persisted watermark decreasing is self-evidently wrong, so this needs no knowledge of the
 * true applied index. All three OMs are watched: each runs its own state machine updater and its
 * own flush daemon, so each is an independent draw. Observed hits land on followers.
 */
public class TestHDDS16092TransactionInfoRegression {

  private static final PrintStream OUT = System.out;
  private static final String OM_SERVICE_ID = "om-repro-16092";
  private static final int RUN_SECONDS = 90;
  private static final long SNAPSHOT_THRESHOLD = 50L;

  @Test
  public void persistedTransactionIndexMustNeverMoveBackwards() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT, BucketLayout.OBJECT_STORE.name());
    conf.setLong(OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);

    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(OM_SERVICE_ID).setNumOfOzoneManagers(3).setNumDatanodes(1);
    MiniOzoneHAClusterImpl cluster = builder.build();

    final AtomicBoolean running = new AtomicBoolean(true);
    final List<String> regressions = new CopyOnWriteArrayList<>();
    final AtomicLong writes = new AtomicLong();
    final AtomicLong samples = new AtomicLong();
    final ExecutorService pool = Executors.newFixedThreadPool(5);

    try {
      cluster.waitForClusterToBeReady();
      try (OzoneClient setup = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf)) {
        setup.getObjectStore().createVolume("vol1");
        setup.getObjectStore().getVolume("vol1").createBucket("buck1");
      }

      for (OzoneManager om : cluster.getOzoneManagersList()) {
        final OzoneManager target = om;
        pool.submit(() -> {
          long highest = -1;
          while (running.get()) {
            try {
              TransactionInfo info = target.getMetadataManager()
                  .getTransactionInfoTable().getSkipCache(TRANSACTION_INFO_KEY);
              if (info != null) {
                long idx = info.getTransactionIndex();
                samples.incrementAndGet();
                if (idx < highest) {
                  String hit = target.getOMNodeId() + ": persisted index went BACKWARDS "
                      + highest + " -> " + idx;
                  regressions.add(hit);
                  OUT.println("HDDS-16092 HIT: " + hit);
                }
                highest = Math.max(highest, idx);
              }
            } catch (Exception ignored) {
              // The store can be briefly unavailable; keep sampling.
            }
          }
          return null;
        });
      }

      for (int w = 0; w < 2; w++) {
        final int id = w;
        pool.submit(() -> {
          try (OzoneClient client = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf)) {
            OzoneVolume vol = client.getObjectStore().getVolume("vol1");
            OzoneBucket bucket = vol.getBucket("buck1");
            long n = 0;
            while (running.get()) {
              write(bucket, "w" + id + "-key" + (n % 50), "v" + n);
              writes.incrementAndGet();
              n++;
            }
          }
          return null;
        });
      }

      OUT.println("HDDS-16092: running up to " + RUN_SECONDS + "s, snapshot threshold "
          + SNAPSHOT_THRESHOLD + ", 3 OMs watched");
      for (int s = 0; s < RUN_SECONDS && regressions.isEmpty(); s++) {
        TimeUnit.SECONDS.sleep(1);
      }
      running.set(false);
      TimeUnit.SECONDS.sleep(2);

      OUT.println("HDDS-16092 SUMMARY: writes=" + writes.get() + " samples=" + samples.get()
          + " regressions=" + regressions.size());
      regressions.forEach(r -> OUT.println("HDDS-16092 SUMMARY:   " + r));

      assertTrue(regressions.isEmpty(),
          () -> "The persisted transaction index moved backwards, so the DB now contains "
              + "transactions its own index disclaims: " + regressions);
    } finally {
      running.set(false);
      pool.shutdownNow();
      cluster.shutdown();
    }
  }

  private static void write(OzoneBucket bucket, String key, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    try (OutputStream out = bucket.createKey(key, bytes.length,
        ReplicationType.RATIS, ReplicationFactor.ONE, new HashMap<>())) {
      out.write(bytes);
    }
  }
}
