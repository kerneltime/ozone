/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.TableConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaOneImpl;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;

@CommandLine.Command(
    name = "cache-test",
    description = "Shell of updating datanode layout format",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
@MetaInfServices(SubcommandWithParent.class)
public class ContainerCacheTest extends GenericCli
    implements Callable<Void>, SubcommandWithParent{

  @CommandLine.Option(names = {"--path"},
      description = "File Path")
  private String storagePath;

  @CommandLine.Option(names = {"--numDB"},
      defaultValue = "1000",
      description = "")
  private int numDb;

  @CommandLine.Option(names = {"--cacheSize"},
      defaultValue = "1024",
      description = "")
  private int cacheSize;

  @CommandLine.Option(names = {"--stripes"},
      defaultValue = "1024",
      description = "")
  private int stripes;

  @CommandLine.Option(names = {"--numIterations"},
      defaultValue = "50000",
      description = "")
  private int iter;

  @CommandLine.Option(names = {"--timeoutInMins"},
      defaultValue = "20",
      description = "")
  private int timeout;

  @CommandLine.Option(names = {"--numThreads"},
      defaultValue = "100",
      description = "")
  private int numThreads;

  @CommandLine.Option(names = {"--skipDBCreation"},
      defaultValue = "false",
      description = "Skip creation of DBs at start, useful to rerun the same test")
  private boolean skipDBCreation;

  @CommandLine.Option(names = {"--numKeysPerIter"},
      defaultValue = "10",
      description = "")
  private int numKeysPerIter;
  @CommandLine.Option(names = {"--runRaw"},
      defaultValue = "false",
      description = "")
  private boolean runRaw;

  @Spec
  private CommandSpec spec;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf = createOzoneConfiguration();
    if (runRaw) {
      runRawRocks(storagePath, numDb, iter, numThreads, numKeysPerIter, skipDBCreation, timeout);
      return null;
    }
    run(conf, storagePath, numDb, cacheSize, stripes, iter, timeout,
        numThreads, numKeysPerIter, skipDBCreation);
    return null;
  }

  public static void main(String[] args) {
    new DatanodeLayout().run(args);
  }

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  public static void runRawRocks(String storagePaths,
                                 int numDb,
                                 int iter,
                                 int numThreads,
                                 int numKeysPerIter,
                                 boolean skipDBCreation,
                                 int timeout) throws Exception {
    String[] paths = new String[42];
    for (int i=0; i <42; i++) {
      int index = i + 1;
      paths[i] = "/data/disk"+index+"/hadoop-ozone/datanode/cache-test-3";
    }
    int numPaths = paths.length;
    final AtomicInteger count = new AtomicInteger();
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger collided = new AtomicInteger();
    for (int i = 0; i < numPaths; i++) {
      File root = new File(paths[i]);
      root.mkdirs();
    }
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < iter; i++) {
      if (i%10000 == 0) {
        System.out.println("Iteration "+i);
      }
      executorService.execute(() -> {
        int cont = new Random().nextInt(numDb);
        File root = new File(paths[cont % numPaths]);
        File containerDir = new File(root, "cont" + cont);
        DBOptions options = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        WriteOptions writeOptions = new WriteOptions().setSync(false);
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        TableConfig tableConfig = new TableConfig("default", columnFamilyOptions);
        columnFamilyDescriptors.add(tableConfig.getDescriptor());
        RocksDB db = null;
        try {
          db = RocksDB.open(options, containerDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);
          opened.incrementAndGet();
        } catch (Exception e) {
          collided.incrementAndGet();
        }
        try {
          if (db != null) {
            db.pauseBackgroundWork();
          }
          if (db != null) {
            db.close();
          }

          if (writeOptions != null) {
            writeOptions.close();
          }

          if (options != null) {
            options.close();
          }

          for (ColumnFamilyDescriptor c : columnFamilyDescriptors) {
            c.getOptions().close();
          }
        } catch (Exception e) {
          System.out.println("Hit exception during pause background work " + e);
        }
        if (count.get()%1000 == 0) {
          System.out.println("Done " + count.incrementAndGet() + " open:" + opened.get() +" collided:"+collided.get());
        }
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(timeout, TimeUnit.MINUTES);
    System.out.println("Done open:" + opened.get() +" collided:"+collided.get());
  }
  public static void run(OzoneConfiguration conf,
                                            String storagePaths,
                                            int numDb,
                                            int cacheSize,
                                            int stripes,
                                            int iter, int timeout,
                         int numThreads, int numKeysPerIter, boolean skipDBCreation) throws Exception {

    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, cacheSize);
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_LOCK_STRIPES, stripes);
    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.clear();
    String[] paths = new String[42];
    for (int i=0; i <42; i++) {
      int index = i + 1;
      paths[i] = "/data/disk"+index+"/hadoop-ozone/datanode/cache-test-3";
    }
    int numPaths = paths.length;
    for (int i = 0; i < numPaths; i ++) {
      File root = new File(paths[i]);
      root.mkdirs();
    }

    if (!skipDBCreation) {
      for (int i = 0; i < numDb; i++) {
        File root = new File(paths[i % numPaths]);
        File containerDir1 = new File(root, "cont" + i);
        createContainerDB(conf, containerDir1);
        System.out.println("Created DB " + (i + 1));
      }
    }

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    AtomicInteger count = new AtomicInteger(0);

    for (int i = 0; i < iter; i++) {
      int finalI1 = i;
      executorService.execute(() -> {
        int cont = new Random().nextInt(numDb);
        File root = new File(paths[cont % numPaths]);
        File containerDir1 = new File(root, "cont" + cont);
        ReferenceCountedDB refcountedDB = null;
        try {
          refcountedDB = cache.getDB(1, "RocksDB",
            containerDir1.getPath(), SCHEMA_V1, conf);
/*          DatanodeStore store = refcountedDB.getStore();
          for (int j = 0; j < numKeysPerIter; j++) {
            store.getMetadataTable().put(String.valueOf(System.currentTimeMillis()), (long) j);
          }
          //store.flushLog(true);
          for (int j = 0; j < 10; j++) {
            store.getMetadataTable().put(String.valueOf(System.currentTimeMillis()), (long) j);
          }
          //store.flushLog(true);*/
          System.out.println("Count = " + count.incrementAndGet());
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          if (refcountedDB != null) {
            System.out.println("No writes Refcounted DB:" + refcountedDB.getStore().getStore().getDbLocation() + " value:" + refcountedDB.getReferenceCount());
            refcountedDB.close();
          }
        }
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(timeout, TimeUnit.MINUTES);
  }

  private static void createContainerDB(OzoneConfiguration conf, File dbFile)
      throws Exception {
    DatanodeStore store = new DatanodeStoreSchemaOneImpl(
        conf, 1, dbFile.getAbsolutePath(), false);

    // we close since the SCM pre-creates containers.
    // we will open and put Db handle into a cache when keys are being created
    // in a container.

    store.stop();
  }
}