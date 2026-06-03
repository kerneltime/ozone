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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;

/**
 * Reads the parts of a schemaVersion 1 multipart upload from the split parts
 * table.
 *
 * <p>The scan is <em>cache-aware</em>: it merges the in-memory table cache over
 * the persisted RocksDB rows so that parts written by an earlier CommitPart in
 * the same (not-yet-flushed) double-buffer batch are visible to a later
 * Complete/Abort applied in that same batch. A raw RocksDB iterator would miss
 * them, because the OM apply path does not flush between transactions. The
 * cache wins over the persisted view, and a cache entry whose value is
 * {@code null} (a delete) tombstones the corresponding row.</p>
 */
public final class MultipartPartScanUtil {

  private MultipartPartScanUtil() {
  }

  /**
   * Returns all parts of {@code uploadId}, ordered ascending by part number.
   *
   * @param omMetadataManager the OM metadata manager
   * @param uploadId the multipart upload id
   * @return part number -&gt; part info, sorted by part number
   */
  public static SortedMap<Integer, OmMultipartPartInfo> scanParts(
      OMMetadataManager omMetadataManager, String uploadId)
      throws IOException {
    Table<OmMultipartPartKey, OmMultipartPartInfo> partsTable =
        omMetadataManager.getMultipartPartsTable();

    SortedMap<Integer, OmMultipartPartInfo> parts = new TreeMap<>();

    // Persisted base: every row under the uploadId prefix, in part-number
    // order (the key encoding sorts by part number).
    try (KeyValueIterator<OmMultipartPartKey, OmMultipartPartInfo> iterator =
        partsTable.iterator(OmMultipartPartKey.prefix(uploadId))) {
      while (iterator.hasNext()) {
        KeyValue<OmMultipartPartKey, OmMultipartPartInfo> kv = iterator.next();
        if (kv.getKey().hasPartNumber()) {
          parts.put(kv.getKey().getPartNumber(), kv.getValue());
        }
      }
    }

    // Overlay the cache (uncommitted writes win; a null value tombstones).
    Iterator<Map.Entry<CacheKey<OmMultipartPartKey>,
        CacheValue<OmMultipartPartInfo>>> cacheIterator =
        partsTable.cacheIterator();
    while (cacheIterator.hasNext()) {
      Map.Entry<CacheKey<OmMultipartPartKey>,
          CacheValue<OmMultipartPartInfo>> entry = cacheIterator.next();
      OmMultipartPartKey key = entry.getKey().getCacheKey();
      if (!uploadId.equals(key.getUploadId()) || !key.hasPartNumber()) {
        continue;
      }
      OmMultipartPartInfo value = entry.getValue().getCacheValue();
      if (value == null) {
        parts.remove(key.getPartNumber());
      } else {
        parts.put(key.getPartNumber(), value);
      }
    }

    return parts;
  }
}
