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

package org.apache.hadoop.ozone.om.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OmMultipartUploadCompleteList}.
 */
public class TestOmMultipartUploadCompleteList {

  /**
   * Pins the HDDS-14661 eTag-less-multipart contract: getPartsList() must
   * populate BOTH the proto partName and eTag fields from the single per-part
   * identifier the caller supplied.
   *
   * <p>For an S3 client that identifier is the part's MD5 eTag; for the native
   * Ozone client (which computes no eTag) it is the part NAME. Mirroring it into
   * the eTag field is what keeps a native, eTag-less Complete on the OM's
   * eTag-based validator path (which it enters only when every Part hasETag) --
   * where it then passes via that validator's stored-part-name fallback. If this
   * mirroring is dropped, native multipart Complete fails every part with
   * INVALID_PART, so this test guards against that regression.</p>
   */
  @Test
  public void testGetPartsListMirrorsIdentifierIntoPartNameAndETag() {
    Map<Integer, String> partMap = new LinkedHashMap<>();
    partMap.put(1, "part-identifier-1");
    partMap.put(2, "part-identifier-2");

    List<Part> parts =
        new OmMultipartUploadCompleteList(partMap).getPartsList();

    assertEquals(2, parts.size());
    for (Part part : parts) {
      assertTrue(part.hasETag(),
          "every Part must carry an eTag so the OM eTag-based validator path is "
              + "taken (load-bearing for native eTag-less Complete)");
      assertEquals(part.getPartName(), part.getETag(),
          "partName and eTag must mirror the same supplied identifier");
    }
    assertEquals("part-identifier-1", parts.get(0).getPartName());
    assertEquals("part-identifier-1", parts.get(0).getETag());
    assertEquals(1, parts.get(0).getPartNumber());
    assertEquals("part-identifier-2", parts.get(1).getETag());
    assertEquals(2, parts.get(1).getPartNumber());
  }
}
