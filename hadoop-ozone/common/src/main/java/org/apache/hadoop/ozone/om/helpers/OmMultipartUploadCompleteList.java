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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;

/**
 * This class represents multipart list, which is required for
 * CompleteMultipart upload request.
 */
public class OmMultipartUploadCompleteList {

  private final LinkedHashMap<Integer, String> multipartMap;

  /**
   * Construct OmMultipartUploadCompleteList which holds multipart map which
   * contains part number and part name.
   * @param partMap
   */
  public OmMultipartUploadCompleteList(Map<Integer, String> partMap) {
    this.multipartMap = new LinkedHashMap<>(partMap);
  }

  /**
   * Return multipartMap which is a map of part number and part name.
   * @return multipartMap
   */
  public Map<Integer, String> getMultipartMap() {
    return multipartMap;
  }

  /**
   * Construct Part list from the multipartMap.
   * @return List<Part>
   */
  public List<Part> getPartsList() {
    List<Part> partList = new ArrayList<>();
    // Each map value is the per-part identifier the caller supplied at Complete:
    // for an S3 client it is the part's MD5 eTag; for the native Ozone client
    // (which never computes an eTag) it is the part NAME. We populate BOTH the
    // proto partName and eTag fields from that single value. Two reasons:
    //   1. partName is a required proto field, so it must be set.
    //   2. CONTRACT (load-bearing for the HDDS-14661 eTag-less multipart path):
    //      the OM's CompleteMultipartUpload validator takes its eTag-based path
    //      only when EVERY Part has an eTag (Part::hasETag). Mirroring the value
    //      into eTag here keeps a native, eTag-less upload on that path, where it
    //      passes via the validator's "request eTag equals the stored part name"
    //      fallback. See S3MultipartUploadCompleteRequest#eTagBasedValidator.
    // Do NOT stop populating eTag here without updating that validator: a native
    // multipart Complete would then fail every part with INVALID_PART.
    multipartMap.forEach((partNumber, partIdentifier) -> partList.add(Part
        .newBuilder().setPartName(partIdentifier).setETag(partIdentifier)
        .setPartNumber(partNumber).build()));
    return partList;
  }
}
