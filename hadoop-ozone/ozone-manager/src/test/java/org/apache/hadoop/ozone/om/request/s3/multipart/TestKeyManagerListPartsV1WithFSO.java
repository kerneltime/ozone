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
import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Tests listParts for schemaVersion 1 uploads in FSO (FILE_SYSTEM_OPTIMIZED)
 * buckets. Inherits every assertion from {@link TestKeyManagerListPartsV1} and
 * overrides only the layout-specific scaffolding, so the same parity and
 * pagination checks run against the FSO part-name reconstruction path in
 * {@code KeyManagerImpl.getPartName}.
 */
public class TestKeyManagerListPartsV1WithFSO
    extends TestKeyManagerListPartsV1 {

  private String dirName = "a/b/c/";

  @Override
  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
      OMRequest initiateMPURequest) throws IOException {
    S3InitiateMultipartUploadRequest request =
        new S3InitiateMultipartUploadRequestWithFSO(initiateMPURequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

  @Override
  protected String getKeyName() {
    return dirName + UUID.randomUUID().toString();
  }

  @Override
  protected void createParentPath(String volumeName, String bucketName)
      throws Exception {
    OMRequestTestUtils.addParentsToDirTable(volumeName, bucketName, dirName,
        omMetadataManager);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
