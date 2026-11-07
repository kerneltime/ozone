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

package org.apache.hadoop.ozone.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.s3sts.S3STSConfigKeys;
import org.apache.ozone.test.ClusterForTests;
import org.apache.ozone.test.GenericTestUtils.PortAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.w3c.dom.Element;

/**
 * Checks that the STS endpoint of the S3 Gateway answers a request it refuses before any endpoint runs in the STS
 * error format, from which STS clients read the error code.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestS3STSErrorResponses extends ClusterForTests<MiniOzoneCluster> {

  private final String stsAddress = PortAllocator.localhostWithFreePort();

  @BeforeAll
  void beforeAll() throws Exception {
    startCluster();
  }

  @AfterAll
  void afterAll() {
    shutdownCluster();
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder()
        .addService(new S3GatewayService(conf -> {
          conf.setBoolean(S3STSConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY, true);
          conf.set(S3STSConfigKeys.OZONE_S3G_STS_HTTP_ADDRESS_KEY, stsAddress);
        }, null))
        .build();
  }

  @Test
  void aRepeatedParameterIsRefusedInTheStsErrorFormat() throws Exception {
    URL url = new URL("http://" + stsAddress + "/?Action=AssumeRole&Action=GetCallerIdentity&Version=2011-06-15");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    try {
      assertEquals(400, connection.getResponseCode());
      Element response;
      try (InputStream body = connection.getErrorStream()) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        response = factory.newDocumentBuilder().parse(body).getDocumentElement();
      }

      assertEquals("ErrorResponse", response.getTagName());
      assertEquals("Sender", text(response, "Type"));
      assertEquals("ValidationError", text(response, "Code"));
      assertEquals("The query parameter Action is repeated.", text(response, "Message"));
    } finally {
      connection.disconnect();
    }
  }

  private static String text(Element parent, String tagName) {
    return parent.getElementsByTagName(tagName).item(0).getTextContent();
  }
}
