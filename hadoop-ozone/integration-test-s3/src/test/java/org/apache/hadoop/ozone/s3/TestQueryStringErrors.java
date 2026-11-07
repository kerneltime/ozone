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

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
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
 * Checks that each listener of the S3 Gateway refuses a repeated query parameter, before any endpoint runs, in its own
 * error format, from which its clients read the error code.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestQueryStringErrors extends ClusterForTests<MiniOzoneCluster> {

  private final String stsAddress = PortAllocator.localhostWithFreePort();
  private final S3GatewayService gateway = new S3GatewayService(conf -> {
    conf.setBoolean(S3STSConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY, true);
    conf.set(S3STSConfigKeys.OZONE_S3G_STS_HTTP_ADDRESS_KEY, stsAddress);
  }, OZONE_S3G_HTTP_ADDRESS_KEY);

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
        .addService(gateway)
        .build();
  }

  /**
   * The S3 application asks CDI for its QueryStringFilter. Only the S3 filter may match: if the STS subclass matched
   * too, Jersey could take either one, and the HTTP checks below could pass by chance.
   */
  @Test
  void theS3ApplicationResolvesTheS3QueryStringFilter() {
    BeanManager beanManager = gateway.getGateway().getHttpServer().getBeanManager();

    assertThat(beanManager.getBeans(QueryStringFilter.class)).extracting(Bean::getBeanClass)
        .containsExactly(QueryStringFilter.class);
  }

  @Test
  void theS3ListenerRefusesARepeatedParameterInTheS3ErrorFormat() throws Exception {
    // The gateway publishes its address under this host's name, which need not be reachable from the host itself, and
    // listens on every interface.
    String address = getCluster().getConf().get(OZONE_S3G_HTTP_ADDRESS_KEY);
    Element error = refusal("http://127.0.0.1" + address.substring(address.lastIndexOf(':')) + "/?prefix=a&prefix=b");

    assertEquals("Error", error.getTagName());
    assertEquals("InvalidArgument", text(error, "Code"));
    assertEquals("The query parameter prefix is repeated.", text(error, "Message"));
  }

  @Test
  void theStsListenerRefusesARepeatedParameterInTheStsErrorFormat() throws Exception {
    Element response =
        refusal("http://" + stsAddress + "/?Action=AssumeRole&Action=GetCallerIdentity&Version=2011-06-15");

    assertEquals("ErrorResponse", response.getTagName());
    assertEquals("Sender", text(response, "Type"));
    assertEquals("ValidationError", text(response, "Code"));
    assertEquals("The query parameter Action is repeated.", text(response, "Message"));
  }

  /** GETs {@code url}, expects a 400, and returns the root element of the error document. */
  private static Element refusal(String url) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    try {
      assertEquals(400, connection.getResponseCode());
      try (InputStream body = connection.getErrorStream()) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        return factory.newDocumentBuilder().parse(body).getDocumentElement();
      }
    } finally {
      connection.disconnect();
    }
  }

  private static String text(Element parent, String tagName) {
    return parent.getElementsByTagName(tagName).item(0).getTextContent();
  }
}
