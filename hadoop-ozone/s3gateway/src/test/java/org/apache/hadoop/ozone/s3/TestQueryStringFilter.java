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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.ozone.s3.signature.StringToSignProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link QueryStringFilter}.
 */
class TestQueryStringFilter {

  private static final URI BASE_URI = URI.create("http://s3g.example.com:9878/");

  /** SigV4 signs a raw '+' as a literal plus, so the endpoints must read one, not a space. */
  @Test
  void rawPlusReachesTheEndpointsAsALiteralPlus() throws Exception {
    ContainerRequestContext context = request("list-type=2&prefix=a+b");

    new QueryStringFilter().filter(context);

    URI rewritten = rewrittenRequestUri(context);
    assertEquals("http://s3g.example.com:9878/bucket?list-type=2&prefix=a%2Bb", rewritten.toString());
    assertEquals("a+b", endpointValue(rewritten, "prefix"));
  }

  /** An encoded plus and an encoded space already reach the endpoints as they are signed. */
  @ParameterizedTest
  @CsvSource({"prefix=a%2Bb, a+b", "prefix=a%20b, a b"})
  void encodedPlusAndSpaceAreLeftAsSent(String query, String prefix) throws Exception {
    ContainerRequestContext context = request(query);

    new QueryStringFilter().filter(context);

    verify(context, never()).setRequestUri(any(URI.class), any(URI.class));
    assertEquals(prefix, endpointValue(context.getUriInfo().getRequestUri(), "prefix"));
  }

  /** A signature over either spelling of a plus therefore runs the request it signed, whichever spelling is sent. */
  @Test
  void bothSpellingsOfAPlusSignAndRunTheSameRequest() throws Exception {
    ContainerRequestContext raw = request("prefix=a+b");
    ContainerRequestContext encoded = request("prefix=a%2Bb");
    new QueryStringFilter().filter(raw);
    new QueryStringFilter().filter(encoded);

    assertEquals(canonicalQuery("prefix=a%2Bb"), canonicalQuery("prefix=a+b"));
    assertEquals(endpointValue(encoded.getUriInfo().getRequestUri(), "prefix"),
        endpointValue(rewrittenRequestUri(raw), "prefix"));
  }

  private static ContainerRequestContext request(String rawQuery) {
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getBaseUri()).thenReturn(BASE_URI);
    when(uriInfo.getRequestUri()).thenReturn(URI.create(BASE_URI + "bucket?" + rawQuery));
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    when(context.getUriInfo()).thenReturn(uriInfo);
    return context;
  }

  private static URI rewrittenRequestUri(ContainerRequestContext context) {
    ArgumentCaptor<URI> requestUri = ArgumentCaptor.forClass(URI.class);
    verify(context).setRequestUri(any(URI.class), requestUri.capture());
    return requestUri.getValue();
  }

  /** The value an endpoint reads: JAX-RS decodes a query parameter as a form value, so '+' becomes a space. */
  private static String endpointValue(URI requestUri, String name) {
    return Arrays.stream(requestUri.getRawQuery().split("&"))
        .filter(parameter -> parameter.startsWith(name + "="))
        .map(parameter -> URLDecoder.decode(parameter.substring(name.length() + 1), UTF_8))
        .findFirst().orElse(null);
  }

  private static String canonicalQuery(String rawQuery) throws Exception {
    return StringToSignProducer.buildCanonicalRequest("GET", "/bucket", "host",
        Collections.singletonMap("host", "s3g.example.com:9878"), rawQuery, "UNSIGNED-PAYLOAD")
        .split("\n")[2];
  }
}
