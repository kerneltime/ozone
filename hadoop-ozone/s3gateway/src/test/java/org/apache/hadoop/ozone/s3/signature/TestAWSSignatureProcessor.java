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

package org.apache.hadoop.ozone.s3.signature;

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.DATE_FORMATTER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_UNSIGNED_PAYLOAD_TRAILER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.UNSIGNED_PAYLOAD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for {@link AWSSignatureProcessor}.
 */
public class TestAWSSignatureProcessor {

  @Test
  public void testLowerCaseHeaderMapRemovesKeysCaseInsensitively() {
    AWSSignatureProcessor.LowerCaseKeyStringMap headers =
        new AWSSignatureProcessor.LowerCaseKeyStringMap();
    headers.put("Authorization", "AWS4-HMAC-SHA256 value");

    assertEquals("AWS4-HMAC-SHA256 value",
        headers.remove("AUTHORIZATION"));
    assertFalse(headers.containsKey("authorization"));
  }

  @Test
  public void testRestoreEmptyContentType() {
    MultivaluedMap<String, String> rawHeaders = new MultivaluedHashMap<>();
    rawHeaders.putSingle(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE, "");

    AWSSignatureProcessor.LowerCaseKeyStringMap headers =
        AWSSignatureProcessor.LowerCaseKeyStringMap.fromHeaderMap(rawHeaders);

    assertTrue(headers.containsKey(HeaderPreprocessor.CONTENT_TYPE));
    assertEquals("", headers.get(HeaderPreprocessor.CONTENT_TYPE));
  }

  @Test
  public void testRepeatedHeaderValuesAreJoinedWithCommas() {
    MultivaluedMap<String, String> rawHeaders = new MultivaluedHashMap<>();
    rawHeaders.addAll("My-Header1", "value4", "value1", "value3", "value2");

    AWSSignatureProcessor.LowerCaseKeyStringMap headers =
        AWSSignatureProcessor.LowerCaseKeyStringMap.fromHeaderMap(rawHeaders);

    assertEquals("value4,value1,value3,value2", headers.get("my-header1"));
  }

  /**
   * S3 decodes the path and percent-encodes it once: every octet but the unreserved ones and '/', in upper-case hex.
   * The first seven canonical paths are the ones S3 used for these paths; the last three paths are already canonical.
   */
  @ParameterizedTest
  @CsvSource({
      "/B/logs/%2Fa.txt, /B/logs//a.txt",
      "/B/logs/%2Fb.txt, /B/logs//b.txt",
      "/B/logs//c.txt, /B/logs//c.txt",
      "/B/logs/%7Ed.txt, /B/logs/~d.txt",
      "/B/logs/%7Ee.txt, /B/logs/~e.txt",
      "/B/logs/a%2bf.txt, /B/logs/a%2Bf.txt",
      "/B/logs/a%2bg.txt, /B/logs/a%2Bg.txt",
      "/b/k, /b/k",
      "/b/a%20b, /b/a%20b",
      "/b/caf%C3%A9, /b/caf%C3%A9"
  })
  public void testCanonicalPathIsTheDecodedPathEncodedOnce(String path, String canonicalPath) throws Exception {
    SignatureInfo signatureInfo = parseSignature("http://s3g.example.com:9878" + path, UNSIGNED_PAYLOAD);

    String canonicalRequest = StringToSignProducer.buildCanonicalRequest("GET", signatureInfo.getUnfilteredURI(),
        "host", Collections.singletonMap("host", "s3g.example.com:9878"), null, UNSIGNED_PAYLOAD);

    assertEquals(canonicalPath, canonicalRequest.split("\n")[1]);
  }

  @Test
  public void testStreamingUnsignedPayloadTrailerIsThePayloadHash() throws Exception {
    SignatureInfo signatureInfo = parseSignature("http://s3g.example.com:9878/bucket/key",
        STREAMING_UNSIGNED_PAYLOAD_TRAILER);

    assertEquals(STREAMING_UNSIGNED_PAYLOAD_TRAILER, signatureInfo.getPayloadHash());
  }

  @Test
  public void testS3ExpressPayloadHashIsTheContentSha256Header() throws Exception {
    // ListDirectoryBuckets is signed for the s3express service, which takes the payload line from the header, as s3.
    SignatureInfo signatureInfo = parseSignature("http://s3g.example.com:9878/?max-directory-buckets=2", "s3express",
        UNSIGNED_PAYLOAD);

    assertEquals(UNSIGNED_PAYLOAD, signatureInfo.getPayloadHash());
  }

  /** Parses a header-signed request sent now to {@code requestUri}, as the gateway receives it. */
  private static SignatureInfo parseSignature(String requestUri, String contentSha256) throws Exception {
    return parseSignature(requestUri, "s3", contentSha256);
  }

  /** Parses a request with an empty body, signed now for {@code service}, as the gateway receives it. */
  private static SignatureInfo parseSignature(String requestUri, String service, String contentSha256)
      throws Exception {
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    String dateTime = StringToSignProducer.TIME_FORMATTER.format(now);
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.putSingle("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"
        + DATE_FORMATTER.format(now) + "/us-east-1/" + service + "/aws4_request, "
        + "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
        + "Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024");
    headers.putSingle("Host", "s3g.example.com:9878");
    headers.putSingle("X-Amz-Content-Sha256", contentSha256);
    headers.putSingle("X-Amz-Date", dateTime);
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getRequestUri()).thenReturn(new URI(requestUri));
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    when(context.getUriInfo()).thenReturn(uriInfo);
    when(context.getHeaders()).thenReturn(headers);
    when(context.getMethod()).thenReturn("GET");
    when(context.getEntityStream()).thenReturn(new ByteArrayInputStream(new byte[0]));

    AWSSignatureProcessor processor = new AWSSignatureProcessor();
    processor.setContext(context);
    return processor.parseSignature();
  }

  @Test
  public void testOutOfRangeExpiresPreSignedUrlReturns403() throws Exception {
    // A pre-signed URL whose X-Amz-Expires is out of range must be rejected
    // with 403 AccessDenied, not 400 MalformedHeader.
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
    queryParams.putSingle("X-Amz-Credential",
        "AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request");
    queryParams.putSingle("X-Amz-Date", "20130524T000000Z");
    queryParams.putSingle("X-Amz-Expires", "604801");
    queryParams.putSingle("X-Amz-SignedHeaders", "host");
    queryParams.putSingle("X-Amz-Signature",
        "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404");

    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getQueryParameters()).thenReturn(queryParams);
    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    when(uriInfo.getPath()).thenReturn("key");
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    when(context.getUriInfo()).thenReturn(uriInfo);
    when(context.getHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(context.getMethod()).thenReturn("GET");

    AWSSignatureProcessor processor = new AWSSignatureProcessor();
    processor.setContext(context);

    assertErrorResponse(S3ErrorTable.ACCESS_DENIED, processor::parseSignature);
  }
}
