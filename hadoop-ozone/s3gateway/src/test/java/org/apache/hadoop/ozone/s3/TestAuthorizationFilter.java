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

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_CREDENTIAL_DATE;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.PAYLOAD_TOO_LARGE;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.REQUEST_TIME_TOO_SKEWED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import static org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor.DATE_FORMATTER;
import static org.apache.hadoop.ozone.s3.signature.SignatureParser.AUTHORIZATION_HEADER;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.CONTENT_MD5;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.CONTENT_TYPE;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.HOST_HEADER;
import static org.apache.hadoop.ozone.s3.signature.StringToSignProducer.X_AMAZ_DATE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.apache.hadoop.ozone.s3sts.S3STSConfigKeys.OZONE_S3G_STS_PAYLOAD_HASH_MAX_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.signature.SignatureTestUtils;
import org.apache.hadoop.ozone.s3.signature.StringToSignProducer;
import org.apache.kerby.util.Hex;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This class test string to sign generation.
 */
public class TestAuthorizationFilter {

  private AuthorizationFilter authorizationFilter = new AuthorizationFilter();
  private final HttpServletRequest servletRequest = mock(HttpServletRequest.class);

  private static final String DATETIME = StringToSignProducer.TIME_FORMATTER.
      format(LocalDateTime.now(ZoneOffset.UTC));

  private static final String CURDATE =
      DATE_FORMATTER.format(LocalDate.now(ZoneOffset.UTC));
  private static final String MISMATCHED_CREDENTIAL_DATE =
      DATE_FORMATTER.format(LocalDate.now(ZoneOffset.UTC).minusDays(1));

  private static final String STATIC_ACCESS_KEY = "STATICACCESSKEY1";
  private static final String STATIC_SECRET = "static-secret-value";
  private static final String SCOPE = CURDATE + "/us-east-1/s3/aws4_request";
  private static final String PRESIGNED_QUERY = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential="
      + "%s%%2F" + CURDATE + "%%2Fus-east-1%%2Fs3%%2Faws4_request&X-Amz-Date=" + DATETIME
      + "&X-Amz-Expires=300&X-Amz-SignedHeaders=host";

  @TempDir
  private Path tempDir;

  private static Stream<Arguments>testAuthFilterFailuresInput() {
    String staleCredentialDate = "20190221";
    String staleDateTime = staleCredentialDate + "T002037Z";
    return Stream.of(
        arguments(
            "GET",
            "AWS4-HMAC-SHA256 Credential=testuser1/" + staleCredentialDate
                + "/us-west-1/s3/aws4_request, SignedHeaders=content-md5;host;"
                + "x-amz-content-sha256;x-amz-date, "
                + "Signature"
                + "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47"
                + "65f46a14cd745ad",
            "Zi68x2nPDDXv5qfDC+ZWTg==",
            "s3g:9878",
            "e2bd43f11c97cde3465e0e8d1aad77af7ec7aa2ed8e213cd0e24"
                + "1e28375860c6",
            staleDateTime,
            "",
            "/",
            HTTP_FORBIDDEN,
            REQUEST_TIME_TOO_SKEWED.getErrorMessage()
        ),
        arguments(
            "GET",
            "AWS4-HMAC-SHA256 "
                + "Credential=AKIDEXAMPLE/" + staleCredentialDate
                + "/us-east-1/iam/aws4_request,"
                + " SignedHeaders=content-type;host;x-amz-date, "
                + "Signature="
                + "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400"
                + "e06b5924a6f2b5d7",
            "",
            "iam.amazonaws.com",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            staleDateTime,
            "application/x-www-form-urlencoded; charset=utf-8",
            "",
            HTTP_FORBIDDEN,
            REQUEST_TIME_TOO_SKEWED.getErrorMessage()
        ),
        arguments(
            "GET",
            "AWS4-HMAC-SHA256 Credential=testuser1/" + MISMATCHED_CREDENTIAL_DATE
                + "/us-east-1/s3/aws4_request, SignedHeaders=content-md5;host;"
                + "x-amz-content-sha256;x-amz-date, "
                + "Signature"
                + "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47"
                + "65f46a14cd745ad",
            "Zi68x2nPDDXv5qfDC+ZWTg==",
            "s3g:9878",
            "e2bd43f11c97cde3465e0e8d1aad77af7ec7aa2ed8e213cd0e24"
                + "1e28375860c6",
            DATETIME,
            "",
            "/",
            HTTP_BAD_REQUEST,
            MALFORMED_CREDENTIAL_DATE.getErrorMessage()
        ),
        arguments(null, null, null, null, null, null, null, null,
            HTTP_FORBIDDEN,
            S3_AUTHINFO_CREATION_ERROR.getErrorMessage()),
        arguments(null, "", null, null, null, null, null, null,
            HTTP_FORBIDDEN,
            S3_AUTHINFO_CREATION_ERROR.getErrorMessage()),
        // AWS V2 signature
        arguments(
            "GET",
            "AWS AKIDEXAMPLE:St7bHPOdkmsX/GITGe98rOQiUCg=",
            "",
            "s3g:9878",
            "",
            "Wed, 22 Mar 2023 17:00:06 +0000",
            "application/octet-stream",
            "/",
            HTTP_FORBIDDEN,
            S3_AUTHINFO_CREATION_ERROR.getErrorMessage()
        ),
        // Too huge payload for signature V4 of STS request
        arguments(
            "POST",
            "AWS4-HMAC-SHA256 Credential=testuser1/" + CURDATE +
            "/us-east-1/sts/aws4_request, " +
                "SignedHeaders=content-type;host;" +
                "x-amz-date, " +
                "Signature" +
                "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47" +
                "65f46a14cd745ad",
            "Content-SHA",
            "s3g:9880",
            "Content-SHA",
            DATETIME,
            "application/x-www-form-urlencoded; charset=utf-8",
            "/",
            HTTP_BAD_REQUEST,
            PAYLOAD_TOO_LARGE.getErrorMessage()
        )
    );
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  @ParameterizedTest
  @MethodSource("testAuthFilterFailuresInput")
  void testAuthFilterFailures(
      String method, String authHeader, String contentMd5,
      String host, String amzContentSha256, String date, String contentType,
      String path, int expectedStatus, String expectedErrorMsg
  ) throws Exception {
    try {
      ContainerRequestContext context = setupContext(method, authHeader,
          contentMd5, host, amzContentSha256, date, contentType, path);

      byte[] payloadBytes = new byte[OZONE_S3G_STS_PAYLOAD_HASH_MAX_VALUE + 1];
      InputStream payLoadStream = new ByteArrayInputStream(payloadBytes);
      when(context.getEntityStream()).thenReturn(payLoadStream);

      AWSSignatureProcessor awsSignatureProcessor = new AWSSignatureProcessor();
      awsSignatureProcessor.setContext(context);

      SignatureInfo signatureInfo = new SignatureInfo();

      authorizationFilter.setSignatureParser(awsSignatureProcessor);
      authorizationFilter.setSignatureInfo(signatureInfo);

      authorizationFilter.filter(context);
      if ("".equals(authHeader)) {
        fail("Empty AuthHeader must fail");
      }
    } catch (WebApplicationException ex) {
      assertEquals(expectedStatus, ex.getResponse().getStatus());
      assertEquals(expectedErrorMsg, ex.getMessage());
    }
  }

  private static Stream<Arguments>testAuthFilterInput() {
    return Stream.of(
        // Path style URI
        arguments(
            "GET",
            "AWS4-HMAC-SHA256 Credential=testuser1/" + CURDATE +
                "/us-east-1/s3/aws4_request, " +
                "SignedHeaders=host;x-amz-content-sha256;" +
                "x-amz-date, " +
                "Signature" +
                "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47" +
                "65f46a14cd745ad",
            "Content-SHA",
            "s3g:9878",
            "Content-SHA",
            DATETIME,
            "",
            "/bucket1/key1"
        ),
        // Virtual style URI
        arguments(
            "GET",
            "AWS4-HMAC-SHA256 Credential=testuser1/" + CURDATE +
                "/us-east-1/s3/aws4_request, " +
                "SignedHeaders=host;x-amz-content-sha256;" +
                "x-amz-date, " +
                "Signature" +
                "=56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf47" +
                "65f46a14cd745ad",
            "Content-SHA",
            "bucket1.s3g.internal:9878",
            "Content-SHA",
            DATETIME,
            "",
            "/key1"
        )
    );
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  @ParameterizedTest
  @MethodSource("testAuthFilterInput")
  void testAuthFilter(
      String method, String authHeader, String contentMd5,
      String host, String amzContentSha256, String date, String contentType,
      String path
  ) throws Exception {
    ContainerRequestContext context = setupContext(method, authHeader,
        contentMd5, host, amzContentSha256, date, contentType, path);

    AWSSignatureProcessor awsSignatureProcessor = new AWSSignatureProcessor();
    awsSignatureProcessor.setContext(context);

    SignatureInfo signatureInfo = new SignatureInfo();

    authorizationFilter.setSignatureParser(awsSignatureProcessor);
    authorizationFilter.setSignatureInfo(signatureInfo);

    authorizationFilter.filter(context);

    if (path.startsWith("/secret")) {
      assertNull(authorizationFilter.getSignatureInfo().getUnfilteredURI());

      assertNull(authorizationFilter.getSignatureInfo().getStringToSign());
    } else {
      String canonicalRequest = method + "\n"
          + path + "\n"
          + "\n"
          + "host:" + host + "\nx-amz-content-sha256:" + amzContentSha256 +
          "\n"
          + "x-amz-date:" + DATETIME + "\n"
          + "\n"
          + "host;x-amz-content-sha256;x-amz-date\n"
          + amzContentSha256;

      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(canonicalRequest.getBytes(StandardCharsets.UTF_8));

      String expectedStrToSign = "AWS4-HMAC-SHA256\n"
          + DATETIME + "\n"
          + CURDATE + "/us-east-1/s3/aws4_request\n"
          + Hex.encode(md.digest()).toLowerCase();

      assertEquals(path,
          authorizationFilter.getSignatureInfo().getUnfilteredURI(),
          "Unfiltered URI is not preserved");

      assertEquals(expectedStrToSign,
          authorizationFilter.getSignatureInfo().getStringToSign(),
          "String to sign is invalid");
    }
  }

  @Test
  void staticCredentialsAcceptACorrectSignature() throws Exception {
    try (StaticCredentialStore store = staticCredentials()) {
      SignatureInfo signatureInfo = filter(store, headerSignedRequest(STATIC_ACCESS_KEY, STATIC_SECRET));

      // The chunk signatures of a streaming upload are verified with this key.
      assertArrayEquals(SignatureTestUtils.signingKey(STATIC_SECRET, CURDATE, "us-east-1", "s3"),
          signatureInfo.getSigningKey());
    }
  }

  @Test
  void staticCredentialsRefuseAWrongSecret() throws Exception {
    try (StaticCredentialStore store = staticCredentials()) {
      assertRefused(store, headerSignedRequest(STATIC_ACCESS_KEY, "wrong-secret"),
          S3ErrorTable.SIGNATURE_DOES_NOT_MATCH);
    }
  }

  @Test
  void staticCredentialsRefuseAnUnknownAccessKey() throws Exception {
    try (StaticCredentialStore store = staticCredentials()) {
      assertRefused(store, headerSignedRequest("UNKNOWNACCESSKEY", STATIC_SECRET), S3ErrorTable.INVALID_ACCESS_KEY_ID);
    }
  }

  @Test
  void staticCredentialsVerifyPresignedRequests() throws Exception {
    try (StaticCredentialStore store = staticCredentials()) {
      filter(store, presignedRequest(STATIC_ACCESS_KEY, STATIC_SECRET));
      assertRefused(store, presignedRequest(STATIC_ACCESS_KEY, "wrong-secret"), S3ErrorTable.SIGNATURE_DOES_NOT_MATCH);
      assertRefused(store, presignedRequest("UNKNOWNACCESSKEY", STATIC_SECRET), S3ErrorTable.INVALID_ACCESS_KEY_ID);
    }
  }

  /**
   * A refusal is audited with the address the request came from. The client_ip header is only set by a later filter,
   * so here it is absent or whatever the caller sent.
   */
  @Test
  void staticCredentialsAuditARefusalWithTheAddressItCameFrom() throws Exception {
    when(servletRequest.getRemoteAddr()).thenReturn("192.0.2.10");
    ContainerRequestContext context = headerSignedRequest(STATIC_ACCESS_KEY, "wrong-secret");
    when(context.getHeaderString(ClientIpFilter.CLIENT_IP_HEADER)).thenReturn("203.0.113.66");
    LogCapturer audit = LogCapturer.log4j2("S3GAudit");
    audit.clearOutput();

    try (StaticCredentialStore store = staticCredentials()) {
      assertThrows(WebApplicationException.class, () -> filter(store, context));
    }

    assertThat(audit.getOutput()).contains("ip=192.0.2.10").doesNotContain("203.0.113.66");
  }

  private StaticCredentialStore staticCredentials() throws Exception {
    Path file = tempDir.resolve("credentials");
    Files.write(file, Collections.singletonList(STATIC_ACCESS_KEY + "=" + STATIC_SECRET), UTF_8);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY, file.toString());
    return StaticCredentialStore.load(conf);
  }

  /** Runs the filter with the given static credentials and returns what it parsed. */
  private SignatureInfo filter(StaticCredentialStore store, ContainerRequestContext context) throws Exception {
    AWSSignatureProcessor awsSignatureProcessor = new AWSSignatureProcessor();
    awsSignatureProcessor.setContext(context);
    SignatureInfo signatureInfo = new SignatureInfo();
    authorizationFilter.setSignatureParser(awsSignatureProcessor);
    authorizationFilter.setSignatureInfo(signatureInfo);
    authorizationFilter.setStaticCredentials(store);
    authorizationFilter.setHttpServletRequest(servletRequest);
    authorizationFilter.filter(context);
    return signatureInfo;
  }

  /** The request gets a 403 with the expected code, and the audit log records it without the signature. */
  private void assertRefused(StaticCredentialStore store, ContainerRequestContext context, S3ErrorTable error)
      throws Exception {
    String signature = context.getUriInfo().getQueryParameters().containsKey("X-Amz-Signature")
        ? context.getUriInfo().getQueryParameters().getFirst("X-Amz-Signature")
        : context.getHeaderString(AUTHORIZATION_HEADER).replaceAll(".*Signature=", "");
    LogCapturer audit = LogCapturer.log4j2("S3GAudit");
    audit.clearOutput();

    WebApplicationException ex = assertThrows(WebApplicationException.class, () -> filter(store, context));

    assertEquals(HTTP_FORBIDDEN, ex.getResponse().getStatus());
    assertThat((String) ex.getResponse().getEntity()).contains("<Code>" + error.getCode() + "</Code>");
    assertThat(audit.getOutput()).contains("AUTH(GET /bucket1/key1)", "ret=FAILURE")
        .doesNotContain(signature, STATIC_SECRET);
  }

  /** A GET of /bucket1/key1 with header auth, signed as SigV4 specifies; the canonical request is written out here. */
  private ContainerRequestContext headerSignedRequest(String accessKey, String secret) throws Exception {
    String canonicalRequest = "GET\n/bucket1/key1\n\nhost:s3g:9878\nx-amz-content-sha256:UNSIGNED-PAYLOAD\n"
        + "x-amz-date:" + DATETIME + "\n\nhost;x-amz-content-sha256;x-amz-date\nUNSIGNED-PAYLOAD";
    String authHeader = "AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + SCOPE
        + ", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=" + sign(secret, canonicalRequest);
    ContainerRequestContext context = setupContext("GET", authHeader, null, "s3g:9878", "UNSIGNED-PAYLOAD",
        DATETIME, null, "/bucket1/key1");
    when(context.getUriInfo().getPath()).thenReturn("bucket1/key1");
    return context;
  }

  /** A presigned GET of /bucket1/key1, signed as SigV4 specifies. */
  private ContainerRequestContext presignedRequest(String accessKey, String secret) throws Exception {
    String query = String.format(PRESIGNED_QUERY, accessKey);
    String signature = sign(secret, "GET\n/bucket1/key1\n" + query + "\nhost:s3g:9878\n\nhost\nUNSIGNED-PAYLOAD");
    MultivaluedMap<String, String> queryMap = new MultivaluedHashMap<>();
    queryMap.putSingle("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
    queryMap.putSingle("X-Amz-Credential", accessKey + "/" + SCOPE);
    queryMap.putSingle("X-Amz-Date", DATETIME);
    queryMap.putSingle("X-Amz-Expires", "300");
    queryMap.putSingle("X-Amz-SignedHeaders", "host");
    queryMap.putSingle("X-Amz-Signature", signature);
    MultivaluedMap<String, String> headerMap = new MultivaluedHashMap<>();
    headerMap.putSingle(HOST_HEADER, "s3g:9878");
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getQueryParameters()).thenReturn(queryMap);
    when(uriInfo.getPathParameters()).thenReturn(new MultivaluedHashMap<>());
    when(uriInfo.getRequestUri()).thenReturn(
        new URI("http://s3g:9878/bucket1/key1?" + query + "&X-Amz-Signature=" + signature));
    when(uriInfo.getPath()).thenReturn("bucket1/key1");
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    when(context.getMethod()).thenReturn("GET");
    when(context.getUriInfo()).thenReturn(uriInfo);
    when(context.getHeaders()).thenReturn(headerMap);
    return context;
  }

  private static String sign(String secret, String canonicalRequest) {
    String stringToSign = "AWS4-HMAC-SHA256\n" + DATETIME + "\n" + SCOPE + "\n"
        + DigestUtils.sha256Hex(canonicalRequest);
    return Hex.encode(SignatureTestUtils.hmac(SignatureTestUtils.signingKey(secret, CURDATE, "us-east-1", "s3"),
        stringToSign));
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private ContainerRequestContext setupContext(
      String method, String authHeader, String contentMd5,
      String host, String amzContentSha256, String date, String contentType,
      String path) throws URISyntaxException {
    MultivaluedMap<String, String> headerMap = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryMap = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> pathParamsMap = new MultivaluedHashMap<>();

    System.err.println("Testing: " + authHeader);
    headerMap.putSingle(AUTHORIZATION_HEADER, authHeader);
    headerMap.putSingle(CONTENT_MD5, contentMd5);
    headerMap.putSingle(HOST_HEADER, host);
    headerMap.putSingle(X_AMZ_CONTENT_SHA256, amzContentSha256);
    headerMap.putSingle(X_AMAZ_DATE, date);
    headerMap.putSingle(CONTENT_TYPE, contentType);

    UriInfo uriInfo = mock(UriInfo.class);
    ContainerRequestContext context = mock(
        ContainerRequestContext.class);
    when(uriInfo.getQueryParameters()).thenReturn(queryMap);
    when(uriInfo.getRequestUri()).thenReturn(
        new URI("http://" + host + path));

    when(context.getMethod()).thenReturn(method);
    when(context.getUriInfo()).thenReturn(uriInfo);
    when(context.getHeaders()).thenReturn(headerMap);
    when(context.getHeaderString(AUTHORIZATION_HEADER))
        .thenReturn(authHeader);
    when(context.getUriInfo().getQueryParameters())
        .thenReturn(queryMap);
    when(context.getUriInfo().getPathParameters())
        .thenReturn(pathParamsMap);

    return context;
  }

}
