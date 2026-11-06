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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_HEADER;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.REQUEST_TIME_TOO_SKEWED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import static org.apache.hadoop.ozone.s3.signature.SignatureProcessor.DATE_FORMATTER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.UNSIGNED_PAYLOAD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ozone.om.AWSV4AuthValidator;
import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor.LowerCaseKeyStringMap;
import org.apache.kerby.util.Hex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test string2sign creation.
 */
public class TestStringToSignProducer {

  private static final String DATETIME = StringToSignProducer.TIME_FORMATTER.
          format(LocalDateTime.now(ZoneOffset.UTC));

  private static final String SUITE_SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
  private static final String SUITE_DATETIME = "20150830T123600Z";
  private static final String SUITE_SCOPE = "20150830/us-east-1/service/aws4_request";
  private static final String S3_SCOPE = "20150830/us-east-1/s3/aws4_request";
  private static final String EMPTY_PAYLOAD_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  private static final String UNRESERVED = "-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  private static final String S3_BASE_HEADERS = "host:s3g.example.com:9878\nx-amz-content-sha256:UNSIGNED-PAYLOAD\n"
      + "x-amz-date:" + SUITE_DATETIME + "\n";
  private static final String S3_BASE_SIGNED = "host;x-amz-content-sha256;x-amz-date";

  @Test
  public void test() throws Exception {

    LowerCaseKeyStringMap headers = new LowerCaseKeyStringMap();
    headers.put("Content-Length", "123");
    headers.put("Host", "0.0.0.0:9878");
    headers.put("X-AMZ-Content-Sha256", "Content-SHA");
    headers.put("X-AMZ-Date", DATETIME);
    headers.put("Content-Type", "ozone/mpu");
    headers.put(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE, "streaming");

    String canonicalRequest = "GET\n"
        + "/buckets\n"
        + "\n"
        + "host:0.0.0.0:9878\nx-amz-content-sha256:Content-SHA\n"
        + "x-amz-date:" + DATETIME + "\ncontent-type:streaming\n"
        + "\n"
        + "host;x-amz-content-sha256;x-amz-date;content-type\n"
        + "Content-SHA";

    String credentialDate = DATETIME.substring(0, 8);
    String authHeader =
        "AWS4-HMAC-SHA256 Credential=AKIAJWFJK62WUTKNFJJA/" + credentialDate
            + "/us-east-1"
            + "/s3/aws4_request, "
            + "SignedHeaders=host;x-amz-content-sha256;x-amz-date;"
            + "content-type, "
            + "Signature"
            +
            "=db81b057718d7c1b3b8dffa29933099551c51d787b3b13b9e0f9ebed45982bf2";

    headers.put("Authorization",
        authHeader);

    final SignatureInfo signatureInfo =
        new AuthorizationV4HeaderParser(authHeader, DATETIME).parseSignature();
    signatureInfo.setPayloadHash("Content-SHA");
    signatureInfo.setUnfilteredURI("/buckets");

    headers.fixContentType();

    final String signatureBase =
        StringToSignProducer.createSignatureBase(
            signatureInfo,
            "GET",
            headers,
            null);

    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(canonicalRequest.getBytes(StandardCharsets.UTF_8));

    assertEquals("AWS4-HMAC-SHA256\n"
            + DATETIME + "\n"
            + credentialDate + "/us-east-1/s3/aws4_request\n"
            + Hex.encode(md.digest()).toLowerCase(),
        signatureBase, "String to sign is invalid");
  }

  private static Stream<Arguments> testUrlEncodeInCanonicalRequestInput() {
    return Stream.of(
        arguments("/bucket/a+b*c~d/foo bar", "/bucket/a%2Bb%2Ac~d/foo%20bar"),
        arguments("", ""),
        arguments("/", "/"),
        arguments("//", "//"),
        arguments("/bucket/", "/bucket/"),
        arguments("//bucket/key", "//bucket/key"),
        arguments("/bucket//key/", "/bucket//key/"),
        arguments("/bucket/a/.././b", "/bucket/a/.././b"),
        arguments("bucket/key", "bucket/key"),
        // The path is the one sent: %2F is an encoded slash in a key, %252F a literal "%2F". An encoded octet stays
        // encoded, with upper-case hex digits.
        arguments("/bucket/a%2Fb", "/bucket/a%2Fb"),
        arguments("/bucket/a%252Fb", "/bucket/a%252Fb"),
        arguments("/bucket/a%2fb", "/bucket/a%2Fb"),
        arguments("/bucket/%7Ekey", "/bucket/%7Ekey"),
        arguments("/bucket/雪/😀", "/bucket/%E9%9B%AA/%F0%9F%98%80"),
        arguments("/bucket/?#", "/bucket/%3F%23")
    );
  }

  @ParameterizedTest
  @MethodSource("testUrlEncodeInCanonicalRequestInput")
  public void testUrlEncodeInCanonicalRequest(String uri, String expectedCanonicalUri) {
    final Map<String, String> headers = new HashMap<>();
    headers.put("host", "example.com");
    headers.put("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    headers.put("x-amz-date", DATETIME);

    final String canonicalRequest = StringToSignProducer.buildCanonicalRequest(
        "GET", uri, "host;x-amz-content-sha256;x-amz-date",
        headers, "q%2B1*2~3=v%204*5%7E6", UNSIGNED_PAYLOAD);

    assertEquals(
        "GET\n"
            + expectedCanonicalUri + "\n"
            + "q%2B1%2A2~3=v%204%2A5~6\n"
            + "host:example.com\n"
            + "x-amz-content-sha256:" + UNSIGNED_PAYLOAD + "\n"
            + "x-amz-date:" + DATETIME + "\n"
            + "\n"
            + "host;x-amz-content-sha256;x-amz-date\n"
            + UNSIGNED_PAYLOAD,
        canonicalRequest);
  }

  /**
   * A client reaching the gateway over IPv6 signs the bracketed Host header as
   * it sent it, so the string to sign has to carry that value through
   * unchanged: splitting the address on its colons would produce a different
   * canonical request and reject every request. Nothing in the signing path
   * reads the Host today, so this pins that rather than guarding a change.
   */
  @ParameterizedTest
  @ValueSource(strings = {"[::1]:9878", "[2001:db8::1]:9878", "[::1]"})
  public void testIPv6HostInStringToSign(String host) throws Exception {
    String signedHeaders = "host;x-amz-content-sha256;x-amz-date";
    String credentialScope = DATETIME.substring(0, 8)
        + "/us-east-1/s3/aws4_request";
    String authHeader = "AWS4-HMAC-SHA256 Credential=ozone/" + credentialScope
        + ", SignedHeaders=" + signedHeaders
        + ", Signature=db81b057718d7c1b3b8"
        + "dffa29933099551c51d787b3b13b9e0f9ebed45982bf2";

    MultivaluedMap<String, String> headerMap = new MultivaluedHashMap<>();
    headerMap.putSingle("Authorization", authHeader);
    headerMap.putSingle("Host", host);
    headerMap.putSingle("X-Amz-Content-Sha256", "Content-SHA");
    headerMap.putSingle("X-Amz-Date", DATETIME);

    // Path style request: the bucket and key stay in the URI, as they do for a
    // client that cannot use virtual host style against an address literal.
    ContainerRequestContext context = setupContext(
        new URI("http://" + host + "/mybucket/mykey"),
        "GET",
        headerMap,
        new MultivaluedHashMap<>());
    SignatureInfo signatureInfo = new AuthorizationV4HeaderParser(
        authHeader, DATETIME).parseSignature();
    signatureInfo.setPayloadHash("Content-SHA");
    signatureInfo.setUnfilteredURI("/mybucket/mykey");

    String canonicalRequest = "GET\n"
        + "/mybucket/mykey\n"
        + "\n"
        + "host:" + host + "\n"
        + "x-amz-content-sha256:Content-SHA\n"
        + "x-amz-date:" + DATETIME + "\n"
        + "\n"
        + signedHeaders + "\n"
        + "Content-SHA";
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(canonicalRequest.getBytes(StandardCharsets.UTF_8));

    assertEquals("AWS4-HMAC-SHA256\n"
            + DATETIME + "\n"
            + credentialScope + "\n"
            + Hex.encode(md.digest()).toLowerCase(),
        StringToSignProducer.createSignatureBase(signatureInfo, context),
        "String to sign is invalid");
  }

  /**
   * Vectors from the AWS SigV4 signing test suite (aws-c-auth, "aws-signing-test-suite/v4"): the request as sent, the
   * published string to sign and the published signature. Each header line is a separate value, as Jersey hands
   * them over.
   */
  private static Stream<Arguments> awsSigningTestSuite() {
    return Stream.of(
        arguments("get-header-key-duplicate", "GET", "/",
            new String[] {"My-Header1", "value2", "My-Header1", "value2", "My-Header1", "value1"},
            "host;my-header1;x-amz-date", "dc7f04a3abfde8d472b0ab1a418b741b7c67174dad1551b4117b15527fbe966c",
            "c9d5ea9f3f72853aea855b47ea873832890dbdd183b4468f858259531a5138ea"),
        arguments("get-header-value-order", "GET", "/",
            new String[] {"My-Header1", "value4", "My-Header1", "value1", "My-Header1", "value3",
                "My-Header1", "value2"},
            "host;my-header1;x-amz-date", "31ce73cd3f3d9f66977ad3dd957dc47af14df92fcd8509f59b349e9137c58b86",
            "08c7e5a9acfcfeb3ab6b2185e75ce8b1deb5e634ec47601a50643f830c755c01"),
        arguments("get-header-value-trim", "GET", "/",
            new String[] {"My-Header1", " value1", "My-Header2", " \"a   b   c\""},
            "host;my-header1;my-header2;x-amz-date", "a726db9b0df21c14f559d0a978e563112acb1b9e05476f0a6a1c7d68f28605c7",
            "acc3ed3afb60bb290fc8d2dd0098b9911fcaa05412b367055dee359757a9c736"),
        arguments("post-header-value-case", "POST", "/", new String[] {"My-Header1", "VALUE1"},
            "host;my-header1;x-amz-date", "d51ced243e649e3de6ef63afbbdcbca03131a21a7103a1583706a64618606a93",
            "cdbc9802e29d2942e5e10b5bccfdd67c5f22c7c4e8ae67b53629efa58b974b7d"),
        arguments("get-vanilla-query-order-encoded", "GET", "/?Param-3=Value3&Param=Value2&%E1%88%B4=Value1",
            new String[0], "host;x-amz-date", "868294f5c38bd141c4972a373a76654f1418a8e4fc18b2e7903ae45e8ae0ec71",
            "371d3713e185cc334048618a97f809c9ffe339c62934c032af5a0e595648fcac"),
        arguments("get-vanilla-query-order-key-case", "GET", "/?Param2=value2&Param1=value1",
            new String[0], "host;x-amz-date", "816cd5b414d056048ba4f7c5386d6e0533120fb1fcfa93762cf0fc39e2cf19e0",
            "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"),
        arguments("get-vanilla-utf8-query", "GET", "/?%E1%88%B4=bar",
            new String[0], "host;x-amz-date", "eb30c5bed55734080471a834cc727ae56beb50e5f39d1bff6d0d38cb192a7073",
            "2cdec8eed098649ff3a119c94853b13c643bcf08f8b0a1d91e12c9027818dd04"),
        arguments("get-vanilla-query-unreserved", "GET", "/?" + UNRESERVED + "=" + UNRESERVED,
            new String[0], "host;x-amz-date", "c30d4703d9f799439be92736156d47ccfb2d879ddf56f5befa6d1d6aab979177",
            "9c3e54bfcdf0b19771a7f523ee5669cdf59bc7cc0884027167c21bb143a40197"),
        arguments("get-utf8", "GET", "/%E1%88%B4",
            new String[0], "host;x-amz-date", "2a0a97d02205e45ce2e994789806b19270cfbbb0921b278ccf58f5249ac42102",
            "8318018e0b0f223aa2bbf98705b62bb787dc9c0e678f255a891fd03141be5d85"),
        arguments("get-unreserved", "GET", "/" + UNRESERVED,
            new String[0], "host;x-amz-date", "6a968768eefaa713e2a6b16b589a8ea192661f098f37349f4e2c0082757446f9",
            "07ef7494c76fa4850883e2b006601f940f8a34d404d0cfa977f52a65bbf5f24f")
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("awsSigningTestSuite")
  void testAwsSigningTestSuite(String vector, String method, String target, String[] headerLines,
      String signedHeaders, String canonicalRequestHash, String signature) throws Exception {
    MultivaluedMap<String, String> headers = headers(headerLines);
    headers.add("Host", "example.amazonaws.com");
    headers.add("X-Amz-Date", SUITE_DATETIME);

    String stringToSign = stringToSign(method, "http://example.amazonaws.com" + target, headers, signedHeaders,
        SUITE_SCOPE, EMPTY_PAYLOAD_SHA256);

    assertEquals(String.join("\n", "AWS4-HMAC-SHA256", SUITE_DATETIME, SUITE_SCOPE, canonicalRequestHash),
        stringToSign);
    assertTrue(AWSV4AuthValidator.validateRequest(stringToSign, signature, SUITE_SECRET));
  }

  /**
   * The S3 inputs where the canonical request used to depart from SigV4, one case each and then all together. The
   * expected canonical request follows the SigV4 rules; each signature was computed independently from it.
   */
  private static Stream<Arguments> s3CanonicalisationInputs() {
    return Stream.of(
        // A mixed-case Content-Type value is signed exactly as sent.
        arguments("PUT", "/bucket/key", new String[] {"Content-Type", "text/plain; charset=UTF-8"},
            "PUT\n/bucket/key\n\ncontent-type:text/plain; charset=UTF-8\n" + S3_BASE_HEADERS
                + "\ncontent-type;" + S3_BASE_SIGNED + "\nUNSIGNED-PAYLOAD",
            "515b8d5c73498de276f36e849cfe1cdc4fa8010480844cd775e73435a421e294"),
        // Every value of a repeated query parameter is signed, sorted by value.
        arguments("GET", "/bucket?a=2&a=1", new String[0],
            "GET\n/bucket\na=1&a=2\n" + S3_BASE_HEADERS + "\n" + S3_BASE_SIGNED + "\nUNSIGNED-PAYLOAD",
            "8dce7f6df7578812f61aa80c9658397d32a0921597e383d57058830b40f48db9"),
        // An encoded slash in a key stays encoded: it is part of the key, not a path separator.
        arguments("GET", "/bucket/dir%2Ffile", new String[0],
            "GET\n/bucket/dir%2Ffile\n\n" + S3_BASE_HEADERS + "\n" + S3_BASE_SIGNED + "\nUNSIGNED-PAYLOAD",
            "c9b221067c9fcc72c7133b4d9a8bf5832b8ffc8fe12e9b030a2bd41adb819073"),
        // A raw "+" in a query value is a literal plus, which SigV4 encodes as %2B.
        arguments("GET", "/bucket?prefix=a+b&list-type=2", new String[0],
            "GET\n/bucket\nlist-type=2&prefix=a%2Bb\n" + S3_BASE_HEADERS + "\n" + S3_BASE_SIGNED
                + "\nUNSIGNED-PAYLOAD",
            "c3bc7cce13db1ae83fad2bdae3de1823b62d87a0e40d5867011d99c6343558d3"),
        arguments("PUT", "/bucket/dir%2Ffile?prefix=a+b&a=2&a=1",
            new String[] {"Content-Type", "text/plain; charset=UTF-8", "X-Amz-Meta-A", "one", "X-Amz-Meta-A", "two",
                "X-Amz-Meta-B", "x  y"},
            "PUT\n/bucket/dir%2Ffile\na=1&a=2&prefix=a%2Bb\ncontent-type:text/plain; charset=UTF-8\n" + S3_BASE_HEADERS
                + "x-amz-meta-a:one,two\nx-amz-meta-b:x y\n\ncontent-type;" + S3_BASE_SIGNED
                + ";x-amz-meta-a;x-amz-meta-b\nUNSIGNED-PAYLOAD",
            "c271d2770704fd9f886146d1e1fc305f6c22809972f0bdea6fbf0a5acc698eec")
    );
  }

  @ParameterizedTest
  @MethodSource("s3CanonicalisationInputs")
  void testS3CanonicalisationInputs(String method, String target, String[] headerLines, String canonicalRequest,
      String signature) throws Exception {
    MultivaluedMap<String, String> headers = headers(headerLines);
    headers.add("Host", "s3g.example.com:9878");
    headers.add("X-Amz-Content-Sha256", UNSIGNED_PAYLOAD);
    headers.add("X-Amz-Date", SUITE_DATETIME);
    String[] canonicalLines = canonicalRequest.split("\n");
    String signedHeaders = canonicalLines[canonicalLines.length - 2];

    String stringToSign = stringToSign(method, "http://s3g.example.com:9878" + target, headers, signedHeaders,
        S3_SCOPE, UNSIGNED_PAYLOAD);

    assertEquals(String.join("\n", "AWS4-HMAC-SHA256", SUITE_DATETIME, S3_SCOPE,
        DigestUtils.sha256Hex(canonicalRequest)), stringToSign);
    assertTrue(AWSV4AuthValidator.validateRequest(stringToSign, signature, SUITE_SECRET));
  }

  /** Header lines as Jersey hands them over: one value per line, under a case-insensitive name. */
  private static MultivaluedMap<String, String> headers(String[] nameValuePairs) {
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    for (int i = 0; i < nameValuePairs.length; i += 2) {
      headers.add(nameValuePairs[i], nameValuePairs[i + 1]);
    }
    return headers;
  }

  /** Builds the string to sign for a request the gateway received at {@code requestUri}, with the path as sent. */
  private String stringToSign(String method, String requestUri, MultivaluedMap<String, String> headers,
      String signedHeaders, String scope, String payloadHash) throws Exception {
    URI uri = new URI(requestUri);
    SignatureInfo signatureInfo = new SignatureInfo.Builder(SignatureInfo.Version.V4)
        .setAlgorithm("AWS4-HMAC-SHA256")
        .setDateTime(SUITE_DATETIME)
        .setCredentialScope(scope)
        .setSignedHeaders(signedHeaders)
        .setPayloadHash(payloadHash)
        .setUnfilteredURI(uri.getRawPath())
        .build();
    return StringToSignProducer.createSignatureBase(signatureInfo,
        setupContext(uri, method, headers, decodedQuery(uri.getRawQuery())));
  }

  /** Query parameters as Jersey decodes them for the resource methods, where "+" is a space. */
  private static MultivaluedMap<String, String> decodedQuery(String rawQuery) {
    MultivaluedMap<String, String> query = new MultivaluedHashMap<>();
    if (rawQuery != null) {
      for (String param : rawQuery.split("&")) {
        int separator = param.indexOf('=');
        String name = separator < 0 ? param : param.substring(0, separator);
        String value = separator < 0 ? "" : param.substring(separator + 1);
        query.add(URLDecoder.decode(name, StandardCharsets.UTF_8), URLDecoder.decode(value, StandardCharsets.UTF_8));
      }
    }
    return query;
  }

  private ContainerRequestContext setupContext(
      URI uri,
      String method,
      MultivaluedMap<String, String> headerMap,
      MultivaluedMap<String, String> queryMap) {
    ContainerRequestContext context =
        mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);

    when(uriInfo.getRequestUri()).thenReturn(uri);
    when(uriInfo.getQueryParameters()).thenReturn(queryMap);

    when(context.getUriInfo()).thenReturn(uriInfo);
    when(context.getMethod()).thenReturn(method);
    when(context.getHeaders()).thenReturn(headerMap);

    return context;
  }

  private static Stream<Arguments> testValidateRequestHeadersInput() {
    String authHeader = "AWS4-HMAC-SHA256 Credential=ozone/"
        + DATE_FORMATTER.format(LocalDate.now(ZoneOffset.UTC))
        + "/us-east-1/s3/aws4_request, "
        + "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date,"
        + " Signature=db81b057718d7c1b3b8"
        + "dffa29933099551c51d787b3b13b9e0f9ebed45982bf2";

    // Well-formed request headers
    MultivaluedMap<String, String> headersMap1 =
        new MultivaluedHashMap<>();
    headersMap1.putSingle("Authorization", authHeader);
    headersMap1.putSingle("Content-Type", "application/octet-stream");
    headersMap1.putSingle("Host", "0.0.0.0:9878");
    headersMap1.putSingle("X-Amz-Content-Sha256", "Content-SHA");
    headersMap1.putSingle("X-Amz-Date", DATETIME);
    //Missing X-Amz-Date Header
    MultivaluedMap<String, String> headersMap2 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap2.remove("X-Amz-Date");
    // Invalid X-Amz-Date format
    MultivaluedMap<String, String> headersMap3 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap3.remove("X-Amz-Date");
    headersMap3.putSingle("X-Amz-Date", LocalDateTime.now().toString());
    // Expired X-Amz-Date (more than 15 minutes in the past)
    MultivaluedMap<String, String> headersMap4 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap4.remove("X-Amz-Date");
    headersMap4.putSingle("X-Amz-Date", StringToSignProducer.TIME_FORMATTER.
        format(LocalDateTime.now(ZoneOffset.UTC).minusMinutes(16)));
    // X-Amz-Date too far in the future (more than 15 minutes)
    MultivaluedMap<String, String> headersMap5 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap5.remove("X-Amz-Date");
    headersMap5.putSingle("X-Amz-Date", StringToSignProducer.TIME_FORMATTER.
        format(LocalDateTime.now(ZoneOffset.UTC).plusMinutes(16)));
    // Missing X-Amz-Content-Sha256
    MultivaluedMap<String, String> headersMap6 =
        new MultivaluedHashMap<String, String>(headersMap1);
    headersMap6.remove("X-Amz-Content-Sha256");

    return Stream.of(
        arguments(headersMap1, "success"),
        arguments(headersMap2, MALFORMED_HEADER.getCode()),
        arguments(headersMap3, MALFORMED_HEADER.getCode()),
        arguments(headersMap4, REQUEST_TIME_TOO_SKEWED.getCode()),
        arguments(headersMap5, REQUEST_TIME_TOO_SKEWED.getCode()),
        arguments(headersMap6, S3_AUTHINFO_CREATION_ERROR.getCode())
    );
  }

  @ParameterizedTest
  @MethodSource("testValidateRequestHeadersInput")
  public void testValidateRequestHeaders(
      MultivaluedMap<String, String> headerMap,
      String expectedResult)
      throws Exception {
    String actualResult = "success";
    ContainerRequestContext context = setupContext(
        new URI("https://0.0.0.0:9878/"),
        "GET",
        headerMap,
        new MultivaluedHashMap<>());
    try {
      SignatureInfo signatureInfo = new AuthorizationV4HeaderParser(
          headerMap.getFirst("Authorization"),
          headerMap.getFirst("X-Amz-Date")).parseSignature();
      signatureInfo.setUnfilteredURI("/");
      StringToSignProducer.createSignatureBase(signatureInfo, context);
    } catch (MalformedResourceException e) {
      if (REQUEST_TIME_TOO_SKEWED.equals(e.getErrorCode())) {
        actualResult = REQUEST_TIME_TOO_SKEWED.getCode();
      } else {
        actualResult = MALFORMED_HEADER.getCode();
      }
    } catch (OS3Exception e) {
      actualResult = e.getCode();
    }

    assertEquals(expectedResult, actualResult);
  }

  private static Stream<Arguments> testValidateCanonicalHeadersInput() {
    return Stream.of(
        // Well-formed signed headers
        arguments("content-type;host;x-amz-content-sha256;" +
                "x-amz-date;x-amz-security-token",
            "success"),
        // No host signed header
        arguments("content-type;x-amz-content-sha256;" +
                "x-amz-date;x-amz-security-token",
            S3_AUTHINFO_CREATION_ERROR.getCode()),
        // Missing x-amz-* i.e., x-amz-security-token signed header
        arguments("content-type;host;x-amz-content-sha256;x-amz-date",
            S3_AUTHINFO_CREATION_ERROR.getCode()),
        // signed header missing in request headers
        arguments("content-type;dummy;host;x-amz-content-sha256;" +
                "x-amz-date;x-amz-security-token",
            S3_AUTHINFO_CREATION_ERROR.getCode())
    );
  }

  @ParameterizedTest
  @MethodSource("testValidateCanonicalHeadersInput")
  public void testValidateCanonicalHeaders(
      String signedHeaders,
      String expectedResult) throws Exception {
    String actualResult = "success";
    String authHeader = "AWS4-HMAC-SHA256 Credential=ozone/"
        + DATE_FORMATTER.format(LocalDate.now(ZoneOffset.UTC))
        + "/us-east-1/s3/aws4_request, "
        + "SignedHeaders=" + signedHeaders + ", "
        + "Signature=db81b057718d7c1b3b" +
        "8dffa29933099551c51d787b3b13b9e0f9ebed45982bf2";
    MultivaluedMap<String, String> headerMap = new MultivaluedHashMap<>();
    headerMap.putSingle("Authorization", authHeader);
    headerMap.putSingle("Content-Length", "123");
    headerMap.putSingle("content-type", "application/octet-stream");
    headerMap.putSingle("host", "0.0.0.0:9878");
    headerMap.putSingle("x-amz-content-sha256", "Content-SHA");
    headerMap.putSingle("x-amz-date", DATETIME);
    headerMap.putSingle("x-amz-security-token", "dummy");
    ContainerRequestContext context = setupContext(
        new URI("https://0.0.0.0:9878/"),
        "GET",
        headerMap,
        new MultivaluedHashMap<>());
    SignatureInfo signatureInfo = new AuthorizationV4HeaderParser(
        headerMap.getFirst("Authorization"),
        headerMap.getFirst("x-amz-date")).parseSignature();
    signatureInfo.setUnfilteredURI("/");

    try {
      StringToSignProducer.createSignatureBase(signatureInfo, context);
    } catch (OS3Exception e) {
      actualResult = e.getCode();
    }

    assertEquals(expectedResult, actualResult);
  }
}
