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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor.LowerCaseKeyStringMap;
import org.apache.kerby.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateless utility to create stringToSign, the base of the signature.
 */
public final class StringToSignProducer {

  public static final String X_AMAZ_DATE = "x-amz-date";
  private static final Logger LOG =
      LoggerFactory.getLogger(StringToSignProducer.class);
  private static final Charset UTF_8 = StandardCharsets.UTF_8;
  private static final String NEWLINE = "\n";
  public static final String HOST = "host";

  private static final String X_AMZ_SIGNATURE = "X-Amz-Signature";
  private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();
  public static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
          .withZone(ZoneOffset.UTC);

  private StringToSignProducer() {
  }

  public static String createSignatureBase(
      SignatureInfo signatureInfo,
      ContainerRequestContext context
  ) throws Exception {
    return createSignatureBase(signatureInfo,
        context.getMethod(),
        LowerCaseKeyStringMap.fromHeaderMap(context.getHeaders()),
        context.getUriInfo().getRequestUri().getRawQuery());
  }

  @VisibleForTesting
  public static String createSignatureBase(
      SignatureInfo signatureInfo,
      String method,
      LowerCaseKeyStringMap headers,
      String rawQuery
  ) throws Exception {
    StringBuilder strToSign = new StringBuilder();
    // According to AWS sigv4 documentation, authorization header should be
    // in following format.
    // Authorization: algorithm Credential=access key ID/credential scope,
    // SignedHeaders=SignedHeaders, Signature=signature

    // Construct String to sign in below format.
    // StringToSign =
    //    Algorithm + \n +
    //    RequestDateTime + \n +
    //    CredentialScope + \n +
    //    HashedCanonicalRequest
    String credentialScope = signatureInfo.getCredentialScope();

    // If the absolute path is empty, use a forward slash (/)
    String uri = signatureInfo.getUnfilteredURI();
    uri = StringUtils.isNotBlank(uri) ? uri : "/";
    // Encode URI and preserve forward slashes
    strToSign.append(signatureInfo.getAlgorithm()).append(NEWLINE);
    if (signatureInfo.getDateTime() == null) {
      LOG.error("DateTime Header not found.");
      throw newError(S3_AUTHINFO_CREATION_ERROR);
    }
    strToSign.append(signatureInfo.getDateTime()).append(NEWLINE)
        .append(credentialScope).append(NEWLINE);

    String canonicalRequest = buildCanonicalRequest(
        method,
        uri,
        signatureInfo.getSignedHeaders(),
        headers,
        rawQuery,
        signatureInfo.getPayloadHash());
    strToSign.append(hash(canonicalRequest));
    if (LOG.isDebugEnabled()) {
      LOG.debug("canonicalRequest:[{}]", canonicalRequest);
      LOG.debug("StringToSign:[{}]", strToSign);
    }

    return strToSign.toString();
  }

  public static Map<String, String> fromMultiValueToSingleValueMap(
      MultivaluedMap<String, String> queryParameters
  ) {
    Map<String, String> result = new HashMap<>();
    for (String key : queryParameters.keySet()) {
      result.put(key, queryParameters.getFirst(key));
    }
    return result;
  }

  public static String hash(String payload) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(payload.getBytes(UTF_8));
    return Hex.encode(md.digest()).toLowerCase();
  }

  @VisibleForTesting
  public static String buildCanonicalRequest(
      String method,
      String uri,
      String signedHeaders,
      Map<String, String> headers,
      String rawQuery,
      String payloadHash
  ) throws OS3Exception {

    String canonicalUri = canonicalUri(uri);

    String canonicalQueryStr = canonicalQuery(rawQuery);

    StringBuilder canonicalHeaders = new StringBuilder();

    for (String header : StringUtils.split(signedHeaders, ';')) {
      canonicalHeaders.append(header.toLowerCase())
          .append(':');
      if (headers.containsKey(header)) {
        // SigV4 trims the value and collapses runs of whitespace, but keeps the case of the value.
        canonicalHeaders.append(StringUtils.normalizeSpace(headers.get(header)))
            .append(NEWLINE);
      } else {
        LOG.error("Header " + header + " not present in "
            + "request but requested to be signed.");
        throw newError(S3_AUTHINFO_CREATION_ERROR);
      }
    }

    validateCanonicalHeaders(canonicalHeaders.toString(), headers);

    return method + NEWLINE
        + canonicalUri + NEWLINE
        + canonicalQueryStr + NEWLINE
        + canonicalHeaders + NEWLINE
        + signedHeaders + NEWLINE
        + payloadHash;
  }
  
  /**
   * S3 signs the path as the client sent it, encoded once: a percent-encoded octet, such as %2F in a key, stays
   * encoded, with upper-case hex digits, and any other octet that is neither unreserved nor '/' is percent-encoded.
   */
  private static String canonicalUri(String rawPath) {
    byte[] bytes = rawPath.getBytes(UTF_8);
    StringBuilder result = new StringBuilder(bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      if (isPercentEncodedOctet(bytes, i)) {
        appendPercentEncoded(result, decodeOctet(bytes, i));
        i += 2;
      } else if (bytes[i] == '/') {
        result.append('/');
      } else {
        appendUriEncoded(result, bytes[i] & 0xFF);
      }
    }
    return result.toString();
  }

  /**
   * Every query parameter, each value of a repeated one included, sorted by encoded name and then by encoded value,
   * without the signature of a presigned request.
   */
  private static String canonicalQuery(String rawQuery) {
    if (StringUtils.isEmpty(rawQuery)) {
      return "";
    }
    List<Pair<String, String>> params = new ArrayList<>();
    for (String param : StringUtils.split(rawQuery, '&')) {
      int separator = param.indexOf('=');
      String name = canonicalQueryComponent(separator < 0 ? param : param.substring(0, separator));
      if (!name.equals(X_AMZ_SIGNATURE)) {
        params.add(Pair.of(name, canonicalQueryComponent(separator < 0 ? "" : param.substring(separator + 1))));
      }
    }
    Collections.sort(params);
    return params.stream().map(p -> p.getLeft() + '=' + p.getRight()).collect(Collectors.joining("&"));
  }

  /** Percent-decodes a query parameter name or value, where '+' is a literal plus, and encodes it as SigV4 does. */
  private static String canonicalQueryComponent(String rawValue) {
    byte[] bytes = rawValue.getBytes(UTF_8);
    StringBuilder result = new StringBuilder(bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      if (isPercentEncodedOctet(bytes, i)) {
        appendUriEncoded(result, decodeOctet(bytes, i));
        i += 2;
      } else {
        appendUriEncoded(result, bytes[i] & 0xFF);
      }
    }
    return result.toString();
  }

  private static boolean isPercentEncodedOctet(byte[] bytes, int i) {
    return bytes[i] == '%' && i + 2 < bytes.length
        && Character.digit(bytes[i + 1], 16) >= 0 && Character.digit(bytes[i + 2], 16) >= 0;
  }

  private static int decodeOctet(byte[] bytes, int i) {
    return Character.digit(bytes[i + 1], 16) << 4 | Character.digit(bytes[i + 2], 16);
  }

  /** Appends an unreserved octet (RFC 3986) as is, and any other octet percent-encoded. */
  private static void appendUriEncoded(StringBuilder result, int octet) {
    if (octet >= 'A' && octet <= 'Z' || octet >= 'a' && octet <= 'z' || octet >= '0' && octet <= '9'
        || octet == '-' || octet == '.' || octet == '_' || octet == '~') {
      result.append((char) octet);
    } else {
      appendPercentEncoded(result, octet);
    }
  }

  private static void appendPercentEncoded(StringBuilder result, int octet) {
    result.append('%').append(HEX_DIGITS[octet >> 4]).append(HEX_DIGITS[octet & 0x0F]);
  }

  /**
   * According to AWS Sig V4 documentation:
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
   * The CanonicalHeaders list must include the following:
   * HTTP host header..
   * Any x-amz-* headers that you plan to include
   * in your request must also be added.
   *
   * @param canonicalHeaders
   * @param headers
   */
  private static void validateCanonicalHeaders(
      String canonicalHeaders,
      Map<String, String> headers)
      throws OS3Exception {
    if (!canonicalHeaders.contains(HOST + ":")) {
      LOG.error("The SignedHeaders list must include HTTP Host header");
      throw newError(S3_AUTHINFO_CREATION_ERROR);
    }
    for (String header : headers.keySet().stream()
        .filter(s -> s.startsWith("x-amz-"))
        .collect(Collectors.toSet())) {
      if (!(canonicalHeaders.contains(header + ":"))) {
        // According to AWS Signature V4 documentation using Authorization Header
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
        // The x-amz-content-sha256 header is not required for CanonicalHeaders
        if (X_AMZ_CONTENT_SHA256.equals(header)) {
          continue;
        }
        LOG.error("The SignedHeaders list must include all "
            + "x-amz-* headers in the request");
        throw newError(S3_AUTHINFO_CREATION_ERROR);
      }
    }
  }

}
