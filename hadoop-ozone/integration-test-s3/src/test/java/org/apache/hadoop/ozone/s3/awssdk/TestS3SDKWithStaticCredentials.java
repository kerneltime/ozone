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

package org.apache.hadoop.ozone.s3.awssdk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STATIC_CREDENTIALS_RELOAD_INTERVAL_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.s3.awssdk.v1.AbstractS3SDKV1Tests;
import org.apache.hadoop.ozone.s3.awssdk.v2.AbstractS3SDKV2Tests;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Runs the AWS SDK tests against S3 Gateways that, with security disabled, verify every signature against static
 * credentials, and checks the requests they refuse.
 */
@ExtendWith(TestS3SDKWithStaticCredentials.SkipRequestsSignedAsNotSent.class)
class TestS3SDKWithStaticCredentials extends OzoneS3SDKTests {

  /** The access key and secret the SDK tests sign with (see {@code S3ClientFactory}). */
  private static final String SDK_ACCESS_KEY = "user";
  private static final String SDK_SECRET = "password";
  private static final String KEPT_ACCESS_KEY = "kept";
  private static final String KEPT_SECRET = "kept-secret-value";
  private static final String REMOVED_ACCESS_KEY = "removed";
  private static final String REMOVED_SECRET = "removed-secret-value";
  private static final String WRONG_SECRET = "wrong-secret-value";
  private static final String STREAMING_PAYLOAD = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
  private static final String EMPTY_SHA256 = DigestUtils.sha256Hex("");

  /** Static, so that it exists before the cluster starts in {@code @BeforeAll}. */
  @TempDir
  private static Path dir;

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = super.createOzoneConfig();
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, false);
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    writeCredentials(REMOVED_ACCESS_KEY + "=" + REMOVED_SECRET, KEPT_ACCESS_KEY + "=" + KEPT_SECRET,
        SDK_ACCESS_KEY + "=" + SDK_SECRET);
    conf.set(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY, credentialsFile().toString());
    conf.set(OZONE_S3G_STATIC_CREDENTIALS_RELOAD_INTERVAL_KEY, "1s");
    return conf;
  }

  @Test
  void requestsSignedWithAConfiguredKeySucceed() {
    String bucket = "static-credentials-accepted";
    try (S3Client client = client(KEPT_ACCESS_KEY, KEPT_SECRET)) {
      client.createBucket(b -> b.bucket(bucket));
      client.putObject(b -> b.bucket(bucket).key("k"), RequestBody.fromString("v"));

      assertEquals("v", client.getObjectAsBytes(b -> b.bucket(bucket).key("k")).asUtf8String());
    }
  }

  @Test
  void aWrongSecretOrAnUnknownKeyIsRefusedBeforeAnyChange() {
    String bucket = "static-credentials-refused";
    try (S3Client valid = client(SDK_ACCESS_KEY, SDK_SECRET);
         S3Client wrongSecret = client(SDK_ACCESS_KEY, WRONG_SECRET);
         S3Client unknownKey = client("UNKNOWNACCESSKEY", SDK_SECRET)) {
      valid.createBucket(b -> b.bucket(bucket));
      // The same request signed with the right secret succeeds, so the refusals below are the key's and the secret's.
      valid.putObject(b -> b.bucket(bucket).key("control"), RequestBody.fromString("v"));
      LogCapturer audit = LogCapturer.log4j2("S3GAudit");
      audit.clearOutput();

      assertRefused("SignatureDoesNotMatch",
          () -> wrongSecret.putObject(b -> b.bucket(bucket).key("k"), RequestBody.fromString("v")));
      assertRefused("InvalidAccessKeyId",
          () -> unknownKey.putObject(b -> b.bucket(bucket).key("k"), RequestBody.fromString("v")));

      assertThrows(NoSuchKeyException.class, () -> valid.headObject(b -> b.bucket(bucket).key("k")));
      String refusals = Arrays.stream(audit.getOutput().split("\n"))
          .filter(line -> line.contains("op=AUTH(PUT /" + bucket + "/k)"))
          .collect(Collectors.joining("\n"));
      assertThat(refusals).contains("user=" + SDK_ACCESS_KEY, "user=UNKNOWNACCESSKEY", "ret=FAILURE")
          .doesNotContain(WRONG_SECRET, SDK_SECRET, "Signature=", "ip=null")
          .doesNotContainPattern("[0-9a-f]{64}");
    }
  }

  @Test
  void aBadChunkSignatureAbortsTheUpload() throws Exception {
    String bucket = "static-credentials-chunks";
    String[] chunks = {"first chunk, ", "second chunk, ", "third chunk"};
    try (S3Client valid = client(SDK_ACCESS_KEY, SDK_SECRET)) {
      valid.createBucket(b -> b.bucket(bucket));

      // The same request with every signature intact succeeds, so the refusal below is the third chunk's.
      assertThat(signedChunkedPut(bucket, "intact", chunks, -1)).startsWith("200\n");
      assertEquals(String.join("", chunks), valid.getObjectAsBytes(b -> b.bucket(bucket).key("intact")).asUtf8String());

      assertThat(signedChunkedPut(bucket, "corrupted", chunks, 2)).startsWith("403\n")
          .contains("<Code>SignatureDoesNotMatch</Code>");
      assertThrows(NoSuchKeyException.class, () -> valid.headObject(b -> b.bucket(bucket).key("corrupted")));
    }
  }

  /** SigV4 signs a raw '+' in the query as {@code %2B}, so both spellings must list the keys under a literal plus. */
  @Test
  void aPlusInTheQueryIsALiteralPlusHoweverItIsSent() throws Exception {
    String bucket = "static-credentials-plus";
    try (S3Client valid = client(SDK_ACCESS_KEY, SDK_SECRET)) {
      valid.createBucket(b -> b.bucket(bucket));
      valid.putObject(b -> b.bucket(bucket).key("a+b/plus"), RequestBody.fromString("v"));
      valid.putObject(b -> b.bucket(bucket).key("a b/space"), RequestBody.fromString("v"));
    }

    assertThat(signedGet(bucket, "list-type=2&prefix=a+b", "list-type=2&prefix=a%2Bb"))
        .startsWith("200\n").contains("<Key>a+b/plus</Key>").doesNotContain("<Key>a b/space</Key>");
    assertThat(signedGet(bucket, "list-type=2&prefix=a%2Bb", "list-type=2&prefix=a%2Bb"))
        .startsWith("200\n").contains("<Key>a+b/plus</Key>").doesNotContain("<Key>a b/space</Key>");
    assertThat(signedGet(bucket, "list-type=2&prefix=a%20b", "list-type=2&prefix=a%20b"))
        .startsWith("200\n").contains("<Key>a b/space</Key>").doesNotContain("<Key>a+b/plus</Key>");
  }

  @Test
  void aReloadSwapsTheKeys() throws Exception {
    try (S3Client kept = client(KEPT_ACCESS_KEY, KEPT_SECRET);
         S3Client removed = client(REMOVED_ACCESS_KEY, REMOVED_SECRET)) {
      kept.listBuckets();
      removed.listBuckets();

      // Remove the first line.
      writeCredentials(KEPT_ACCESS_KEY + "=" + KEPT_SECRET, SDK_ACCESS_KEY + "=" + SDK_SECRET);

      GenericTestUtils.waitFor(() -> isRefusedAsUnknown(removed), 200, 30_000);
      kept.listBuckets();
    }
  }

  private static void assertRefused(String errorCode, Runnable request) {
    S3Exception e = assertThrows(S3Exception.class, request::run);
    assertEquals(403, e.statusCode());
    assertEquals(errorCode, e.awsErrorDetails().errorCode());
  }

  private static boolean isRefusedAsUnknown(S3Client client) {
    try {
      client.listBuckets();
      return false;
    } catch (S3Exception e) {
      assertEquals("InvalidAccessKeyId", e.awsErrorDetails().errorCode());
      return true;
    }
  }

  private S3Client client(String accessKey, String secret) {
    return S3Client.builder()
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create("http://" + getCluster().getConf().get(OZONE_S3G_HTTP_ADDRESS_KEY)))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secret)))
        .forcePathStyle(true)
        .build();
  }

  /**
   * PUTs the chunks as a {@code STREAMING-AWS4-HMAC-SHA256-PAYLOAD} upload, signed as SigV4 specifies, with the
   * signature of chunk {@code corruptedChunk} corrupted, or none if it is negative.
   *
   * @return the status code, a newline and the response body
   */
  private String signedChunkedPut(String bucket, String key, String[] chunks, int corruptedChunk) throws Exception {
    URL url = new URL("http://" + getCluster().getConf().get(OZONE_S3G_HTTP_ADDRESS_KEY) + "/" + bucket + "/" + key);
    String dateTime = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(ZonedDateTime.now(ZoneOffset.UTC));
    String scope = dateTime.substring(0, 8) + "/us-east-1/s3/aws4_request";
    byte[] signingKey = hmac(hmac(hmac(hmac(("AWS4" + SDK_SECRET).getBytes(UTF_8), dateTime.substring(0, 8)),
        "us-east-1"), "s3"), "aws4_request");
    int decodedLength = Arrays.stream(chunks).mapToInt(chunk -> chunk.getBytes(UTF_8).length).sum();
    String signedHeaders = "host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length";
    String canonicalRequest = "PUT\n/" + bucket + "/" + key + "\n\n"
        + "host:" + url.getHost() + ":" + url.getPort() + "\n"
        + "x-amz-content-sha256:" + STREAMING_PAYLOAD + "\n"
        + "x-amz-date:" + dateTime + "\n"
        + "x-amz-decoded-content-length:" + decodedLength + "\n\n"
        + signedHeaders + "\n" + STREAMING_PAYLOAD;
    String seedSignature = Hex.encodeHexString(hmac(signingKey,
        "AWS4-HMAC-SHA256\n" + dateTime + "\n" + scope + "\n" + DigestUtils.sha256Hex(canonicalRequest)));

    StringBuilder body = new StringBuilder();
    String previousSignature = seedSignature;
    for (int i = 0; i <= chunks.length; i++) {
      String chunk = i < chunks.length ? chunks[i] : "";
      String signature = Hex.encodeHexString(hmac(signingKey, "AWS4-HMAC-SHA256-PAYLOAD\n" + dateTime + "\n" + scope
          + "\n" + previousSignature + "\n" + EMPTY_SHA256 + "\n" + DigestUtils.sha256Hex(chunk)));
      previousSignature = signature;
      String sent = i == corruptedChunk ? (signature.charAt(0) == '0' ? "1" : "0") + signature.substring(1) : signature;
      body.append(Integer.toHexString(chunk.getBytes(UTF_8).length)).append(";chunk-signature=").append(sent)
          .append("\r\n").append(chunk).append("\r\n");
    }

    byte[] payload = body.toString().getBytes(UTF_8);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("PUT");
    connection.setDoOutput(true);
    connection.setFixedLengthStreamingMode(payload.length);
    connection.setRequestProperty("Content-Type", "application/octet-stream");
    connection.setRequestProperty("x-amz-content-sha256", STREAMING_PAYLOAD);
    connection.setRequestProperty("x-amz-date", dateTime);
    connection.setRequestProperty("x-amz-decoded-content-length", String.valueOf(decodedLength));
    connection.setRequestProperty("Authorization", "AWS4-HMAC-SHA256 Credential=" + SDK_ACCESS_KEY + "/" + scope
        + ", SignedHeaders=" + signedHeaders + ", Signature=" + seedSignature);
    try {
      try (OutputStream out = connection.getOutputStream()) {
        out.write(payload);
      }
      int status = connection.getResponseCode();
      try (InputStream response = status < 400 ? connection.getInputStream() : connection.getErrorStream()) {
        return status + "\n" + (response == null ? "" : IOUtils.toString(response, UTF_8));
      }
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Skips the SDK tests whose requests the SDK signs differently from how it sends them, so that a gateway that
   * verifies signatures refuses them.
   */
  static class SkipRequestsSignedAsNotSent implements ExecutionCondition {
    private static final String V1 = AbstractS3SDKV1Tests.class.getName();
    private static final String V2 = AbstractS3SDKV2Tests.class.getName();
    private static final Map<String, String> REASONS = Map.of(
        V1 + "#testPutDoubleSlashPrefixObject", "SDK v1 sends the key //dir1 as /%2F/dir1 but signs ///dir1",
        V1 + "$PresignedUrlTests#testPresignedUrlMultipartUpload",
        "SDK v1 presigns a POST without a body with an empty query string, but the URL carries the query",
        V2 + "$PresignedUrlTests#testPresignedUrlMultipartUpload",
        "Over plain HTTP, SDK v2 presigns CompleteMultipartUpload with the SHA-256 of its body as the payload hash,"
            + " and sends that hash nowhere; a presigned request is verified with UNSIGNED-PAYLOAD");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      return context.getTestMethod()
          .map(method -> REASONS.get(method.getDeclaringClass().getName() + "#" + method.getName()))
          .map(ConditionEvaluationResult::disabled)
          .orElse(ConditionEvaluationResult.enabled("signed as sent"));
    }
  }

  /**
   * GETs {@code /bucket?rawQuery}, signed as SigV4 specifies with {@code canonicalQuery}, the canonical form of the
   * query written out by hand.
   *
   * @return the status code, a newline and the response body
   */
  private String signedGet(String bucket, String rawQuery, String canonicalQuery) throws Exception {
    String address = getCluster().getConf().get(OZONE_S3G_HTTP_ADDRESS_KEY);
    URL url = new URL("http://" + address + "/" + bucket + "?" + rawQuery);
    String dateTime = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(ZonedDateTime.now(ZoneOffset.UTC));
    String scope = dateTime.substring(0, 8) + "/us-east-1/s3/aws4_request";
    byte[] signingKey = hmac(hmac(hmac(hmac(("AWS4" + SDK_SECRET).getBytes(UTF_8), dateTime.substring(0, 8)),
        "us-east-1"), "s3"), "aws4_request");
    String signedHeaders = "host;x-amz-content-sha256;x-amz-date";
    String canonicalRequest = "GET\n/" + bucket + "\n" + canonicalQuery + "\n"
        + "host:" + url.getHost() + ":" + url.getPort() + "\n"
        + "x-amz-content-sha256:UNSIGNED-PAYLOAD\n"
        + "x-amz-date:" + dateTime + "\n\n"
        + signedHeaders + "\nUNSIGNED-PAYLOAD";
    String signature = Hex.encodeHexString(hmac(signingKey,
        "AWS4-HMAC-SHA256\n" + dateTime + "\n" + scope + "\n" + DigestUtils.sha256Hex(canonicalRequest)));

    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
    connection.setRequestProperty("x-amz-date", dateTime);
    connection.setRequestProperty("Authorization", "AWS4-HMAC-SHA256 Credential=" + SDK_ACCESS_KEY + "/" + scope
        + ", SignedHeaders=" + signedHeaders + ", Signature=" + signature);
    try {
      int status = connection.getResponseCode();
      try (InputStream response = status < 400 ? connection.getInputStream() : connection.getErrorStream()) {
        return status + "\n" + (response == null ? "" : IOUtils.toString(response, UTF_8));
      }
    } finally {
      connection.disconnect();
    }
  }

  private static byte[] hmac(byte[] key, String data) throws Exception {
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(key, "HmacSHA256"));
    return mac.doFinal(data.getBytes(UTF_8));
  }

  private Path credentialsFile() {
    return dir.resolve("credentials");
  }

  /** Replaces the credentials file in one step, as a reload must never read a partly written file. */
  private void writeCredentials(String... lines) {
    try {
      Path next = dir.resolve("credentials.next");
      Files.write(next, Arrays.asList(lines), UTF_8);
      Files.move(next, credentialsFile(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
