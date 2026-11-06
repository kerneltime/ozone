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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STATIC_CREDENTIALS_RELOAD_INTERVAL_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests loading and reloading the static credentials the S3 Gateway verifies signatures with.
 */
class TestStaticCredentialStore {

  private static final String FIRST_SECRET = "first-secret-value";
  private static final String SECOND_SECRET = "second-secret-value";

  @TempDir
  private Path dir;
  private Path file;

  @BeforeEach
  void setup() {
    file = dir.resolve("credentials");
  }

  @AfterEach
  void unregisterMetrics() {
    S3GatewayMetrics.unRegister();
  }

  @Test
  void isOffWhenNoFileIsConfigured() throws IOException {
    assertNull(StaticCredentialStore.load(new OzoneConfiguration()));
  }

  @Test
  void loadsEveryKeyOfTheFile() throws IOException {
    write("# keys of the gateway", "", "first=" + FIRST_SECRET, "  second = " + SECOND_SECRET + "  ", "third=a=b");

    try (StaticCredentialStore store = StaticCredentialStore.load(conf())) {
      assertEquals(FIRST_SECRET, store.getSecret("first"));
      assertEquals(SECOND_SECRET, store.getSecret("second"));
      assertEquals("a=b", store.getSecret("third"));
      assertNull(store.getSecret("unknown"));
    }
  }

  /** File states that refuse a start and that a reload ignores: the name, the lines (null: no file), the reason. */
  static Stream<Arguments> invalidFiles() {
    return Stream.of(
        arguments("absent", null, "does not exist"),
        arguments("empty", new String[] {"", "# no keys yet"}, "holds no credentials"),
        arguments("line without '='", new String[] {"first=" + FIRST_SECRET, SECOND_SECRET}, "line 2"),
        arguments("empty access key ID", new String[] {"=" + FIRST_SECRET}, "line 1"),
        arguments("empty secret", new String[] {"first="}, "line 1"),
        arguments("duplicate access key ID", new String[] {"first=" + FIRST_SECRET, "first=" + SECOND_SECRET},
            "line 2 repeats access key ID first"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidFiles")
  void refusesToStartWithAnInvalidFile(String state, String[] lines, String reason) throws IOException {
    if (lines != null) {
      write(lines);
    }

    IOException e = assertThrows(IOException.class, () -> StaticCredentialStore.load(conf()));

    assertThat(e.getMessage()).contains(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY, reason)
        .doesNotContain(FIRST_SECRET, SECOND_SECRET);
  }

  @Test
  void refusesToStartWithSecurityEnabled() throws IOException {
    write("first=" + FIRST_SECRET);
    OzoneConfiguration conf = conf();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> StaticCredentialStore.load(conf));

    assertThat(e.getMessage()).contains(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY, OZONE_SECURITY_ENABLED_KEY);
  }

  @Test
  void reloadSwapsToTheNewKeys() throws IOException {
    write("first=" + FIRST_SECRET, "second=" + SECOND_SECRET);

    try (StaticCredentialStore store = StaticCredentialStore.load(conf())) {
      write("second=" + SECOND_SECRET);
      store.reload();

      assertNull(store.getSecret("first"));
      assertEquals(SECOND_SECRET, store.getSecret("second"));
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidFiles")
  void reloadKeepsThePreviousKeysWhenTheFileIsInvalid(String state, String[] lines, String reason) throws IOException {
    write("first=" + FIRST_SECRET, "second=" + SECOND_SECRET);
    S3GatewayMetrics metrics = S3GatewayMetrics.create(new OzoneConfiguration());

    try (StaticCredentialStore store = StaticCredentialStore.load(conf());
         LogCapturer logs = LogCapturer.captureLogs(StaticCredentialStore.class)) {
      if (lines == null) {
        Files.delete(file);
      } else {
        write(lines);
      }
      long failures = metrics.getStaticCredentialsReloadFailure();

      store.reload();

      assertEquals(FIRST_SECRET, store.getSecret("first"));
      assertEquals(SECOND_SECRET, store.getSecret("second"));
      assertEquals(failures + 1, metrics.getStaticCredentialsReloadFailure());
      assertThat(logs.getOutput()).contains(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY, reason)
          .doesNotContain(FIRST_SECRET, SECOND_SECRET);
    }
  }

  @Test
  void reloadsAtTheConfiguredInterval() throws Exception {
    write("first=" + FIRST_SECRET, "second=" + SECOND_SECRET);
    OzoneConfiguration conf = conf();
    conf.set(OZONE_S3G_STATIC_CREDENTIALS_RELOAD_INTERVAL_KEY, "100ms");

    try (StaticCredentialStore store = StaticCredentialStore.load(conf)) {
      store.start();
      write("second=" + SECOND_SECRET);

      GenericTestUtils.waitFor(() -> store.getSecret("first") == null, 50, 10_000);
      assertEquals(SECOND_SECRET, store.getSecret("second"));
    }
  }

  private OzoneConfiguration conf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY, file.toString());
    return conf;
  }

  /** Replaces the file in one step, as a reload must never read a partly written file. */
  private void write(String... lines) throws IOException {
    Path next = dir.resolve("credentials.next");
    Files.write(next, Arrays.asList(lines), UTF_8);
    Files.move(next, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }
}
