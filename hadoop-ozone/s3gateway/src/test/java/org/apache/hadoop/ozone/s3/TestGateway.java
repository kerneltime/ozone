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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Tests S3 Gateway startup behavior.
 */
public class TestGateway {

  private final Logger rootLogger = LogManager.getLogManager().getLogger("");
  private Handler[] originalHandlers;

  @BeforeEach
  void saveRootLoggerHandlers() {
    originalHandlers = rootLogger.getHandlers();
    removeRootLoggerHandlers();
  }

  @AfterEach
  void restoreRootLoggerHandlers() {
    removeRootLoggerHandlers();
    for (Handler handler : originalHandlers) {
      rootLogger.addHandler(handler);
    }
  }

  @Test
  void redirectsJulLogsToSlf4j() {
    Logger julLogger = Logger.getLogger(TestGateway.class.getName());
    LogCapturer logCapturer = LogCapturer.captureLogs(TestGateway.class);
    rootLogger.addHandler(new ConsoleHandler());

    try {
      Gateway.redirectJulToSlf4j();

      assertThat(rootLogger.getHandlers())
          .singleElement()
          .isInstanceOf(SLF4JBridgeHandler.class);

      String message = "JUL warning redirected to the S3 Gateway log";
      julLogger.warning(message);

      assertThat(logCapturer.getOutput()).contains(message);
    } finally {
      logCapturer.stopCapturing();
    }
  }

  /** Misconfigurations, each with the lines of the credentials file (null: no file) and the reason in the error. */
  static Stream<Arguments> staticCredentialMisconfigurations() {
    return Stream.of(
        arguments(true, new String[] {"first=first-secret-value"}, OZONE_SECURITY_ENABLED_KEY),
        arguments(false, null, "does not exist"),
        arguments(false, new String[] {"# no keys yet"}, "holds no credentials"),
        arguments(false, new String[] {"first=first-secret-value", "first=second-secret-value"},
            "line 2 repeats access key ID first"));
  }

  /** The key exists before the gateway first starts, so a gateway that cannot verify signatures refuses to start. */
  @ParameterizedTest
  @MethodSource("staticCredentialMisconfigurations")
  void refusesToStartWithMisconfiguredStaticCredentials(boolean securityEnabled, String[] lines, String reason,
      @TempDir Path dir) throws Exception {
    Path file = dir.resolve("credentials");
    if (lines != null) {
      Files.write(file, Arrays.asList(lines), UTF_8);
    }
    Gateway gateway = new Gateway();
    StringWriter err = new StringWriter();
    gateway.getCmd().setErr(new PrintWriter(err, true));
    OzoneConfigurationHolder.resetConfiguration();
    int exitCode = GenericCli.EXECUTION_ERROR_EXIT_CODE;
    try {
      // Ephemeral addresses, so that a gateway that does start binds no well-known port.
      exitCode = gateway.execute(new String[] {"-D", OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY + "=" + file,
          "-D", OZONE_SECURITY_ENABLED_KEY + "=" + securityEnabled,
          "-D", S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY + "=127.0.0.1:0",
          "-D", S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY + "=127.0.0.1:0"});

      assertEquals(GenericCli.EXECUTION_ERROR_EXIT_CODE, exitCode);
      assertThat(err.toString()).contains(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY, reason)
          .doesNotContain("secret-value");
    } finally {
      if (exitCode == 0) {
        gateway.stop();
      }
      OzoneConfigurationHolder.resetConfiguration();
    }
  }

  private void removeRootLoggerHandlers() {
    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }
  }
}
