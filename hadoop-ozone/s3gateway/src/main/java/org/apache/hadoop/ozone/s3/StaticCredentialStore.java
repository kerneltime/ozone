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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STATIC_CREDENTIALS_RELOAD_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STATIC_CREDENTIALS_RELOAD_INTERVAL_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The access keys and secrets the S3 Gateway verifies SigV4 signatures with when Ozone security is disabled, read from
 * {@value S3GatewayConfigKeys#OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY}: one {@code <accessKeyId>=<secret>} per line,
 * ignoring blank lines and lines starting with {@code #}. The file is re-read at the reload interval. A file that is
 * absent, empty or malformed keeps the gateway from starting, and at a reload keeps the previous keys.
 */
public final class StaticCredentialStore implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(StaticCredentialStore.class);

  private final Path file;
  private final long reloadIntervalMillis;
  private final ScheduledExecutorService reloader;
  /** Replaced as a whole, so that a request sees either the previous keys or the new ones. */
  private volatile Map<String, String> secrets;

  private StaticCredentialStore(Path file, Map<String, String> secrets, long reloadIntervalMillis) {
    this.file = file;
    this.secrets = secrets;
    this.reloadIntervalMillis = reloadIntervalMillis;
    this.reloader = reloadIntervalMillis > 0 ? Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
        .setNameFormat("s3g-static-credentials-reload-%d")
        .setDaemon(true)
        .build()) : null;
  }

  /**
   * @return the store, or {@code null} if no credentials file is configured
   * @throws IOException if the file is absent, empty or malformed
   * @throws IllegalArgumentException if Ozone security is enabled
   */
  public static StaticCredentialStore load(ConfigurationSource conf) throws IOException {
    String fileName = conf.getTrimmed(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY, "");
    if (fileName.isEmpty()) {
      return null;
    }
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      throw new IllegalArgumentException(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY + " is set while "
          + OZONE_SECURITY_ENABLED_KEY + " is true: with security enabled, OM verifies S3 signatures");
    }
    Path file = Paths.get(fileName);
    Map<String, String> secrets = read(file);
    long reloadIntervalMillis = conf.getTimeDuration(OZONE_S3G_STATIC_CREDENTIALS_RELOAD_INTERVAL_KEY,
        OZONE_S3G_STATIC_CREDENTIALS_RELOAD_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    LOG.info("Verifying S3 signatures against {} access keys from {}", secrets.size(), file);
    return new StaticCredentialStore(file, secrets, reloadIntervalMillis);
  }

  /** @return the secret of {@code accessKeyId}, or {@code null} if the file has no such access key */
  public String getSecret(String accessKeyId) {
    return secrets.get(accessKeyId);
  }

  /** Starts re-reading the file at the reload interval, unless the interval is 0. */
  public void start() {
    if (reloader != null) {
      reloader.scheduleWithFixedDelay(this::reload, reloadIntervalMillis, reloadIntervalMillis, TimeUnit.MILLISECONDS);
    }
  }

  @VisibleForTesting
  void reload() {
    try {
      secrets = read(file);
    } catch (IOException | RuntimeException e) {
      // A periodic task that throws is never run again, so every failure is caught here.
      LOG.error("Keeping the previous S3 access keys: {}", e.getMessage());
      S3GatewayMetrics metrics = S3GatewayMetrics.getMetrics();
      if (metrics != null) {
        metrics.incStaticCredentialsReloadFailure();
      }
    }
  }

  @Override
  public void close() {
    if (reloader != null) {
      reloader.shutdownNow();
    }
  }

  /** Reads the whole file before anything is replaced. Error messages name the line, never its content. */
  private static Map<String, String> read(Path file) throws IOException {
    List<String> lines;
    try {
      lines = Files.readAllLines(file, StandardCharsets.UTF_8);
    } catch (NoSuchFileException e) {
      throw new IOException(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY + ": " + file + " does not exist", e);
    } catch (IOException e) {
      throw new IOException(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY + ": cannot read " + file + ": " + e, e);
    }
    Map<String, String> secrets = new HashMap<>();
    for (int i = 0; i < lines.size(); i++) {
      String line = lines.get(i).trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      int separator = line.indexOf('=');
      String accessKeyId = separator < 0 ? "" : line.substring(0, separator).trim();
      String secret = separator < 0 ? "" : line.substring(separator + 1).trim();
      if (accessKeyId.isEmpty() || secret.isEmpty()) {
        throw new IOException(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY + ": " + file + ": line " + (i + 1)
            + " is not <accessKeyId>=<secret>");
      }
      if (secrets.putIfAbsent(accessKeyId, secret) != null) {
        throw new IOException(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY + ": " + file + ": line " + (i + 1)
            + " repeats access key ID " + accessKeyId);
      }
    }
    if (secrets.isEmpty()) {
      throw new IOException(OZONE_S3G_STATIC_CREDENTIALS_FILE_KEY + ": " + file + " holds no credentials");
    }
    return Collections.unmodifiableMap(secrets);
  }
}
