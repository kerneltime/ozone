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

package org.apache.hadoop.ozone.s3.iam;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Configuration for AWS IAM integration.
 */
@ConfigGroup(prefix = "ozone.s3g.iam")
@InterfaceAudience.Private
public class AWSIAMConfig {

  @Config(key = "enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "false",
      tags = {ConfigTag.SECURITY, ConfigTag.S3GATEWAY},
      description = "Whether to enable AWS IAM integration for STS requests")
  private boolean enabled;

  @Config(key = "region",
      type = ConfigType.STRING,
      defaultValue = "us-east-1",
      tags = {ConfigTag.SECURITY, ConfigTag.S3GATEWAY},
      description = "AWS region to use for IAM and STS client")
  private String region;
  
  @Config(key = "access.key",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = {ConfigTag.SECURITY, ConfigTag.S3GATEWAY},
      description = "AWS access key to use for IAM and STS client")
  private String accessKey;
  
  @Config(key = "secret.key",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = {ConfigTag.SECURITY, ConfigTag.S3GATEWAY},
      description = "AWS secret key to use for IAM and STS client")
  private String secretKey;
  
  @Config(key = "endpoint",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = {ConfigTag.SECURITY, ConfigTag.S3GATEWAY},
      description = "Custom AWS endpoint to use for IAM and STS client")
  private String endpoint;
  
  @Config(key = "role.prefix",
      type = ConfigType.STRING,
      defaultValue = "arn:aws:iam::",
      tags = {ConfigTag.SECURITY, ConfigTag.S3GATEWAY},
      description = "Role ARN prefix to recognize valid AWS role ARNs")
  private String rolePrefix;
  
  @Config(key = "token.duration.seconds",
      type = ConfigType.INT,
      defaultValue = "3600",
      tags = {ConfigTag.SECURITY, ConfigTag.S3GATEWAY},
      description = "Default token duration in seconds for temporary credentials")
  private int tokenDuration;
  
  @Config(key = "max.token.duration.seconds",
      type = ConfigType.INT,
      defaultValue = "43200",
      tags = {ConfigTag.SECURITY, ConfigTag.S3GATEWAY},
      description = "Maximum token duration in seconds for temporary credentials")
  private int maxTokenDuration;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getRolePrefix() {
    return rolePrefix;
  }

  public void setRolePrefix(String rolePrefix) {
    this.rolePrefix = rolePrefix;
  }

  public int getTokenDuration() {
    return tokenDuration;
  }

  public void setTokenDuration(int tokenDuration) {
    this.tokenDuration = tokenDuration;
  }

  public int getMaxTokenDuration() {
    return maxTokenDuration;
  }

  public void setMaxTokenDuration(int maxTokenDuration) {
    this.maxTokenDuration = maxTokenDuration;
  }
}