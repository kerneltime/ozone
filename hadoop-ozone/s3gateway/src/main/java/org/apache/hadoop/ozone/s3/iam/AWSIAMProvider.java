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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Provider for AWS IAM and STS clients.
 */
@InterfaceAudience.Private
@Singleton
public class AWSIAMProvider {

  private static final Logger LOG = 
      LoggerFactory.getLogger(AWSIAMProvider.class);

  private AmazonIdentityManagement iamClient;
  private AWSSecurityTokenService stsClient;
  private final AWSIAMConfig iamConfig;
  private boolean initialized = false;

  @Inject
  public AWSIAMProvider(OzoneConfiguration conf) {
    this.iamConfig = conf.getObject(AWSIAMConfig.class);
  }

  /**
   * Initialize the IAM and STS clients.
   * 
   * @throws IllegalStateException if AWS credentials are not configured
   */
  private synchronized void initializeClients() {
    if (initialized) {
      return;
    }

    if (!iamConfig.isEnabled()) {
      LOG.info("AWS IAM integration is disabled");
      return;
    }

    if (iamConfig.getAccessKey().isEmpty() || iamConfig.getSecretKey().isEmpty()) {
      throw new IllegalStateException(
          "AWS access key and secret key must be configured for IAM integration");
    }

    // Create AWS credentials
    AWSCredentials credentials = new BasicAWSCredentials(
        iamConfig.getAccessKey(), iamConfig.getSecretKey());
    
    // Configure client settings
    ClientConfiguration clientConfig = new ClientConfiguration();
    AWSStaticCredentialsProvider credentialsProvider = 
        new AWSStaticCredentialsProvider(credentials);

    // Create IAM client
    AmazonIdentityManagementClientBuilder iamBuilder = 
        AmazonIdentityManagementClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withClientConfiguration(clientConfig);
            
    // Create STS client
    AWSSecurityTokenServiceClientBuilder stsBuilder = 
        AWSSecurityTokenServiceClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withClientConfiguration(clientConfig);
    
    // Configure region or custom endpoint if specified
    if (!iamConfig.getEndpoint().isEmpty()) {
      EndpointConfiguration endpointConfig = new EndpointConfiguration(
          iamConfig.getEndpoint(), iamConfig.getRegion());
      iamBuilder.withEndpointConfiguration(endpointConfig);
      stsBuilder.withEndpointConfiguration(endpointConfig);
      LOG.info("Using custom AWS endpoint: {}", iamConfig.getEndpoint());
    } else {
      iamBuilder.withRegion(iamConfig.getRegion());
      stsBuilder.withRegion(iamConfig.getRegion());
      LOG.info("Using AWS region: {}", iamConfig.getRegion());
    }
    
    this.iamClient = iamBuilder.build();
    this.stsClient = stsBuilder.build();
    this.initialized = true;
    
    LOG.info("AWS IAM and STS clients initialized successfully");
  }

  /**
   * Get the IAM client.
   * 
   * @return AmazonIdentityManagement client
   * @throws IllegalStateException if IAM integration is disabled
   */
  public AmazonIdentityManagement getIAMClient() {
    if (!iamConfig.isEnabled()) {
      throw new IllegalStateException("AWS IAM integration is disabled");
    }
    
    if (!initialized) {
      initializeClients();
    }
    
    return iamClient;
  }

  /**
   * Get the STS client.
   * 
   * @return AWSSecurityTokenService client
   * @throws IllegalStateException if IAM integration is disabled
   */
  public AWSSecurityTokenService getSTSClient() {
    if (!iamConfig.isEnabled()) {
      throw new IllegalStateException("AWS IAM integration is disabled");
    }
    
    if (!initialized) {
      initializeClients();
    }
    
    return stsClient;
  }

  /**
   * Get the IAM configuration.
   * 
   * @return AWSIAMConfig
   */
  public AWSIAMConfig getConfig() {
    return iamConfig;
  }

  /**
   * Check if IAM integration is enabled.
   * 
   * @return true if enabled
   */
  public boolean isEnabled() {
    return iamConfig.isEnabled();
  }
}