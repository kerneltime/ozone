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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.identitymanagement.model.Role;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service for AWS IAM operations.
 */
@InterfaceAudience.Private
@Singleton
public class AWSIAMService {

  private static final Logger LOG = 
      LoggerFactory.getLogger(AWSIAMService.class);
  
  private static final Pattern ROLE_ARN_PATTERN = 
      Pattern.compile("arn:aws:iam::(\\d+):role/([\\w+=,.@-]+)");

  private final AWSIAMProvider iamProvider;

  @Inject
  public AWSIAMService(AWSIAMProvider iamProvider) {
    this.iamProvider = iamProvider;
  }

  /**
   * Check if a role exists in AWS IAM.
   * 
   * @param roleArn ARN of the role to check
   * @return true if role exists
   */
  public boolean validateRole(String roleArn) {
    if (!iamProvider.isEnabled()) {
      // If IAM integration is disabled, skip validation
      return true;
    }
    
    // Extract the role name from the ARN
    String roleName = extractRoleName(roleArn);
    if (roleName == null) {
      LOG.warn("Invalid role ARN format: {}", roleArn);
      return false;
    }
    
    try {
      // Attempt to get the role from AWS IAM
      GetRoleRequest request = new GetRoleRequest().withRoleName(roleName);
      Role role = iamProvider.getIAMClient().getRole(request).getRole();
      LOG.debug("Validated role exists in AWS IAM: {}", roleArn);
      return true;
    } catch (NoSuchEntityException e) {
      LOG.warn("Role does not exist in AWS IAM: {}", roleArn);
      return false;
    } catch (Exception e) {
      LOG.error("Error validating role with AWS IAM: {}", roleArn, e);
      // If there's an error communicating with AWS, fail safely
      return false;
    }
  }
  
  /**
   * Assume a role using AWS STS.
   * 
   * @param roleArn The role ARN to assume
   * @param roleSessionName The session name to use
   * @param durationSeconds The duration in seconds for the credentials
   * @return AWS temporary credentials
   * @throws Exception if role assumption fails
   */
  public Credentials assumeRole(String roleArn, String roleSessionName, 
      int durationSeconds) throws Exception {
    if (!iamProvider.isEnabled()) {
      throw new IllegalStateException("AWS IAM integration is disabled");
    }
    
    try {
      // Create the assume role request
      AssumeRoleRequest request = new AssumeRoleRequest()
          .withRoleArn(roleArn)
          .withRoleSessionName(roleSessionName)
          .withDurationSeconds(durationSeconds);
      
      // Execute the assume role request
      AssumeRoleResult result = 
          iamProvider.getSTSClient().assumeRole(request);
      
      LOG.info("Successfully assumed role {} with session name {}", 
          roleArn, roleSessionName);
      
      return result.getCredentials();
    } catch (Exception e) {
      LOG.error("Failed to assume role {} with session name {}: {}", 
          roleArn, roleSessionName, e.getMessage());
      throw e;
    }
  }
  
  /**
   * Extract the role name from a role ARN.
   * 
   * @param roleArn The role ARN
   * @return The role name, or null if invalid format
   */
  private String extractRoleName(String roleArn) {
    Matcher matcher = ROLE_ARN_PATTERN.matcher(roleArn);
    if (matcher.matches()) {
      return matcher.group(2);
    }
    return null;
  }

  /**
   * Check if IAM integration is enabled.
   * 
   * @return true if enabled
   */
  public boolean isEnabled() {
    return iamProvider.isEnabled();
  }
  
  /**
   * Get the default token duration from configuration.
   * 
   * @return default token duration in seconds
   */
  public int getDefaultTokenDuration() {
    return iamProvider.getConfig().getTokenDuration();
  }
  
  /**
   * Get the maximum token duration from configuration.
   * 
   * @return maximum token duration in seconds
   */
  public int getMaxTokenDuration() {
    return iamProvider.getConfig().getMaxTokenDuration();
  }
}