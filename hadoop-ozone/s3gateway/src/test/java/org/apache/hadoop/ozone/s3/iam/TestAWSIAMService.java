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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Date;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.identitymanagement.model.Role;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

/**
 * Test for AWSIAMService.
 */
public class TestAWSIAMService {

  private AWSIAMService iamService;
  private AWSIAMProvider iamProvider;
  private AWSIAMConfig iamConfig;
  private AmazonIdentityManagement iamClient;
  private AWSSecurityTokenService stsClient;

  @BeforeEach
  public void setUp() {
    // Mock IAM configuration
    iamConfig = new AWSIAMConfig();
    iamConfig.setEnabled(true);
    iamConfig.setTokenDuration(3600);
    iamConfig.setMaxTokenDuration(43200);
    
    // Mock IAM provider
    iamProvider = mock(AWSIAMProvider.class);
    when(iamProvider.isEnabled()).thenReturn(true);
    when(iamProvider.getConfig()).thenReturn(iamConfig);
    
    // Mock IAM client
    iamClient = mock(AmazonIdentityManagement.class);
    when(iamProvider.getIAMClient()).thenReturn(iamClient);
    
    // Mock STS client
    stsClient = mock(AWSSecurityTokenService.class);
    when(iamProvider.getSTSClient()).thenReturn(stsClient);
    
    // Create IAM service with mock provider
    iamService = new AWSIAMService(iamProvider);
  }
  
  @Test
  public void testValidateRoleSuccess() {
    // Mock successful role validation
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    
    GetRoleResult getRoleResult = new GetRoleResult();
    Role role = new Role();
    role.setRoleName("test-role");
    role.setArn(roleArn);
    getRoleResult.setRole(role);
    
    when(iamClient.getRole(any(GetRoleRequest.class))).thenReturn(getRoleResult);
    
    assertTrue(iamService.validateRole(roleArn));
  }
  
  @Test
  public void testValidateRoleNotFound() {
    // Mock role not found
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    
    when(iamClient.getRole(any(GetRoleRequest.class)))
        .thenThrow(new NoSuchEntityException("Role not found"));
    
    assertFalse(iamService.validateRole(roleArn));
  }
  
  @Test
  public void testValidateRoleInvalidFormat() {
    // Test with invalid role ARN format
    String roleArn = "invalid-role-arn";
    
    assertFalse(iamService.validateRole(roleArn));
  }
  
  @Test
  public void testAssumeRoleSuccess() throws Exception {
    // Mock successful role assumption
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";
    int durationSeconds = 3600;
    
    AssumeRoleResult assumeRoleResult = new AssumeRoleResult();
    Credentials credentials = new Credentials();
    credentials.setAccessKeyId("AKIA1234567890");
    credentials.setSecretAccessKey("secretKey1234567890");
    credentials.setSessionToken("sessionToken1234567890");
    credentials.setExpiration(new Date(System.currentTimeMillis() + 3600000));
    assumeRoleResult.setCredentials(credentials);
    
    when(stsClient.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(assumeRoleResult);
    
    Credentials result = 
        iamService.assumeRole(roleArn, roleSessionName, durationSeconds);
    
    assertNotNull(result);
    assertEquals("AKIA1234567890", result.getAccessKeyId());
    assertEquals("secretKey1234567890", result.getSecretAccessKey());
    assertEquals("sessionToken1234567890", result.getSessionToken());
  }
  
  @Test
  public void testIsEnabledAndDuration() {
    assertTrue(iamService.isEnabled());
    assertEquals(3600, iamService.getDefaultTokenDuration());
    assertEquals(43200, iamService.getMaxTokenDuration());
    
    // Test when IAM integration is disabled
    when(iamProvider.isEnabled()).thenReturn(false);
    assertFalse(iamService.isEnabled());
  }
}