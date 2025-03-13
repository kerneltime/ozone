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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import javax.ws.rs.core.Response;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.iam.AWSIAMService;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test STS endpoint implementation.
 */
public class TestSTSEndpoint {

  private STSEndpoint stsEndpoint;
  private OzoneClient ozoneClient;
  private ObjectStore objectStore;
  private ClientProtocol clientProtocol;
  private AuditLogger auditLogger;
  private S3GatewayMetrics metrics;
  private AWSIAMService iamService;
  
  @BeforeEach
  public void setUp() throws IOException {
    // Create mocks
    ozoneClient = mock(OzoneClient.class);
    objectStore = mock(ObjectStore.class);
    clientProtocol = mock(ClientProtocol.class);
    auditLogger = mock(AuditLogger.class);
    metrics = mock(S3GatewayMetrics.class);
    iamService = mock(AWSIAMService.class);
    
    // Setup endpoint with mocks
    stsEndpoint = new STSEndpoint();
    stsEndpoint.setClient(ozoneClient);
    // Use reflection to set the audit logger, metrics, and IAM service
    setField(stsEndpoint, "AUDIT", auditLogger);
    setField(stsEndpoint, "metrics", metrics);
    setField(stsEndpoint, "iamService", iamService);
    
    // Configure IAM service mock defaults
    when(iamService.isEnabled()).thenReturn(false); // Disabled by default in tests
    when(iamService.getDefaultTokenDuration()).thenReturn(3600);
    when(iamService.getMaxTokenDuration()).thenReturn(43200);
    when(iamService.validateRole(anyString())).thenReturn(true); // Role always valid by default
    
    // Setup mock behavior
    when(ozoneClient.getObjectStore()).thenReturn(objectStore);
    when(objectStore.getClientProxy()).thenReturn(clientProtocol);
    
    // Mock setS3Secret to return the input values
    when(clientProtocol.setS3Secret(
        org.mockito.ArgumentMatchers.anyString(), 
        org.mockito.ArgumentMatchers.anyString()))
        .thenAnswer(invocation -> {
          Object[] args = invocation.getArguments();
          return S3SecretValue.of((String) args[0], (String) args[1]);
        });
  }
  
  @Test
  public void testAssumeRoleWithValidRequest() throws Exception {
    // Test parameters
    String action = "AssumeRole";
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";
    String durationSeconds = "3600";
    
    // Set up metrics verification
    when(metrics.updateAssumeRoleTime(anyLong()))
        .thenReturn(1000L);
    
    // Call the endpoint
    Response response = stsEndpoint.assumeRole(
        action, roleArn, roleSessionName, durationSeconds);
    
    // Verify the response
    assertNotNull(response);
    assertEquals(200, response.getStatus());
    
    // Verify the response contains XML with credentials
    String responseStr = response.getEntity().toString();
    assertTrue(responseStr.contains("<AssumeRoleResponse"));
    assertTrue(responseStr.contains("<AccessKeyId>"));
    assertTrue(responseStr.contains("<SecretAccessKey>"));
    assertTrue(responseStr.contains("<SessionToken>"));
    assertTrue(responseStr.contains("<Expiration>"));
    assertTrue(responseStr.contains(roleArn));
    
    // Verify metrics were updated
    verify(metrics).updateAssumeRoleTime(anyLong());
  }
  
  @Test
  public void testAssumeRoleWithInvalidAction() throws Exception {
    // Test with invalid action
    String action = "InvalidAction";
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";
    String durationSeconds = "3600";
    
    try {
      stsEndpoint.assumeRole(action, roleArn, roleSessionName, durationSeconds);
      // Should not reach here
      throw new AssertionError("Expected OS3Exception was not thrown");
    } catch (OS3Exception e) {
      assertEquals("InvalidRequest", e.getCode());
      
      // Verify metrics were updated for failure
      org.mockito.Mockito.verify(metrics).updateAssumeRoleFailureStats();
    }
  }
  
  @Test
  public void testAssumeRoleWithMissingRoleArn() throws Exception {
    // Test with missing roleArn
    String action = "AssumeRole";
    String roleArn = null;
    String roleSessionName = "test-session";
    String durationSeconds = "3600";
    
    try {
      stsEndpoint.assumeRole(action, roleArn, roleSessionName, durationSeconds);
      // Should not reach here
      throw new AssertionError("Expected OS3Exception was not thrown");
    } catch (OS3Exception e) {
      assertEquals("InvalidRequest", e.getCode());
      
      // Verify metrics were updated for failure
      verify(metrics).updateAssumeRoleFailureStats();
    }
  }
  
  @Test
  public void testAssumeRoleWithInvalidDuration() throws Exception {
    // Test with invalid duration (too short)
    String action = "AssumeRole";
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";
    String durationSeconds = "800"; // Less than 900 seconds
    
    try {
      stsEndpoint.assumeRole(action, roleArn, roleSessionName, durationSeconds);
      // Should not reach here
      throw new AssertionError("Expected OS3Exception was not thrown");
    } catch (OS3Exception e) {
      assertEquals("InvalidRequest", e.getCode());
      
      // Verify metrics were updated for failure
      verify(metrics).updateAssumeRoleFailureStats();
    }
    
    // Test with invalid duration (too long)
    durationSeconds = "50000"; // More than max duration
    
    try {
      stsEndpoint.assumeRole(action, roleArn, roleSessionName, durationSeconds);
      // Should not reach here
      throw new AssertionError("Expected OS3Exception was not thrown");
    } catch (OS3Exception e) {
      assertEquals("InvalidRequest", e.getCode());
      
      // Verify metrics were updated for failure
      verify(metrics, times(2))
          .updateAssumeRoleFailureStats();
    }
  }
  
  @Test
  public void testAssumeRoleGeneratesUniqueCredentials() throws Exception {
    // Test parameters
    String action = "AssumeRole";
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";
    String durationSeconds = "3600";
    
    // Set up metrics verification
    when(metrics.updateAssumeRoleTime(anyLong()))
        .thenReturn(1000L);
    
    // Call the endpoint twice
    Response response1 = stsEndpoint.assumeRole(
        action, roleArn, roleSessionName, durationSeconds);
    Response response2 = stsEndpoint.assumeRole(
        action, roleArn, roleSessionName, durationSeconds);
    
    // Verify both responses have unique credentials
    String responseStr1 = response1.getEntity().toString();
    String responseStr2 = response2.getEntity().toString();
    
    // Extract and compare access keys - they should be different
    String accessKey1 = extractAccessKey(responseStr1);
    String accessKey2 = extractAccessKey(responseStr2);
    assertNotNull(accessKey1);
    assertNotNull(accessKey2);
    assertTrue(!accessKey1.equals(accessKey2), 
        "Generated access keys should be unique");
    
    // Verify metrics were updated twice
    verify(metrics, times(2)).updateAssumeRoleTime(anyLong());
  }
  
  @Test
  public void testAssumeRoleWithAWSIAMIntegration() throws Exception {
    // Test parameters
    String action = "AssumeRole";
    String roleArn = "arn:aws:iam::123456789012:role/test-role";
    String roleSessionName = "test-session";
    String durationSeconds = "3600";
    
    // Enable AWS IAM integration
    when(iamService.isEnabled()).thenReturn(true);
    
    // Mock the AWS STS credentials
    com.amazonaws.services.securitytoken.model.Credentials awsCredentials = 
        new com.amazonaws.services.securitytoken.model.Credentials();
    awsCredentials.setAccessKeyId("AKIA_AWS_KEY");
    awsCredentials.setSecretAccessKey("AWS_SECRET_KEY");
    awsCredentials.setSessionToken("AWS_SESSION_TOKEN");
    awsCredentials.setExpiration(
        new Date(System.currentTimeMillis() + 3600000));
    
    // Mock assumeRole to return AWS credentials
    when(iamService.assumeRole(
        eq(roleArn), 
        eq(roleSessionName), 
        eq(3600)))
        .thenReturn(awsCredentials);
    
    // Call the endpoint
    Response response = stsEndpoint.assumeRole(
        action, roleArn, roleSessionName, durationSeconds);
    
    // Verify the response
    assertNotNull(response);
    assertEquals(200, response.getStatus());
    
    // Verify the response contains AWS credentials
    String responseStr = response.getEntity().toString();
    assertTrue(responseStr.contains("<AccessKeyId>AKIA_AWS_KEY</AccessKeyId>"));
    assertTrue(responseStr.contains("<SecretAccessKey>AWS_SECRET_KEY</SecretAccessKey>"));
    assertTrue(responseStr.contains("<SessionToken>AWS_SESSION_TOKEN</SessionToken>"));
    
    // Verify IAM service was called
    verify(iamService).assumeRole(
        eq(roleArn), 
        eq(roleSessionName), 
        eq(3600));
    
    // Verify metrics were updated
    verify(metrics).updateAssumeRoleTime(anyLong());
  }
  
  @Test
  public void testAssumeRoleWithInvalidRoleInAWS() throws Exception {
    // Test parameters
    String action = "AssumeRole";
    String roleArn = "arn:aws:iam::123456789012:role/invalid-role";
    String roleSessionName = "test-session";
    String durationSeconds = "3600";
    
    // Enable AWS IAM integration
    when(iamService.isEnabled()).thenReturn(true);
    
    // Mock role validation to fail
    when(iamService.validateRole(roleArn)).thenReturn(false);
    
    try {
      stsEndpoint.assumeRole(action, roleArn, roleSessionName, durationSeconds);
      // Should not reach here
      throw new AssertionError("Expected OS3Exception was not thrown");
    } catch (OS3Exception e) {
      assertEquals("InvalidRequest", e.getCode());
      assertTrue(e.getMessage().contains("Role does not exist"));
      
      // Verify metrics were updated for failure
      verify(metrics).updateAssumeRoleFailureStats();
    }
  }
  
  /**
   * Helper method to extract access key from response XML.
   * 
   * @param responseXml The XML response string
   * @return The access key value
   */
  private String extractAccessKey(String responseXml) {
    // Simple string extraction for test purposes
    int start = responseXml.indexOf("<AccessKeyId>") + "<AccessKeyId>".length();
    int end = responseXml.indexOf("</AccessKeyId>");
    if (start > 0 && end > start) {
      return responseXml.substring(start, end);
    }
    return null;
  }
  
  /**
   * Helper to set mock objects for testing.
   */
  public void setClient(OzoneClient client) {
    this.ozoneClient = client;
  }
  
  /**
   * Utility method to set a private or final field using reflection.
   *
   * @param object The object to modify
   * @param fieldName The name of the field to set
   * @param value The value to set
   * @throws RuntimeException if the field cannot be set
   */
  private void setField(Object object, String fieldName, Object value) {
    try {
      // Get the field from the parent class if needed
      Field field = object.getClass().getSuperclass().getDeclaredField(fieldName);
      field.setAccessible(true);
      
      // Remove final modifier
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
      
      // Set the field
      field.set(object, value);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set field " + fieldName, e);
    }
  }
  
}