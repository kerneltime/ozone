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

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.s3.iam.AWSIAMService;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * STS endpoint implementation for AWS STS integration.
 * 
 * This endpoint handles AWS STS API requests at the "/sts" path.
 */
@Path("/sts")
public class STSEndpoint extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(STSEndpoint.class);

  private static final String DEFAULT_SESSION_TOKEN_DURATION = "3600";
  private static final int MAX_SESSION_DURATION_SECONDS = 43200; // 12 hours

  @Inject
  private OzoneClient client;
  
  @Inject
  private AWSIAMService iamService;
  
  // Metrics for tracking STS operations
  private S3GatewayMetrics metrics;

  @Override
  public void init() {
    // Initialize metrics
    metrics = getMetrics();
  }
  
  /**
   * Sets the Ozone client for testing.
   *
   * @param ozoneClient the Ozone client
   */
  public void setClient(OzoneClient ozoneClient) {
    this.client = ozoneClient;
  }
  

  /**
   * Implements the AWS STS AssumeRole API operation.
   * 
   * @param action The STS action (must be AssumeRole)
   * @param roleArn The ARN of the role to assume
   * @param roleSessionName A name for the session
   * @param durationSeconds The duration in seconds for temporary credentials
   * @return AssumeRole response with temporary credentials
   * @throws IOException If an error occurs during processing
   * @throws OS3Exception If parameters are invalid or operation fails
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  public Response assumeRole(
      @FormParam("Action") String action,
      @FormParam("RoleArn") String roleArn,
      @FormParam("RoleSessionName") String roleSessionName,
      @FormParam("DurationSeconds") String durationString) 
          throws IOException, OS3Exception {

    long startTime = System.nanoTime();
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put("action", action);
    auditMap.put("roleArn", roleArn);
    auditMap.put("roleSessionName", roleSessionName);
    auditMap.put("durationSeconds", durationString);

    try {
      // Validate parameters
      if (!"AssumeRole".equals(action)) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, action);
      }
      
      if (roleArn == null || roleArn.isEmpty()) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, 
            "Role ARN is required");
      }
      
      if (roleSessionName == null || roleSessionName.isEmpty()) {
        roleSessionName = "ozone-session-" + UUID.randomUUID();
      }
      
      // Parse duration seconds
      int defaultDuration = iamService.isEnabled() ?
          iamService.getDefaultTokenDuration() :
          Integer.parseInt(DEFAULT_SESSION_TOKEN_DURATION);
          
      int maxDuration = iamService.isEnabled() ?
          iamService.getMaxTokenDuration() :
          MAX_SESSION_DURATION_SECONDS;
      
      int durationSeconds = durationString != null ? 
          Integer.parseInt(durationString) : defaultDuration;
      
      if (durationSeconds < 900 || durationSeconds > maxDuration) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST,
            "DurationSeconds must be between 900 and " + maxDuration);
      }
      
      // Validate the role ARN with AWS IAM if integration is enabled
      if (!validateRole(roleArn)) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST,
            "Role does not exist or cannot be assumed: " + roleArn);
      }
      
      S3SecretValue s3Secret;
      String accessKey;
      String secretKey;
      String sessionToken;
      
      // If AWS IAM integration is enabled, use AWS STS to assume the role
      if (iamService.isEnabled()) {
        try {
          // Call AWS STS assumeRole API
          com.amazonaws.services.securitytoken.model.Credentials awsCreds = 
              iamService.assumeRole(roleArn, roleSessionName, durationSeconds);
          
          // Use the credentials returned by AWS STS
          accessKey = awsCreds.getAccessKeyId();
          secretKey = awsCreds.getSecretAccessKey();
          sessionToken = awsCreds.getSessionToken();
          
          // Store the AWS STS credentials in Ozone Manager
          s3Secret = S3SecretValue.of(accessKey, secretKey);
          
          LOG.info("Using AWS STS credentials for role: {}", roleArn);
        } catch (Exception e) {
          LOG.error("Failed to assume role with AWS STS: {}", roleArn, e);
          throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, 
              "Failed to assume role: " + e.getMessage());
        }
      } else {
        // Use our internal credential generation when AWS IAM integration is disabled
        String username = extractUsernameFromRoleArn(roleArn);
        s3Secret = generateS3Credentials(username);
        accessKey = s3Secret.getAwsAccessKey();
        secretKey = s3Secret.getAwsSecret();
        sessionToken = generateSessionToken();
        
        LOG.info("Using internally generated credentials for role: {}", roleArn);
      }
      
      // Store the credentials in Ozone Manager
      storeS3CredentialsInOM(roleArn, s3Secret);
      
      // Construct response
      Instant expiration = Instant.now().plus(durationSeconds, ChronoUnit.SECONDS);
      String response = formatAssumeRoleResponse(
          roleArn, 
          roleSessionName, 
          accessKey, 
          secretKey,
          sessionToken, 
          expiration);
      
      auditMap.put("result", "success");
      AUDIT.logWriteSuccess(
          buildAuditMessageForSuccess(S3GAction.STS_ASSUME_ROLE, auditMap));
      
      return Response.ok(response).build();
    } catch (IllegalArgumentException e) {
      LOG.error("Invalid request parameter", e);
      auditMap.put("error", e.getMessage());
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(S3GAction.STS_ASSUME_ROLE, auditMap, e));
      getMetrics().updateAssumeRoleFailureStats();
      throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, e.getMessage(), e);
    } catch (OS3Exception ex) {
      LOG.error("STS request failed", ex);
      auditMap.put("error", ex.getMessage());
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(S3GAction.STS_ASSUME_ROLE, auditMap, ex));
      getMetrics().updateAssumeRoleFailureStats();
      throw ex;
    } catch (Exception ex) {
      LOG.error("STS request failed with unexpected error", ex);
      auditMap.put("error", ex.getMessage());
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(S3GAction.STS_ASSUME_ROLE, auditMap, ex));
      getMetrics().updateAssumeRoleFailureStats();
      throw S3ErrorTable.newError(S3ErrorTable.INTERNAL_ERROR, ex.getMessage(), ex);
    } finally {
      getMetrics().updateAssumeRoleTime(System.nanoTime() - startTime);
    }
  }

  /**
   * Extracts the username from a role ARN.
   * 
   * @param roleArn The role ARN 
   * @return The extracted username
   */
  private String extractUsernameFromRoleArn(String roleArn) {
    // When AWS IAM integration is enabled, use the full role ARN as the username
    // to maintain a mapping between the AWS IAM role and Ozone credentials
    // This allows for better tracking and management
    
    // When not using AWS IAM integration, extract the role name from the ARN
    if (!iamService.isEnabled()) {
      String[] parts = roleArn.split("/");
      return parts[parts.length - 1];
    }
    
    return roleArn;
  }
  
  /**
   * Validates the role ARN with AWS IAM.
   * 
   * @param roleArn The role ARN to validate
   * @return True if valid, false otherwise
   */
  private boolean validateRole(String roleArn) {
    // If AWS IAM integration is enabled, validate the role ARN
    if (iamService.isEnabled()) {
      return iamService.validateRole(roleArn);
    }
    
    // Otherwise, assume the role is valid (for backward compatibility)
    return true;
  }
  
  /**
   * Generates S3 compatible credentials.
   * 
   * @param username The username to generate credentials for
   * @return S3SecretValue containing AWS access key and secret
   */
  private S3SecretValue generateS3Credentials(String username) {
    // Generate a UUID-based access key
    String accessKey = "AKIA" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    // Generate a secure random secret key
    String secretKey = UUID.randomUUID().toString().replace("-", "") + 
                       UUID.randomUUID().toString().replace("-", "");
    
    // Use the factory method to create S3SecretValue
    return S3SecretValue.of(accessKey, secretKey);
  }
  
  /**
   * Stores the S3 credentials in the Ozone Manager.
   * 
   * @param username The username associated with the credentials
   * @param s3Secret The S3 credentials to store
   * @throws IOException If storing the credentials fails
   */
  private void storeS3CredentialsInOM(String username, S3SecretValue s3Secret) 
      throws IOException {
    ClientProtocol clientProtocol = client.getObjectStore().getClientProxy();
    // Use setS3Secret method to store the credentials in OM
    s3Secret = clientProtocol.setS3Secret(s3Secret.getAwsAccessKey(), 
                                         s3Secret.getAwsSecret());
    LOG.debug("Stored S3 credentials for user {} with access key {}", 
             username, s3Secret.getAwsAccessKey());
  }
  
  /**
   * Generates a session token.
   * 
   * @return A session token string
   */
  private String generateSessionToken() {
    // In a real implementation, this would be a properly signed token
    // For now, we generate a random UUID-based string
    return "FQoG" + UUID.randomUUID().toString().replace("-", "") +
           UUID.randomUUID().toString().replace("-", "");
  }
  
  /**
   * Formats the AssumeRole XML response.
   * 
   * @param roleArn The role ARN that was assumed
   * @param roleSessionName The session name
   * @param accessKey The AWS access key
   * @param secretKey The AWS secret key
   * @param sessionToken The session token
   * @param expiration The expiration time
   * @return XML response string
   */
  private String formatAssumeRoleResponse(
      String roleArn, 
      String roleSessionName,
      String accessKey, 
      String secretKey, 
      String sessionToken,
      Instant expiration) {
    
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
           "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n" +
           "  <AssumeRoleResult>\n" +
           "    <AssumedRoleUser>\n" +
           "      <Arn>" + roleArn + "</Arn>\n" +
           "      <AssumedRoleId>" + accessKey + ":" + roleSessionName + "</AssumedRoleId>\n" +
           "    </AssumedRoleUser>\n" +
           "    <Credentials>\n" +
           "      <AccessKeyId>" + accessKey + "</AccessKeyId>\n" +
           "      <SecretAccessKey>" + secretKey + "</SecretAccessKey>\n" +
           "      <SessionToken>" + sessionToken + "</SessionToken>\n" +
           "      <Expiration>" + expiration.toString() + "</Expiration>\n" +
           "    </Credentials>\n" +
           "  </AssumeRoleResult>\n" +
           "  <ResponseMetadata>\n" +
           "    <RequestId>" + UUID.randomUUID().toString() + "</RequestId>\n" +
           "  </ResponseMetadata>\n" +
           "</AssumeRoleResponse>";
  }
}