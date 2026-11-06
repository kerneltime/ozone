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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ACCESS_KEY_ID;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.SIGNATURE_DOES_NOT_MATCH;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapOS3Exception;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditLoggerType;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.AWSV4AuthValidator;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.signature.AuthOperation;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;
import org.apache.hadoop.ozone.s3.signature.SignatureProcessor;
import org.apache.hadoop.ozone.s3.signature.StringToSignProducer;
import org.apache.hadoop.ozone.s3.util.AuditUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter used to construct string to sign from unfiltered request.
 * It should be executed before all other filters as the original
 * could be enriched later.
 */

@Provider
@PreMatching
@Priority(AuthorizationFilter.PRIORITY)
public class AuthorizationFilter implements ContainerRequestFilter {
  public static final int PRIORITY = 50;

  private static final Logger LOG = LoggerFactory.getLogger(
      AuthorizationFilter.class);

  private static final AuditLogger AUDIT = new AuditLogger(AuditLoggerType.S3GLOGGER);

  @Inject
  private SignatureProcessor signatureProcessor;

  @Inject
  private SignatureInfo signatureInfo;

  /** Set when the gateway verifies signatures itself, with Ozone security disabled. */
  @Inject
  private StaticCredentialStore staticCredentials;

  @Override
  public void filter(ContainerRequestContext context) throws
      IOException {
    try {
      signatureInfo.initialize(signatureProcessor.parseSignature());
      if (signatureInfo.getVersion() == Version.V4) {
        signatureInfo.setStrToSign(
            StringToSignProducer.createSignatureBase(signatureInfo, context));
      } else {
        LOG.debug("Unsupported AWS signature version: {}",
            signatureInfo.getVersion());
        throw S3ErrorTable.newError(S3_AUTHINFO_CREATION_ERROR, String.valueOf(signatureInfo.getVersion()));
      }

      String awsAccessId = signatureInfo.getAwsAccessId();
      // ONLY validate aws access id when needed.
      if (awsAccessId == null || awsAccessId.equals("")) {
        LOG.debug("Malformed s3 header. awsAccessID: {}", awsAccessId);
        throw S3ErrorTable.newError(ACCESS_DENIED);
      }
      if (staticCredentials != null) {
        verifySignature(context);
      }
    } catch (OS3Exception ex) {
      LOG.debug("Error during Client Creation: ", ex);
      throw wrapOS3Exception(ex);
    } catch (Exception e) {
      // For any other critical errors during object creation throw Internal
      // error.
      LOG.debug("Error during Client Creation: ", e);
      throw wrapOS3Exception(S3ErrorTable.newError(INTERNAL_ERROR, e));
    }
  }

  /**
   * With security disabled OM verifies no signature, so the gateway verifies it against its static credentials,
   * before any endpoint runs.
   */
  private void verifySignature(ContainerRequestContext context) throws OS3Exception {
    String secret = staticCredentials.getSecret(signatureInfo.getAwsAccessId());
    S3ErrorTable error = null;
    if (secret == null) {
      error = INVALID_ACCESS_KEY_ID;
    } else if (!AWSV4AuthValidator.validateRequest(signatureInfo.getStringToSign(), signatureInfo.getSignature(),
        secret)) {
      error = SIGNATURE_DOES_NOT_MATCH;
    }
    if (error != null) {
      OS3Exception ex = S3ErrorTable.newError(error);
      auditRefusal(context, ex);
      throw ex;
    }
    // The chunk signatures of a streaming upload are verified with the same key.
    signatureInfo.setSigningKey(AWSV4AuthValidator.getSigningKey(secret, signatureInfo.getStringToSign()));
  }

  private void auditRefusal(ContainerRequestContext context, OS3Exception error) {
    Map<String, String> params = AuditUtils.getAuditParameters(context);
    // A presigned request carries its signature, and any session token, as query parameters.
    params.keySet().removeIf(name -> "X-Amz-Signature".equalsIgnoreCase(name)
        || "X-Amz-Security-Token".equalsIgnoreCase(name));
    AUDIT.logAuthFailure(new AuditMessage.Builder()
        .setUser(signatureInfo.getAwsAccessId())
        .atIp(AuditUtils.getClientIpAddress(context))
        .forOperation(AuthOperation.fromContext(context))
        .withParams(params)
        .withResult(AuditEventStatus.FAILURE)
        .withException(error)
        .build());
  }

  @VisibleForTesting
  public void setSignatureParser(SignatureProcessor awsSignatureProcessor) {
    this.signatureProcessor = awsSignatureProcessor;
  }

  @VisibleForTesting
  public void setSignatureInfo(SignatureInfo signatureInfo) {
    this.signatureInfo = signatureInfo;
  }

  @VisibleForTesting
  public SignatureInfo getSignatureInfo() {
    return signatureInfo;
  }

  @VisibleForTesting
  public void setStaticCredentials(StaticCredentialStore staticCredentials) {
    this.staticCredentials = staticCredentials;
  }

}
