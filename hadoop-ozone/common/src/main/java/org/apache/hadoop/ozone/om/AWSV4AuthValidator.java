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

package org.apache.hadoop.ozone.om;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.ozone.security.HmacSha256;
import org.apache.hadoop.util.StringUtils;
import org.apache.kerby.util.Hex;

/**
 * AWS v4 authentication payload validator. For more details refer to AWS
 * documentation https://docs.aws.amazon.com/general/latest/gr/
 * sigv4-create-canonical-request.html.
 **/
public final class AWSV4AuthValidator {

  private AWSV4AuthValidator() {
  }

  public static String hash(String payload) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(payload.getBytes(StandardCharsets.UTF_8));
    return String.format("%064x", new java.math.BigInteger(1, md.digest()));
  }

  private static byte[] sign(byte[] key, String msg) {
    try {
      return HmacSha256.sign(new SecretKeySpec(key, HmacSha256.ALGORITHM), msg);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(
          "Failed to initialize Mac instance that implements the " + HmacSha256.ALGORITHM + " algorithm.", e);
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] getSigningKey(String key, String strToSign) {
    String credentialScope = StringUtils.split(strToSign, '\n')[2];
    return getSigningKeyFromCredentialScope(key, credentialScope);
  }

  private static byte[] getSigningKeyFromCredentialScope(String key, String credentialScope) {
    String[] signData = StringUtils.split(credentialScope, '/');
    String dateStamp = signData[0];
    String regionName = signData[1];
    String serviceName = signData[2];
    byte[] kDate = sign(("AWS4" + key)
        .getBytes(StandardCharsets.UTF_8), dateStamp);
    byte[] kRegion = sign(kDate, regionName);
    byte[] kService = sign(kRegion, serviceName);
    byte[] kSigning = sign(kService, "aws4_request");
    return kSigning;
  }

  public static boolean validateRequest(String strToSign, String signature,
      String userKey) {
    String expectedSignature = Hex.encode(sign(getSigningKey(userKey,
        strToSign), strToSign));
    // Constant time, so that how long the comparison takes does not tell how much of a forged signature is right.
    // Hex digits may come in either case.
    return signature != null && MessageDigest.isEqual(expectedSignature.getBytes(StandardCharsets.UTF_8),
        signature.toLowerCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8));
  }
}
