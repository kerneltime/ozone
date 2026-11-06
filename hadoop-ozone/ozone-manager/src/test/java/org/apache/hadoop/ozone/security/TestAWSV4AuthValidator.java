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

package org.apache.hadoop.ozone.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.ozone.om.AWSV4AuthValidator;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.slf4j.event.Level;

/**
 * Tests AWS V4 Auth Validator.
 */
public class TestAWSV4AuthValidator {

  private static final String STRING_TO_SIGN = "AWS4-HMAC-SHA256\n"
      + "20150830T123600Z\n"
      + "20150830/us-east-1/iam/aws4_request\n"
      + "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";
  private static final String SIGNATURE = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7";
  private static final String SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {
            "AWS4-HMAC-SHA256\n" +
                "20190221T002037Z\n" +
                "20190221/us-west-1/s3/aws4_request\n" +
                "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d" +
                "91851294efc47d",
            "56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf" +
                "4765f46a14cd745ad",
            "dbaksbzljandlkandlsd",
            true
        },
        {
            "AWS4-HMAC-SHA256\n" +
                "20150830T123600Z\n" +
                "20150830/us-east-1/iam/aws4_request\n" +
                "f536975d06c0309214f805bb90ccff089219ecd68b2" +
                "577efef23edd43b7e1a59",
            "5d672d79c15b13162d9279b0855cfba" +
                "6789a8edb4c82c400e06b5924a6f2b5d7",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            true
        },
        // Invalid signature
        {
            "AWS4-HMAC-SHA256\n" +
                "20150830T123600Z\n" +
                "20150830/us-east-1/iam/aws4_request\n" +
                "f536975d06c0309214f805bb90ccff089219ecd68b2" +
                "577efef23edd43b7e1a59",
            "5d672d79c15b13162d9279b0855cfba" +
                "6789a8edb4c82c400e06b5924a6f2b5d8",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            false
        }
    });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testValidateRequest(String stringToSign, String sign,
                                  String accessKey, Boolean testResult) {
    assertEquals(testResult, AWSV4AuthValidator.validateRequest(
        stringToSign, sign, accessKey));
  }

  /** The signature is hex, and hex digits may come in either case. */
  @Test
  public void testUpperCaseSignatureVerifies() {
    assertTrue(AWSV4AuthValidator.validateRequest(STRING_TO_SIGN, SIGNATURE.toUpperCase(Locale.ROOT), SECRET));
  }

  @ParameterizedTest
  @ValueSource(strings = {"",
      "zz672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7",
      "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d",
      "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d70",
      "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d700"})
  @NullSource
  public void testMalformedSignatureIsRefusedWithoutException(String signature) {
    assertFalse(AWSV4AuthValidator.validateRequest(STRING_TO_SIGN, signature, SECRET));
  }

  /** A comparison that stops at the first differing byte tells a caller how much of a forged signature is right. */
  @Test
  public void testSignaturesAreComparedInConstantTime() {
    try (MockedStatic<MessageDigest> messageDigest = mockStatic(MessageDigest.class, CALLS_REAL_METHODS)) {
      assertTrue(AWSV4AuthValidator.validateRequest(STRING_TO_SIGN, SIGNATURE, SECRET));
      messageDigest.verify(() -> MessageDigest.isEqual(any(byte[].class), any(byte[].class)));
    }
  }

  /** The signing key signs any request of its day, region and service, so it must not reach the log. */
  @Test
  public void testSigningKeyIsNotLogged() {
    String signingKey = Hex.encodeHexString(AWSV4AuthValidator.getSigningKey(SECRET, STRING_TO_SIGN));
    GenericTestUtils.setLogLevel(AWSV4AuthValidator.class, Level.DEBUG);
    try (LogCapturer logs = LogCapturer.captureLogs(AWSV4AuthValidator.class)) {
      AWSV4AuthValidator.getSigningKey(SECRET, STRING_TO_SIGN);
      assertTrue(AWSV4AuthValidator.validateRequest(STRING_TO_SIGN, SIGNATURE, SECRET));

      assertFalse(logs.getOutput().toLowerCase(Locale.ROOT).contains(signingKey), "the signing key was logged");
    }
  }
}
