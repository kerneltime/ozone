# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Test HDDS-13405: ozone admin container create fails fast without kinit
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        2 minutes
Suite Setup         Setup test environment

*** Variables ***
${SCM}       scm

*** Keywords ***
Setup test environment
    # Ensure we start with a clean authentication state
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab

Test container create with authentication
    [arguments]    ${should_succeed}=True
    ${output} =    Execute And Ignore Error    ozone admin container create
    IF    ${should_succeed}
        Should contain    ${output}    is created
    ELSE
        Should contain    ${output}    Access denied
    END

Test container create without authentication
    [arguments]    ${should_fail_fast}=True
    # Clear any existing authentication
    Execute    kdestroy 2>/dev/null || true
    ${output} =    Execute And Ignore Error    timeout 30 ozone admin container create
    IF    ${should_fail_fast}
        # Should fail quickly with access denied, not hang
        Should contain    ${output}    Access denied
        Should not contain    ${output}    timeout
    ELSE
        # Old behavior would hang indefinitely
        Should contain    ${output}    timeout
    END

Test container create with invalid credentials
    [arguments]    ${should_fail_fast}=True
    # Try with invalid keytab
    Execute    kdestroy 2>/dev/null || true
    ${output} =    Execute And Ignore Error    timeout 30 kinit -k -t /nonexistent.keytab testuser@EXAMPLE.COM 2>&1 || true
    ${output} =    Execute And Ignore Error    timeout 30 ozone admin container create
    IF    ${should_fail_fast}
        # Should fail quickly with access denied, not hang
        Should contain    ${output}    Access denied
        Should not contain    ${output}    timeout
    ELSE
        # Old behavior would hang indefinitely
        Should contain    ${output}    timeout
    END

*** Test Cases ***
Container create succeeds with proper authentication
    [Documentation]    Test that container create works when properly authenticated
    [Tags]    authentication    positive
    Test container create with authentication    should_succeed=True

Container create fails fast without authentication
    [Documentation]    Test that container create fails quickly without kinit (HDDS-13405 fix)
    [Tags]    authentication    negative    HDDS-13405
    Pass Execution If    '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    Test container create without authentication    should_fail_fast=True

Container create fails fast with invalid credentials
    [Documentation]    Test that container create fails quickly with invalid credentials (HDDS-13405 fix)
    [Tags]    authentication    negative    HDDS-13405
    Pass Execution If    '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    Test container create with invalid credentials    should_fail_fast=True

Container create timeout behavior verification
    [Documentation]    Verify that the command doesn't hang indefinitely without authentication
    [Tags]    authentication    timeout    HDDS-13405
    Pass Execution If    '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    # Clear authentication and test with a short timeout
    Execute    kdestroy 2>/dev/null || true
    ${start_time} =    Get Time    epoch
    ${output} =    Execute And Ignore Error    timeout 10 ozone admin container create
    ${end_time} =    Get Time    epoch
    ${duration} =    Evaluate    ${end_time} - ${start_time}
    # Should fail within 10 seconds, not hang
    Should Be True    ${duration} < 10
    Should contain    ${output}    Access denied

Reset authentication after tests
    [Documentation]    Reset authentication state after negative tests
    [Tags]    cleanup
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab 