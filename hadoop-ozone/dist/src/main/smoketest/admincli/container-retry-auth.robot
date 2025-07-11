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
Documentation       Test HDDS-13405: Retry behavior and access control exception handling
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        3 minutes
Suite Setup         Setup test environment

*** Variables ***
${SCM}       scm

*** Keywords ***
Setup test environment
    # Ensure we start with a clean authentication state
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab

Test retry behavior with authentication exceptions
    [arguments]    ${should_fail_fast}=True
    # Clear authentication to trigger access control exceptions
    Execute    kdestroy 2>/dev/null || true
    ${start_time} =    Get Time    epoch
    ${output} =    Execute And Ignore Error    timeout 60 ozone admin container create
    ${end_time} =    Get Time    epoch
    ${duration} =    Evaluate    ${end_time} - ${start_time}
    
    IF    ${should_fail_fast}
        # With the fix, should fail quickly (within 30 seconds)
        Should Be True    ${duration} < 30
        Should contain    ${output}    Access denied
        Log    Command failed fast as expected (duration: ${duration}s)
    ELSE
        # Old behavior would hang indefinitely
        Should Be True    ${duration} >= 60
        Should contain    ${output}    timeout
        Log    Command hung as expected (duration: ${duration}s)
    END

Test multiple authentication scenarios
    [arguments]    ${scenario}
    IF    '${scenario}' == 'no_auth'
        Execute    kdestroy 2>/dev/null || true
    ELSE IF    '${scenario}' == 'invalid_auth'
        Execute    kdestroy 2>/dev/null || true
        Execute    kinit -k -t /nonexistent.keytab testuser@EXAMPLE.COM 2>&1 || true
    ELSE IF    '${scenario}' == 'valid_auth'
        Kinit test user    testuser    testuser.keytab
    END
    
    ${output} =    Execute And Ignore Error    timeout 30 ozone admin container create
    [return]    ${output}

Verify access control exception handling
    [arguments]    ${output}
    # Should contain access denied message, not hang indefinitely
    Should contain    ${output}    Access denied
    Should not contain    ${output}    timeout
    Should not contain    ${output}    Unable to obtain complete CA list

*** Test Cases ***
Container create fails fast on access control exception
    [Documentation]    Test that container create fails quickly when access control exceptions occur (HDDS-13405 fix)
    [Tags]    authentication    retry    HDDS-13405
    Pass Execution If    '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    Test retry behavior with authentication exceptions    should_fail_fast=True

Container create with no authentication fails fast
    [Documentation]    Test container create behavior with no authentication
    [Tags]    authentication    negative    HDDS-13405
    Pass Execution If    '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    ${output} =    Test multiple authentication scenarios    no_auth
    Verify access control exception handling    ${output}

Container create with invalid authentication fails fast
    [Documentation]    Test container create behavior with invalid authentication
    [Tags]    authentication    negative    HDDS-13405
    Pass Execution If    '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    ${output} =    Test multiple authentication scenarios    invalid_auth
    Verify access control exception handling    ${output}

Container create with valid authentication succeeds
    [Documentation]    Test container create behavior with valid authentication
    [Tags]    authentication    positive
    ${output} =    Test multiple authentication scenarios    valid_auth
    Should contain    ${output}    is created

Retry policy behavior verification
    [Documentation]    Verify that the retry policy correctly handles access control exceptions
    [Tags]    authentication    retry    HDDS-13405
    Pass Execution If    '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    # Test that the command doesn't retry forever on auth exceptions
    Execute    kdestroy 2>/dev/null || true
    ${start_time} =    Get Time    epoch
    ${output} =    Execute And Ignore Error    timeout 45 ozone admin container create
    ${end_time} =    Get Time    epoch
    ${duration} =    Evaluate    ${end_time} - ${start_time}
    
    # Should fail within reasonable time (not hang for 45+ seconds)
    Should Be True    ${duration} < 45
    Should contain    ${output}    Access denied
    Log    Retry policy correctly failed fast on auth exception (duration: ${duration}s)

Cleanup authentication state
    [Documentation]    Reset authentication state after tests
    [Tags]    cleanup
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab 