/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Objects;

/**
 * Disables the delegate rule if the given system property matches a specific
 * value.
 */
public class DisableOnProperty implements TestRule {

  private final TestRule delegate;
  private final boolean enabled;

  public DisableOnProperty(TestRule delegate, String key, String value) {
    this.delegate = delegate;
    enabled = !Objects.equals(value, System.getProperty(key, ""));
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return enabled ? delegate.apply(base, description) : base;
  }
}
