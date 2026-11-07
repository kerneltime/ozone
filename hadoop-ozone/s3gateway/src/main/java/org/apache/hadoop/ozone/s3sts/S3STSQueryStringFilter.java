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

package org.apache.hadoop.ozone.s3sts;

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.STS_VALIDATION_ERROR;

import javax.annotation.Priority;
import javax.enterprise.inject.Typed;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.ozone.s3.QueryStringFilter;
import org.apache.hadoop.ozone.s3.exception.OSTSException;

/**
 * The {@link QueryStringFilter} of the STS endpoint. It refuses a repeated parameter with an {@link OSTSException},
 * which the STS exception mapper writes in the STS error format that STS clients parse.
 *
 * <p>Its CDI bean type is only this class. Otherwise a lookup of {@link QueryStringFilter}, which the S3 endpoint
 * makes, could resolve to this subclass and answer S3 clients in the STS format.
 */
@Provider
@PreMatching
@Priority(QueryStringFilter.PRIORITY)
@Typed(S3STSQueryStringFilter.class)
public class S3STSQueryStringFilter extends QueryStringFilter {

  @Override
  protected RuntimeException repeatedParameter(String name, String resource) {
    return new OSTSException(STS_VALIDATION_ERROR).withMessage("The query parameter " + name + " is repeated.");
  }
}
