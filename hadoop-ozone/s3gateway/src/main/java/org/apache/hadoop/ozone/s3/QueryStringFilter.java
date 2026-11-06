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

import java.net.URI;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;

/**
 * Makes the query parameters that the endpoints read agree with the query that SigV4 signs.
 *
 * <p>The canonical query takes a raw '+' as a literal plus, but JAX-RS decodes it as a space, so a request signed
 * with {@code prefix=a+b} would list the keys under {@code a b}, and a raw '+' could replace a {@code %2B} without
 * changing the signature. The filter encodes a raw '+' in the query as {@code %2B} before anything reads the query
 * parameters, so that both spellings run the request they sign.
 */
@Provider
@PreMatching
@Priority(QueryStringFilter.PRIORITY)
public class QueryStringFilter implements ContainerRequestFilter {

  /** Before {@link AuthorizationFilter}, which reads the parameters of a presigned request. */
  public static final int PRIORITY = AuthorizationFilter.PRIORITY - S3GatewayHttpServer.FILTER_PRIORITY_DO_AFTER;

  @Override
  public void filter(ContainerRequestContext context) {
    URI requestUri = context.getUriInfo().getRequestUri();
    String rawQuery = requestUri.getRawQuery();
    if (rawQuery != null && rawQuery.indexOf('+') >= 0) {
      // Replaces the query in the URI as sent: rebuilding the URI from its parts would re-encode the rest of it.
      String uri = requestUri.toString();
      int queryStart = uri.indexOf('?') + 1;
      context.setRequestUri(context.getUriInfo().getBaseUri(), URI.create(uri.substring(0, queryStart)
          + rawQuery.replace("+", "%2B") + uri.substring(queryStart + rawQuery.length())));
    }
  }
}
