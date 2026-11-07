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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapOS3Exception;

import java.net.URI;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/**
 * Makes the query parameters that the endpoints read agree with the query that SigV4 signs.
 *
 * <p>The canonical query takes a raw '+' as a literal plus, but JAX-RS decodes it as a space, so a request signed
 * with {@code prefix=a+b} would list the keys under {@code a b}, and a raw '+' could replace a {@code %2B} without
 * changing the signature. The filter encodes a raw '+' in the query as {@code %2B} before anything reads the query
 * parameters, so that both spellings run the request they sign.
 *
 * <p>The canonical query also signs every value of a repeated parameter, while an endpoint reads only the first one,
 * so reordering the values of a signed {@code partNumber=1&partNumber=2} would change the part without changing the
 * signature. S3 defines no query parameter that takes several values, so the filter refuses a repeated one.
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
    if (rawQuery == null) {
      return;
    }
    if (rawQuery.indexOf('+') >= 0) {
      // Replaces the query in the URI as sent: rebuilding the URI from its parts would re-encode the rest of it.
      String uri = requestUri.toString();
      int queryStart = uri.indexOf('?') + 1;
      String query = rawQuery.replace("+", "%2B");
      context.setRequestUri(context.getUriInfo().getBaseUri(),
          URI.create(uri.substring(0, queryStart) + query + uri.substring(queryStart + rawQuery.length())));
      rawQuery = query;
    }
    Set<String> names = new HashSet<>();
    for (String parameter : StringUtils.split(rawQuery, '&')) {
      int separator = parameter.indexOf('=');
      String name = decode(separator < 0 ? parameter : parameter.substring(0, separator));
      if (!names.add(name)) {
        throw repeatedParameter(name, requestUri.getRawPath());
      }
    }
  }

  /** The error that refuses a repeated parameter, in the S3 error format. */
  protected RuntimeException repeatedParameter(String name, String resource) {
    return wrapOS3Exception(S3ErrorTable.newError(INVALID_ARGUMENT, resource)
        .withMessage("The query parameter " + name + " is repeated."));
  }

  /** Decodes a parameter name as the endpoints read it; no raw '+' is left to become a space. */
  private static String decode(String rawName) {
    try {
      return URLDecoder.decode(rawName, UTF_8);
    } catch (IllegalArgumentException e) {
      // A malformed escape: compare the name as sent.
      return rawName;
    }
  }
}
