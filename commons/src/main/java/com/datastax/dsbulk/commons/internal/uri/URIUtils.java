/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.uri;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.StringTokenizer;

public class URIUtils {

  public static final String LINE = "line";
  public static final String COLUMN = "column";

  public static URI createLocationURI(URL resource, long line, int column) {
    return URI.create(
        resource.toExternalForm()
            + (resource.getQuery() == null ? '?' : '&')
            + LINE
            + "="
            + line
            + "&"
            + COLUMN
            + "="
            + column);
  }

  public static URI addParamsToURI(URI uri, String key, String value, String... rest) {
    if (rest.length % 2 == 1) {
      throw new IllegalArgumentException("params list must have an even number of elements");
    }

    StringBuilder sb = new StringBuilder(uri.toString());
    sb.append(uri.getQuery() == null ? '?' : '&');
    sb.append(key).append("=").append(value);
    for (int i = 0; i < rest.length; i += 2) {
      try {
        sb.append("&")
            .append(URLEncoder.encode(rest[i], "UTF-8"))
            .append('=')
            .append(URLEncoder.encode(rest[i + 1], "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        // swallow, this should never happen for UTF-8
      }
    }
    return URI.create(sb.toString());
  }

  public static long extractLine(URI location) {
    ListMultimap<String, String> parameters = parseURIParameters(location);
    List<String> values = parameters.get(LINE);
    if (values.isEmpty()) {
      return -1;
    }
    return Long.parseLong(values.get(0));
  }

  public static URI getBaseURI(URI uri) throws URISyntaxException {
    return new URI(
        uri.getScheme(),
        uri.getAuthority(),
        uri.getPath(),
        null, // Ignore the query part of the input url
        uri.getFragment());
  }

  public static ListMultimap<String, String> parseURIParameters(URI uri) {
    if (uri == null || uri.getQuery() == null) {
      return null;
    }
    ArrayListMultimap<String, String> map = ArrayListMultimap.create();
    StringTokenizer tokenizer = new StringTokenizer(uri.getQuery(), "&");
    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken();
      int idx = token.indexOf("=");
      map.put(token.substring(0, idx), token.substring(idx + 1));
    }
    return map;
  }
}
