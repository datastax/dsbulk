/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.uri;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.StringTokenizer;

public class URIUtils {

  private static final String LINE = "line";

  public static URI createLocationURI(URL resource, long line) {
    return URI.create(
        resource.toExternalForm() + (resource.getQuery() == null ? '?' : '&') + LINE + "=" + line);
  }

  public static URI addParamsToURI(URI uri, String key, String value, String... rest) {
    if (rest.length % 2 == 1) {
      throw new IllegalArgumentException("params list must have an even number of elements");
    }

    StringBuilder sb = new StringBuilder(uri.toString());
    sb.append(uri.getQuery() == null ? '?' : '&');
    try {
      sb.append(URLEncoder.encode(key, "UTF-8"))
          .append("=")
          .append(URLEncoder.encode(value, "UTF-8"));
      for (int i = 0; i < rest.length; i += 2) {
        sb.append('&')
            .append(URLEncoder.encode(rest[i], "UTF-8"))
            .append('=')
            .append(URLEncoder.encode(rest[i + 1], "UTF-8"));
      }
    } catch (UnsupportedEncodingException e) {
      // swallow, this should never happen for UTF-8
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

  @SuppressWarnings("WeakerAccess")
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

  /**
   * Returns the location {@link URI} of a row in a read result.
   *
   * <p>URIs returned by this method are of the following form:
   *
   * <pre>
   * cql://host:port/keyspace/table?var1=value1&var2=value2
   * </pre>
   *
   * <p>Both bound variables – if any – and result set variables are collected and returned in the
   * location URI.
   *
   * <p>Variable values are {@link TypeCodec#format(Object) formatted} as CQL literals.
   *
   * @param statement The statement that was executed to produce the result
   * @param row The row of the result
   * @param executionInfo The execution info of the result
   * @return The read result row location URI.
   */
  public static URI getRowLocation(Statement statement, Row row, ExecutionInfo executionInfo) {
    InetSocketAddress host = executionInfo.getQueriedHost().getSocketAddress();
    ColumnDefinitions resultVariables = row.getColumnDefinitions();
    CodecRegistry codecRegistry = DriverCoreHooks.getCodecRegistry(resultVariables);
    // this might break if the statement has no result variables (unlikely)
    // or if the first variable is not associated to a keyspace and table (also unlikely)
    String keyspace = resultVariables.getKeyspace(0);
    String table = resultVariables.getTable(0);
    StringBuilder sb =
        new StringBuilder("cql://")
            .append(host.getAddress().getHostAddress())
            .append(':')
            .append(host.getPort())
            .append('/')
            .append(keyspace)
            .append('/')
            .append(table);
    sb.append('?');
    int n = 0;
    // Bound variables and result variables can intersect, but in the general case they will not.
    // A typical read statement is "SELECT a, b, c FROM t WHERE token (a) > :start and token(a) <= :end".
    // Here, bound variables 'start' and 'end' are completely different from selected columns 'a', 'b', 'c'.
    // Granted, there can be duplication if the user adds a custom WHERE clause containing
    // 'a', 'b', or 'c'. But duplicated query parameters are perfectly valid and don't invalidate the resulting URI.
    if (statement instanceof BoundStatement) {
      BoundStatement bs = (BoundStatement) statement;
      n = appendVariables(sb, bs.preparedStatement().getVariables(), bs, codecRegistry, n);
    }
    appendVariables(sb, resultVariables, row, codecRegistry, n);
    return URI.create(sb.toString());
  }

  private static int appendVariables(
      StringBuilder sb,
      ColumnDefinitions variables,
      GettableData data,
      CodecRegistry codecRegistry,
      int n) {
    for (ColumnDefinitions.Definition variable : variables) {
      String name = variable.getName();
      DataType type = variable.getType();
      String value;
      try {
        TypeCodec<Object> codec = codecRegistry.codecFor(type);
        value = codec.format(data.getObject(name));
      } catch (Exception e) {
        // This is unlikely to happen. We can safely assume that all values
        // retrieved with getObject() can be formatted using
        // TypeCodec.format() – because getObject() uses built-in codecs only,
        // and these are very robust.
        value = "?";
      }
      if (n++ > 0) {
        sb.append('&');
      }
      try {
        sb.append(URLEncoder.encode(name, "UTF-8"))
            .append('=')
            .append(URLEncoder.encode(value, "UTF-8"));
      } catch (UnsupportedEncodingException ignored) {
      }
    }
    return n;
  }
}
