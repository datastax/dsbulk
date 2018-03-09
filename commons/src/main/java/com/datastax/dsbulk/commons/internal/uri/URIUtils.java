/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;

public class URIUtils {

  private static final String POSITION = "pos";

  public static URI createResourceURI(URL resource) {
    return URI.create(resource.toExternalForm());
  }

  public static URI createLocationURI(URL resource, long position) {
    return URI.create(
        resource.toExternalForm()
            + (resource.getQuery() == null ? '?' : '&')
            + POSITION
            + "="
            + position);
  }

  public static URI addParamsToURI(URI uri, String key, String value, String... rest) {
    if (rest.length % 2 == 1) {
      throw new IllegalArgumentException("params list must have an even number of elements");
    }
    StringBuilder sb = new StringBuilder(uri.toString());
    sb.append(uri.getQuery() == null ? '?' : '&');
    try {
      if (key != null && value != null) {
        sb.append(URLEncoder.encode(key, "UTF-8"))
            .append("=")
            .append(URLEncoder.encode(value, "UTF-8"));
      }
      for (int i = 0; i < rest.length; i += 2) {
        String key2 = rest[i];
        String value2 = rest[i + 1];
        if (key2 != null && value2 != null) {
          sb.append('&')
              .append(URLEncoder.encode(key2, "UTF-8"))
              .append('=')
              .append(URLEncoder.encode(value2, "UTF-8"));
        }
      }
    } catch (UnsupportedEncodingException e) {
      // swallow, this should never happen for UTF-8
    }
    return URI.create(sb.toString());
  }

  /**
   * Returns the resource {@link URI} of a row in a read result.
   *
   * <p>URIs returned by this method are of the following form:
   *
   * <pre>{@code
   * cql://host:port/keyspace/table
   * }</pre>
   *
   * @param row The row of the result
   * @param executionInfo The execution info of the result
   * @return The read result row resource URI.
   */
  public static URI getRowResource(Row row, ExecutionInfo executionInfo) {
    InetSocketAddress host = executionInfo.getQueriedHost().getSocketAddress();
    ColumnDefinitions resultVariables = row.getColumnDefinitions();
    // this might break if the statement has no result variables (unlikely)
    // or if the first variable is not associated to a keyspace and table (also unlikely)
    String keyspace = resultVariables.getKeyspace(0);
    String table = resultVariables.getTable(0);
    String sb =
        "cql://"
            + host.getAddress().getHostAddress()
            + ':'
            + host.getPort()
            + '/'
            + keyspace
            + '/'
            + table;
    return URI.create(sb);
  }

  /**
   * Returns the location {@link URI} of a row in a read result.
   *
   * <p>URIs returned by this method are of the following form:
   *
   * <pre>{@code
   * cql://host:port/keyspace/table?var1=value1&var2=value2
   * }</pre>
   *
   * <p>Both bound variables – if any – and result set variables are collected and returned in the
   * location URI.
   *
   * <p>Variable values are {@link TypeCodec#format(Object) formatted} as CQL literals.
   *
   * @param row The row of the result
   * @param executionInfo The execution info of the result
   * @param statement The statement that was executed to produce the result
   * @return The read result row location URI.
   */
  public static URI getRowLocation(Row row, ExecutionInfo executionInfo, Statement statement) {
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
    // A typical read statement is
    // "SELECT a, b, c FROM t WHERE token (a) > :start and token(a) <= :end".
    // Here, bound variables 'start' and 'end' are completely different from selected columns 'a',
    // 'b', 'c'.
    // Granted, there can be duplication if the user adds a custom WHERE clause containing
    // 'a', 'b', or 'c'. But duplicated query parameters are perfectly valid and don't invalidate
    // the resulting URI.
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
