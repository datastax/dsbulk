/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

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
import com.datastax.dsbulk.executor.api.result.ReadResult;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;

class ResultUtils {

  /**
   * Returns the location {@link URI} of a read result.
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
   * @param result The read result to compute a location for.
   * @return The read result location URI.
   */
  static URI getLocation(ReadResult result) {
    Statement statement = result.getStatement();
    Row row = result.getRow().orElseThrow(IllegalStateException::new);
    ExecutionInfo executionInfo = result.getExecutionInfo().orElseThrow(IllegalStateException::new);
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
