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
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.base.Suppliers;

/** */
@SuppressWarnings("unused")
public class CQLReadResultMapper implements ReadResultMapper {

  @Override
  public Record map(ReadResult result) {
    return new DefaultRecord(
        result,
        Suppliers.memoize(
            () ->
                URIUtils.getRowResource(
                    result.getRow().orElseThrow(IllegalStateException::new),
                    result.getExecutionInfo().orElseThrow(IllegalStateException::new))),
        -1,
        Suppliers.memoize(
            () ->
                URIUtils.getRowLocation(
                    result.getRow().orElseThrow(IllegalStateException::new),
                    result.getExecutionInfo().orElseThrow(IllegalStateException::new),
                    result.getStatement())),
        inferWriteQuery(result));
  }

  private static String inferWriteQuery(ReadResult result) {
    Row row = result.getRow().orElseThrow(IllegalStateException::new);
    Statement statement = result.getStatement();
    ColumnDefinitions boundVariables = null;
    if (statement instanceof BoundStatement) {
      boundVariables = ((BoundStatement) statement).preparedStatement().getVariables();
    }
    ColumnDefinitions resultVariables = row.getColumnDefinitions();
    CodecRegistry codecRegistry = DriverCoreHooks.getCodecRegistry(resultVariables);
    String keyspace = Metadata.quoteIfNecessary(resultVariables.getKeyspace(0));
    String table = Metadata.quoteIfNecessary(resultVariables.getTable(0));
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(keyspace).append('.').append(table).append('(');
    int n = 0;
    if (boundVariables != null) {
      n = appendNames(boundVariables, sb, n);
    }
    appendNames(resultVariables, sb, n);
    sb.append(") VALUES (");
    n = 0;
    if (boundVariables != null) {
      n = appendValues(row, boundVariables, codecRegistry, sb, n);
    }
    appendValues(row, resultVariables, codecRegistry, sb, n);
    sb.append(')');
    return sb.toString();
  }

  private static int appendNames(ColumnDefinitions variables, StringBuilder sb, int n) {
    for (ColumnDefinitions.Definition variable : variables) {
      if (n++ > 0) {
        sb.append(',');
      }
      String col = variable.getName();
      sb.append(Metadata.quoteIfNecessary(col));
    }
    return n;
  }

  private static int appendValues(
      Row row, ColumnDefinitions variables, CodecRegistry codecRegistry, StringBuilder sb, int n) {
    for (ColumnDefinitions.Definition variable : variables) {
      if (n++ > 0) {
        sb.append(',');
      }
      DataType type = variable.getType();
      TypeCodec<Object> codec = codecRegistry.codecFor(type);
      sb.append(codec.format(row.getObject(variable.getName())));
    }
    return n;
  }
}
