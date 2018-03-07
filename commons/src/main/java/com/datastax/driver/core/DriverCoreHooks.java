/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.Map;

public class DriverCoreHooks {

  public static int valuesCount(
      RegularStatement statement, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    ByteBuffer[] values = statement.getValues(protocolVersion, codecRegistry);
    if (values != null) {
      return values.length;
    }
    Map<String, ByteBuffer> namedValues = statement.getNamedValues(protocolVersion, codecRegistry);
    if (namedValues != null) {
      return namedValues.size();
    } else {
      return 0;
    }
  }

  public static int valuesCount(
      BatchStatement batchStatement, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    int count = 0;
    for (Statement statement : batchStatement.getStatements()) {
      if (statement instanceof StatementWrapper) {
        statement = ((StatementWrapper) statement).getWrappedStatement();
      }
      if (statement instanceof RegularStatement) {
        count += valuesCount((RegularStatement) statement, protocolVersion, codecRegistry);
      } else {
        assert statement instanceof BoundStatement;
        BoundStatement st = (BoundStatement) statement;
        count += st.wrapper.values.length;
      }
    }
    return count;
  }

  public static BatchStatement.Type batchType(BatchStatement statement) {
    return statement.batchType;
  }

  public static Statement wrappedStatement(StatementWrapper statement) {
    return statement.getWrappedStatement();
  }

  public static CodecRegistry getCodecRegistry(ColumnDefinitions variables) {
    return variables.codecRegistry;
  }

  public static String handleId(String id) {
    return Metadata.handleId(id);
  }

  public static ColumnDefinitions resultSetVariables(PreparedStatement ps) {
    return ps.getPreparedId().resultSetMetadata.variables;
  }
}
