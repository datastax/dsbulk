/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.driver.core;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;
import java.util.List;

public class DriverCoreHooks {

  public static int valuesCount(
      BoundStatement statement, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    List<ByteBuffer> values = statement.getValues();
    if (values != null) {
      return values.size();
    }
    return 0;
  }

  public static int valuesCount(
      BatchStatement batchStatement, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    int count = 0;
    for (Statement statement : batchStatement) {
      if (statement instanceof BoundStatement) {
        assert statement instanceof BoundStatement;
        count += valuesCount((BoundStatement) statement, protocolVersion, codecRegistry);
      }
    }
    return count;
  }

  public static BatchType batchType(BatchStatement statement) {
    return statement.getBatchType();
  }

  public static CodecRegistry getCodecRegistry(ColumnDefinitions variables) {
    throw new UnsupportedOperationException("getCodecRegistry");
  }

  public static ColumnDefinitions resultSetVariables(PreparedStatement ps) {
    return ps.getResultSetDefinitions();
  }
}
