package com.datastax.dsbulk.commons.internal.utils;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import java.nio.ByteBuffer;
import java.util.Map;

public class StatementUtils {
  public static long getDataSize(Statement stmt, ProtocolVersion version, CodecRegistry registry) {
    long dataSize = 0;
    if (stmt instanceof BoundStatement) {
      BoundStatement bs = (BoundStatement) stmt;
      int numVariables = bs.preparedStatement().getVariables().size();
      for (int i = 0; i < numVariables; i++) {
        ByteBuffer bb = bs.getBytesUnsafe(i);
        dataSize += bb == null ? 0 : bb.remaining();
      }
    } else if (stmt instanceof RegularStatement) {
      RegularStatement rs = (RegularStatement) stmt;
      if (rs.hasValues()) {
        if (rs.usesNamedValues()) {
          Map<String, ByteBuffer> values = rs.getNamedValues(version, registry);
          if (values != null) {
            for (ByteBuffer bb : values.values()) {
              dataSize += bb == null ? 0 : bb.remaining();
            }
          }
        } else {
          ByteBuffer[] values = rs.getValues(version, registry);
          if (values != null) {
            for (ByteBuffer bb : values) {
              dataSize += bb == null ? 0 : bb.remaining();
            }
          }
        }
      }
    } else if (stmt instanceof BatchStatement) {
      BatchStatement bs = (BatchStatement) stmt;

      for (Statement st : bs.getStatements()) {
        dataSize += getDataSize(st, version, registry);
      }

    } else if (stmt instanceof StatementWrapper) {
      StatementWrapper sw = (StatementWrapper) stmt;
      dataSize += getDataSize(DriverCoreHooks.wrappedStatement(sw), version, registry);
    }
    return dataSize;
  }
}
