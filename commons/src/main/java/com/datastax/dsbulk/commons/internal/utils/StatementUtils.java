/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import java.nio.ByteBuffer;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class StatementUtils {

  /**
   * Evaluates the data size contained in the given statement. The data size is the total number of
   * bytes required to encode all the bound variables contained in the statement.
   *
   * <p>This method can be used to guess if a given batch statement risks to exceed the thresholds
   * defined server-side in the in the <a
   * href="https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/config/configCassandra_yaml.html#configCassandra_yaml__advProps">cassandra.yaml
   * configuration file</a> for the options {@code batch_size_warn_threshold_in_kb} and {@code
   * batch_size_fail_threshold_in_kb}. But please note that the actual algorithm used by Apache
   * Cassandra, which can be found in {@code org.apache.cassandra.db.IMutation.dataSize()}, cannot
   * be easily reproduced client-side. Instead, this method follows a more straight-forward
   * algorithm that may sometimes underestimate or overestimate the size computed server-side.
   *
   * @param stmt The statement to inspect; cannot be {@code null}.
   * @param version The protocol version to use; cannot be {@code null}.
   * @param registry The codec registry to use; cannot be {@code null}.
   * @return The approximate size of inserted data contained in the statement.
   */
  public static long getDataSize(
      @NotNull Statement stmt, @NotNull ProtocolVersion version, @NotNull CodecRegistry registry) {
    long dataSize = 0;
    if (stmt instanceof BoundStatement) {
      return getDataSize((BoundStatement) stmt);
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
      Statement statement = DriverCoreHooks.wrappedStatement(sw);
      dataSize += getDataSize(statement, version, registry);
    }
    return dataSize;
  }

  /**
   * Evaluates the data size contained in the given {@linkplain GettableData data container}. The
   * data size is the total number of bytes required to encode all the data contained in the
   * container.
   *
   * @param container The data container to inspect; cannot be {@code null}.
   * @return The total size in bytes of all the encoded data contained in the container.
   */
  public static long getDataSize(@NotNull GettableData container) {
    long dataSize = 0;
    if (container instanceof BoundStatement) {
      dataSize +=
          getDataSize(container, ((BoundStatement) container).preparedStatement().getVariables());
    } else if (container instanceof Row) {
      dataSize += getDataSize(container, ((Row) container).getColumnDefinitions());
    }
    return dataSize;
  }

  private static long getDataSize(GettableData container, ColumnDefinitions metadata) {
    long dataSize = 0L;
    if (metadata.size() > 0) {
      for (int i = 0; i < metadata.size(); i++) {
        ByteBuffer bb = container.getBytesUnsafe(i);
        if (bb != null) {
          dataSize += bb.remaining();
        }
      }
    }
    return dataSize;
  }
}
