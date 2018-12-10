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
import com.datastax.driver.core.DriverCoreHooks;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import java.nio.ByteBuffer;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class StatementUtils {

  /**
   * Evaluates the data size contained in the given statement.
   *
   * <p>This only applies to mutations (INSERT, UPDATE, DELETE statements), and is specially useful
   * for BATCH statements, where this method can be used to guess if a given batch risks to exceed
   * the thresholds defined server-side in the in cassandra.yaml configuration file for the options
   * {@code batch_size_warn_threshold_in_kb} and {@code batch_size_fail_threshold_in_kb}.
   *
   * <p>The actual algorithm used by Apache Cassandra can be found in {@code
   * org.apache.cassandra.db.IMutation.dataSize()} but cannot be easily reproduced client-side.
   * Instead, this method tries to guess the actual amount, in bytes, that the mutations triggered
   * by the statement will generate, but unfortunately the heuristic used here is not 100% accurate
   * and sometimes underestimates or overestimates the actual data size.
   *
   * @see <a
   *     href=https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/config/configCassandra_yaml.html#configCassandra_yaml__advProps">cassandra.yaml
   *     configuration file</a>
   * @param stmt The statement to inspect; cannot be {@code null}.
   * @param version The protocol version to use; cannot be {@code null}.
   * @param registry The codec registry to use; cannot be {@code null}.
   * @return The approximate size of inserted data contained in the statement.
   */
  public static long getDataSize(
      @NotNull Statement stmt, @NotNull ProtocolVersion version, @NotNull CodecRegistry registry) {
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
