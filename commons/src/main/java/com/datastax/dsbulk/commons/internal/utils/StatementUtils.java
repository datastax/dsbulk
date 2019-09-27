/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

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
   * <p>Also note: this method does <em>NOT</em> follow the algorithm found in the driver's {@link
   * com.datastax.oss.driver.internal.core.util.Sizes Sizes} class; this method attempts to guess
   * the mutation size server-side, whereas the latter attempts to guess the size of the encoded
   * statement, protocol-wise. These can be very different, especially for batch statements.
   *
   * @param stmt The statement to inspect; cannot be {@code null}.
   * @param version The protocol version to use; cannot be {@code null}.
   * @param registry The codec registry to use; cannot be {@code null}.
   * @return The approximate size of inserted data contained in the statement.
   */
  public static long getDataSize(
      @NonNull Statement<?> stmt,
      @NonNull ProtocolVersion version,
      @NonNull CodecRegistry registry) {
    long dataSize = 0;
    if (stmt instanceof BoundStatement) {
      BoundStatement bs = (BoundStatement) stmt;
      dataSize = getDataSize(bs, bs.getPreparedStatement().getVariableDefinitions());
    } else if (stmt instanceof SimpleStatement) {
      SimpleStatement rs = (SimpleStatement) stmt;
      if (!rs.getNamedValues().isEmpty()) {
        Map<CqlIdentifier, Object> values = rs.getNamedValues();
        for (Object v : values.values()) {
          ByteBuffer bb = Conversions.encode(v, registry, version);
          dataSize += bb == null ? 0 : bb.remaining();
        }
      } else if (!rs.getPositionalValues().isEmpty()) {
        List<Object> values = rs.getPositionalValues();
        for (Object v : values) {
          ByteBuffer bb = Conversions.encode(v, registry, version);
          dataSize += bb == null ? 0 : bb.remaining();
        }
      }
    } else if (stmt instanceof BatchStatement) {
      BatchStatement bs = (BatchStatement) stmt;
      for (Statement<?> st : bs) {
        dataSize += getDataSize(st, version, registry);
      }
    }
    return dataSize;
  }

  /**
   * Evaluates the data size contained in the given {@linkplain Row row}. The data size is the total
   * number of bytes required to encode all the data contained in the row.
   *
   * @param row The row to inspect; cannot be {@code null}.
   * @return The total size in bytes of all the encoded data contained in the row.
   */
  public static long getDataSize(@NonNull Row row) {
    return getDataSize(row, row.getColumnDefinitions());
  }

  private static long getDataSize(GettableByIndex container, ColumnDefinitions metadata) {
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
