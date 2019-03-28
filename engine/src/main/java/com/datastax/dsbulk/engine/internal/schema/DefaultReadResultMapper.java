/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.VARIABLE;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultErrorRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.reflect.TypeToken;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultReadResultMapper implements ReadResultMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultReadResultMapper.class);
  private static final URI UNKNOWN_ROW_RESOURCE = URI.create("cql://unknown");

  private final Mapping mapping;
  private final RecordMetadata recordMetadata;

  public DefaultReadResultMapper(Mapping mapping, RecordMetadata recordMetadata) {
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
  }

  @NotNull
  @Override
  public Record map(@NotNull ReadResult result) {
    Row row = result.getRow().orElseThrow(IllegalStateException::new);
    Supplier<URI> resource =
        () ->
            getRowResource(row, result.getExecutionInfo().orElseThrow(IllegalStateException::new));
    try {
      DefaultRecord record = new DefaultRecord(result, resource, -1);
      for (Definition def : row.getColumnDefinitions()) {
        CQLIdentifier variable = CQLIdentifier.fromInternal(def.getName());
        String name = variable.render(VARIABLE);
        DataType cqlType = def.getType();
        Collection<Field> fields = mapping.variableToFields(variable);
        for (Field field : fields) {
          TypeToken<?> fieldType = recordMetadata.getFieldType(field, cqlType);
          try {
            TypeCodec<?> codec = mapping.codec(variable, cqlType, fieldType);
            Object value = row.get(name, codec);
            record.setFieldValue(field, value);
          } catch (RuntimeException e) {
            ByteBuffer bb = row.getBytesUnsafe(name);
            String bytes = Bytes.toHexString(bb);
            String msg =
                String.format(
                    "Could not deserialize column %s of type %s as %s (raw value was: %s)",
                    name, cqlType, fieldType, bytes);
            throw new IllegalArgumentException(msg, e);
          }
        }
      }
      return record;
    } catch (Exception e) {
      return new DefaultErrorRecord(result, resource, -1, e);
    }
  }

  /**
   * Returns the resource {@link URI} of a row in a read result.
   *
   * <p>It is unfortunately almost impossible to uniquely identify a row; this method returns the
   * same resource for two rows obtained from the same coordinator.
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
  private static URI getRowResource(Row row, ExecutionInfo executionInfo) {
    try {
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
    } catch (Exception e) {
      LOGGER.error("Cannot create URI for row", e);
      return UNKNOWN_ROW_RESOURCE;
    }
  }
}
