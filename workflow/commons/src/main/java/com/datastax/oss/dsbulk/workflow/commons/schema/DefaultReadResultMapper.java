/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.connectors.api.DefaultErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.Mapping;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Set;
import java.util.function.Supplier;
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

  @NonNull
  @Override
  public Record map(@NonNull ReadResult result) {
    Row row = result.getRow().orElseThrow(IllegalStateException::new);
    Supplier<URI> resource =
        () ->
            getRowResource(row, result.getExecutionInfo().orElseThrow(IllegalStateException::new));
    try {
      DefaultRecord record = new DefaultRecord(result, resource, -1);
      for (ColumnDefinition def : row.getColumnDefinitions()) {
        CQLWord variable = CQLWord.fromInternal(def.getName().asInternal());
        CqlIdentifier name = variable.asIdentifier();
        DataType cqlType = def.getType();
        Set<Field> fields = mapping.variableToFields(variable);
        for (Field field : fields) {
          GenericType<?> fieldType = null;
          try {
            fieldType = recordMetadata.getFieldType(field, cqlType);
            TypeCodec<?> codec = mapping.codec(variable, cqlType, fieldType);
            Object value = row.get(name, codec);
            record.setFieldValue(field, value);
          } catch (Exception e) {
            String msg =
                String.format(
                    "Could not deserialize column %s of type %s as %s",
                    name.asCql(true), cqlType, fieldType);
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
      Node coordinator = executionInfo.getCoordinator();
      // always present for executed queries
      assert coordinator != null;
      InetSocketAddress host = (InetSocketAddress) coordinator.getEndPoint().resolve();
      ColumnDefinitions resultVariables = row.getColumnDefinitions();
      // this might break if the statement has no result variables (unlikely)
      // or if the first variable is not associated to a keyspace and table (also unlikely)
      String keyspace = resultVariables.get(0).getKeyspace().asInternal();
      String table = resultVariables.get(0).getTable().asInternal();
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
