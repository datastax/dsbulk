/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultErrorRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.base.Suppliers;
import com.google.common.reflect.TypeToken;
import java.net.URI;
import java.util.function.Supplier;

public class DefaultReadResultMapper implements ReadResultMapper {

  private final Mapping mapping;
  private final RecordMetadata recordMetadata;

  public DefaultReadResultMapper(Mapping mapping, RecordMetadata recordMetadata) {
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
  }

  @Override
  public Record map(ReadResult result) {
    Row row = result.getRow().orElseThrow(IllegalStateException::new);
    Supplier<URI> resource =
        Suppliers.memoize(
            () ->
                URIUtils.getRowResource(
                    result.getRow().orElseThrow(IllegalStateException::new),
                    result.getExecutionInfo().orElseThrow(IllegalStateException::new)));
    Supplier<URI> location =
        Suppliers.memoize(
            () ->
                URIUtils.getRowLocation(
                    result.getRow().orElseThrow(IllegalStateException::new),
                    result.getExecutionInfo().orElseThrow(IllegalStateException::new),
                    result.getStatement()));
    try {
      DefaultRecord record = DefaultRecord.indexed(result, resource, -1, location);
      for (Field field : mapping.fields()) {
        // do not quote variable names here as the mapping expects them unquoted
        CQLFragment variable = mapping.fieldToVariable(field);
        DataType type = row.getColumnDefinitions().getType(variable.asVariable());
        TypeToken<?> fieldType = recordMetadata.getFieldType(field, type);
        if (fieldType != null) {
          TypeCodec<?> codec = mapping.codec(variable, type, fieldType);
          Object value = row.get(variable.asVariable(), codec);
          record.setFieldValue(field, value);
        }
      }
      return record;
    } catch (Exception e) {
      return new DefaultErrorRecord(result, resource, -1, location, e);
    }
  }
}
