/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.connectors.api.RecordMetadata;
import com.datastax.loader.connectors.api.internal.ErrorRecord;
import com.datastax.loader.connectors.api.internal.MapRecord;
import com.datastax.loader.executor.api.result.ReadResult;
import com.google.common.base.Suppliers;
import com.google.common.reflect.TypeToken;

public class DefaultReadResultMapper implements ReadResultMapper {

  private static final TypeToken<String> STRING_TYPE_TOKEN = TypeToken.of(String.class);

  private final Mapping mapping;
  private final RecordMetadata recordMetadata;
  private final String nullWord;

  public DefaultReadResultMapper(Mapping mapping, RecordMetadata recordMetadata, String nullWord) {
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.nullWord = nullWord;
  }

  @Override
  public Record map(ReadResult result) {
    Row row = result.getRow().orElseThrow(IllegalStateException::new);
    try {
      MapRecord record =
          new MapRecord(result, Suppliers.memoize(() -> ResultUtils.getLocation(result)));
      for (ColumnDefinitions.Definition col : row.getColumnDefinitions()) {
        String variable = Metadata.quoteIfNecessary(col.getName());
        String field = mapping.variableToField(variable);
        if (field != null) {
          TypeToken<?> fieldType = recordMetadata.getFieldType(field, col.getType());
          if (fieldType != null) {
            TypeCodec<Object> codec = mapping.codec(variable, col.getType(), fieldType);
            Object value = row.get(col.getName(), codec);
            if (value == null && nullWord != null && fieldType.equals(STRING_TYPE_TOKEN)) {
              value = nullWord;
            }
            record.setFieldValue(field, value);
          }
        }
      }
      return record;
    } catch (Exception e) {
      return new ErrorRecord(result, Suppliers.memoize(() -> ResultUtils.getLocation(result)), e);
    }
  }
}
