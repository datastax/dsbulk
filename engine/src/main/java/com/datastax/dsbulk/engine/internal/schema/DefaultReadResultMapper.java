/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
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
      DefaultRecord record = new DefaultRecord(result, resource, -1, location);
      for (ColumnDefinitions.Definition col : row.getColumnDefinitions()) {
        // do not quote variable names here as the mapping expects them unquoted
        String variable = col.getName();
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
      return new DefaultErrorRecord(result, resource, -1, location, e);
    }
  }
}
