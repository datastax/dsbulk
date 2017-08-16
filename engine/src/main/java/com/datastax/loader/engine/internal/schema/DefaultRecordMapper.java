/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.connectors.api.RecordMetadata;
import com.datastax.loader.engine.internal.statement.BulkBoundStatement;
import com.datastax.loader.engine.internal.statement.UnmappableStatement;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.util.function.BiFunction;

/** */
public class DefaultRecordMapper implements RecordMapper {

  private final PreparedStatement insertStatement;

  private final Mapping mapping;

  private final RecordMetadata recordMetadata;

  /** Values in input that we treat as null in the loader. */
  private final ImmutableSet<String> nullWords;

  /** Whether to map null input to "unset" */
  private final boolean nullToUnset;

  private final BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory;

  public DefaultRecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      RecordMetadata recordMetadata,
      ImmutableSet<String> nullWords,
      boolean nullToUnset) {
    this(
        insertStatement,
        mapping,
        recordMetadata,
        nullWords,
        nullToUnset,
        (mappedRecord, statement) -> new BulkBoundStatement<>(mappedRecord, insertStatement));
  }

  DefaultRecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      RecordMetadata recordMetadata,
      ImmutableSet<String> nullWords,
      boolean nullToUnset,
      BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory) {
    this.insertStatement = insertStatement;
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.nullWords = nullWords;
    this.nullToUnset = nullToUnset;
    this.boundStatementFactory = boundStatementFactory;
  }

  @Override
  public Statement map(Record record) {
    try {
      BoundStatement bs = boundStatementFactory.apply(record, insertStatement);
      for (String field : record.fields()) {
        String variable = mapping.fieldToVariable(field);
        if (variable != null) {
          Object raw = record.getFieldValue(field);
          DataType cqlType = insertStatement.getVariables().getType(variable);
          TypeToken<?> fieldType = recordMetadata.getFieldType(field, cqlType);
          if (fieldType != null) {
            bindColumn(bs, variable, raw, cqlType, fieldType);
          }
        }
      }
      return bs;
    } catch (Exception e) {
      return new UnmappableStatement(record, e);
    }
  }

  private void bindColumn(
      BoundStatement bs, String variable, Object raw, DataType cqlType, TypeToken<?> javaType) {
    // If the raw value is one of the nullWords, the input represents null.
    Object convertedValue = raw;
    if (raw == null || (raw instanceof String && nullWords.contains(raw))) {
      convertedValue = null;
    }

    // Account for nullToUnset.
    if (convertedValue == null && nullToUnset) {
      return;
    }

    if (convertedValue == null) {
      bs.setToNull(variable);
    } else {
      TypeCodec<Object> codec = mapping.codec(variable, cqlType, javaType);
      bs.set(variable, convertedValue, codec);
    }
  }
}
