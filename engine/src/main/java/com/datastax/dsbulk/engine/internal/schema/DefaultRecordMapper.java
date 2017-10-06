/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.util.function.BiFunction;

/** */
public class DefaultRecordMapper implements RecordMapper {

  private final PreparedStatement insertStatement;

  private final Mapping mapping;

  private final RecordMetadata recordMetadata;

  /** Values in input that we treat as null in the loader. */
  private final ImmutableSet<String> nullStrings;

  /** Whether to map null input to "unset" */
  private final boolean nullToUnset;

  private final BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory;

  public DefaultRecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      RecordMetadata recordMetadata,
      ImmutableSet<String> nullStrings,
      boolean nullToUnset) {
    this(
        insertStatement,
        mapping,
        recordMetadata,
        nullStrings,
        nullToUnset,
        (mappedRecord, statement) -> new BulkBoundStatement<>(mappedRecord, insertStatement));
  }

  DefaultRecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      RecordMetadata recordMetadata,
      ImmutableSet<String> nullStrings,
      boolean nullToUnset,
      BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory) {
    this.insertStatement = insertStatement;
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.nullStrings = nullStrings;
    this.nullToUnset = nullToUnset;
    this.boundStatementFactory = boundStatementFactory;
  }

  @Override
  public Statement map(Record record) {
    String currentField = null;
    String variable = null;
    try {
      BoundStatement bs = boundStatementFactory.apply(record, insertStatement);
      for (String field : record.fields()) {
        currentField = field;
        variable = mapping.fieldToVariable(field);
        if (variable != null) {
          DataType cqlType = insertStatement.getVariables().getType(variable);
          TypeToken<?> fieldType = recordMetadata.getFieldType(field, cqlType);
          if (fieldType != null) {
            Object raw = record.getFieldValue(field);
            bindColumn(bs, variable, raw, cqlType, fieldType);
          }
        }
      }
      return bs;
    } catch (Exception e) {
      return new UnmappableStatement(
          record,
          URIUtils.addParamsToURI(
              record.getLocation(), "fieldName", currentField, "columnName", variable),
          e);
    }
  }

  private void bindColumn(
      BoundStatement bs, String variable, Object raw, DataType cqlType, TypeToken<?> javaType) {
    // If the raw value is one of the nullStrings, the input represents null.
    Object convertedValue = raw;
    if (raw == null || (raw instanceof String && nullStrings.contains(raw))) {
      convertedValue = null;
    }

    // Account for nullToUnset.
    if (convertedValue == null && nullToUnset) {
      return;
    }

    // the mapping provides unquoted variable names,
    // so we need to quote them now
    if (convertedValue == null) {
      bs.setToNull(Metadata.quoteIfNecessary(variable));
    } else {
      TypeCodec<Object> codec = mapping.codec(variable, cqlType, javaType);
      bs.set(Metadata.quoteIfNecessary(variable), convertedValue, codec);
    }
  }
}
