/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToTimestampSinceEpoch;
import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.TIMESTAMP_VARNAME;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.function.BiFunction;

/** */
public class DefaultRecordMapper implements RecordMapper {

  private static final String FIELD = "field";
  private static final String CQL_TYPE = "cqlType";
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

  @VisibleForTesting
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
    Object raw = null;
    DataType cqlType = null;
    try {
      BoundStatement bs = boundStatementFactory.apply(record, insertStatement);
      for (String field : record.fields()) {
        currentField = field;
        variable = mapping.fieldToVariable(field);
        if (variable != null) {
          cqlType = insertStatement.getVariables().getType(variable);
          TypeToken<?> fieldType = recordMetadata.getFieldType(field, cqlType);
          if (fieldType != null) {
            raw = record.getFieldValue(field);
            bindColumn(bs, variable, raw, cqlType, fieldType);
          }
        }
      }
      record.clear();
      return bs;
    } catch (Exception e) {
      String finalCurrentField = currentField;
      String finalVariable = variable;
      Object finalRaw = raw;
      DataType finalCqlType = cqlType;
      return new UnmappableStatement(
          record,
          Suppliers.memoize(
              () ->
                  URIUtils.addParamsToURI(
                      record.getLocation(),
                      FIELD,
                      finalCurrentField,
                      finalVariable,
                      finalRaw == null ? null : finalRaw.toString(),
                      CQL_TYPE,
                      finalCqlType == null ? null : finalCqlType.toString())),
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
    String name = Metadata.quoteIfNecessary(variable);
    if (convertedValue == null) {
      bs.setToNull(name);
    } else if (variable.equals(TIMESTAMP_VARNAME)) {
      // The input value is intended to be the timestamp of the inserted data.
      // Parse it specially.
      Instant timestamp;
      if (convertedValue instanceof Date) {
        timestamp = ((Date) convertedValue).toInstant();
      } else if (convertedValue instanceof Instant) {
        timestamp = ((Instant) convertedValue);
      } else if (convertedValue instanceof ZonedDateTime) {
        timestamp = ((ZonedDateTime) convertedValue).toInstant();
      } else {
        ConvertingCodec<Object, Instant> codec =
            (ConvertingCodec<Object, Instant>) mapping.codec(variable, timestamp(), javaType);
        timestamp = codec.convertFrom(convertedValue);
      }
      bs.setLong(name, instantToTimestampSinceEpoch(timestamp, MICROSECONDS));
    } else {
      TypeCodec<Object> codec = mapping.codec(variable, cqlType, javaType);
      bs.set(name, convertedValue, codec);
    }
  }
}
