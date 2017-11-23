/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.WorkflowUtils.parseTimestamp;
import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.settings.SchemaSettings.TTL_VARNAME;

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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.time.format.DateTimeFormatter;
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
  private final int ttl;
  private final long timestamp;
  private final boolean hasTtlInMapping;
  private final boolean hasTimestampInMapping;
  private final DateTimeFormatter timestampFormat;

  public DefaultRecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      RecordMetadata recordMetadata,
      ImmutableSet<String> nullStrings,
      boolean nullToUnset,
      int ttl,
      long timestamp,
      DateTimeFormatter timestampFormat) {
    this(
        insertStatement,
        mapping,
        recordMetadata,
        nullStrings,
        nullToUnset,
        ttl,
        timestamp,
        timestampFormat,
        (mappedRecord, statement) -> new BulkBoundStatement<>(mappedRecord, insertStatement));
  }

  DefaultRecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      RecordMetadata recordMetadata,
      ImmutableSet<String> nullStrings,
      boolean nullToUnset,
      int ttl,
      long timestamp,
      DateTimeFormatter timestampFormat,
      BiFunction<Record, PreparedStatement, BoundStatement> boundStatementFactory) {
    this.insertStatement = insertStatement;
    this.mapping = mapping;
    this.recordMetadata = recordMetadata;
    this.nullStrings = nullStrings;
    this.nullToUnset = nullToUnset;
    this.ttl = ttl;
    this.timestamp = timestamp;
    this.timestampFormat = timestampFormat;
    this.boundStatementFactory = boundStatementFactory;
    hasTimestampInMapping = mapping.variableToField(TIMESTAMP_VARNAME) != null;
    hasTtlInMapping = mapping.variableToField(TTL_VARNAME) != null;
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

      if (ttl != -1 && !hasTtlInMapping) {
        bindColumn(bs, TTL_VARNAME, ttl, DataType.cint(), TypeToken.of(Integer.class));
      }

      if (timestamp != -1 && !hasTimestampInMapping) {
        bindColumn(bs, TIMESTAMP_VARNAME, timestamp, DataType.bigint(), TypeToken.of(Long.class));
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
    if (convertedValue == null) {
      bs.setToNull(Metadata.quoteIfNecessary(variable));
    } else {
      if (variable.equals(TIMESTAMP_VARNAME)) {
        // The input value is intended to be the timestamp of the inserted data.
        // Parse it specially.
        convertedValue = parseTimestamp(convertedValue.toString(), "field value", timestampFormat);
        javaType = TypeToken.of(Long.class);
      }
      TypeCodec<Object> codec = mapping.codec(variable, cqlType, javaType);
      bs.set(Metadata.quoteIfNecessary(variable), convertedValue, codec);
    }
  }
}
