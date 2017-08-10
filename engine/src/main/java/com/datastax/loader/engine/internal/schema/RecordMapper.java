/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.loader.connectors.api.CQLRecord;
import com.datastax.loader.connectors.api.MappedRecord;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.engine.internal.statement.BulkBoundStatement;
import com.datastax.loader.engine.internal.statement.BulkSimpleStatement;
import com.datastax.loader.engine.internal.statement.UnmappableStatement;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.function.BiFunction;

/** */
public class RecordMapper {

  private final PreparedStatement insertStatement;

  private final Mapping mapping;

  /** Values in input that we treat as null in the loader. */
  private final List<String> nullWords;

  /** Whether to map null input to "unset" */
  private final boolean nullToUnset;

  private final BiFunction<MappedRecord, PreparedStatement, BoundStatement> boundStatementFactory;

  public RecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      List<String> nullWords,
      boolean nullToUnset) {
    this(
        insertStatement,
        mapping,
        nullWords,
        nullToUnset,
        (mappedRecord, statement) -> new BulkBoundStatement<>(mappedRecord, insertStatement));
  }

  RecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      List<String> nullWords,
      boolean nullToUnset,
      BiFunction<MappedRecord, PreparedStatement, BoundStatement> boundStatementFactory) {
    this.insertStatement = insertStatement;
    this.mapping = mapping;
    this.nullWords = nullWords;
    this.nullToUnset = nullToUnset;
    this.boundStatementFactory = boundStatementFactory;
  }

  public Statement map(Record record) {
    Object field = null;
    String variable = null;
    try {
      if (record instanceof MappedRecord) {
        MappedRecord mappedRecord = (MappedRecord) record;
        BoundStatement bs = boundStatementFactory.apply(mappedRecord, insertStatement);
        for (Object f : mappedRecord.fields()) {
          field = f;
          Object raw = mappedRecord.getFieldValue(field);
          variable = mapping.map(field);
          if (variable != null) {
            bindColumn(bs, raw, variable);
          }
        }
        return bs;
      } else if (record instanceof CQLRecord) {
        CQLRecord CQLRecord = (CQLRecord) record;
        return new BulkSimpleStatement<>(CQLRecord, CQLRecord.getQueryString());
      }
    } catch (Exception e) {
      return new UnmappableStatement(getLocation(record, field, variable), record, e);
    }
    throw new IllegalStateException("Unknown Record implementation: " + record.getClass());
  }

  private static URL getLocation(Record record, Object field, Object variable) {
    URL location = record.getLocation();
    try {
      if (field != null) {
        location =
            new URL(
                location.toExternalForm()
                    + (location.getQuery() == null ? '?' : '&')
                    + "field="
                    + URLEncoder.encode(field.toString(), "UTF-8"));
      }
      if (variable != null) {
        location =
            new URL(
                location.toExternalForm()
                    + (location.getQuery() == null ? '?' : '&')
                    + "param="
                    + URLEncoder.encode(variable.toString(), "UTF-8"));
      }
    } catch (MalformedURLException | UnsupportedEncodingException ignored) {
    }
    return location;
  }

  private void bindColumn(BoundStatement bs, Object raw, String variable) {
    // If the raw value is one of the nullWords, the input represents null.
    Object convertedValue = raw;
    if (raw == null || nullWords.contains(raw.toString())) {
      convertedValue = null;
    }

    // Account for nullToUnset.
    if (convertedValue == null && nullToUnset) {
      return;
    }

    if (convertedValue == null) {
      bs.setToNull(variable);
    } else {
      //noinspection unchecked
      bs.set(variable, convertedValue, (Class<Object>) convertedValue.getClass());
    }
  }
}
