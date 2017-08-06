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
import org.jetbrains.annotations.TestOnly;

/** */
public class RecordMapper {

  private final PreparedStatement insertStatement;

  private final Mapping mapping;

  /** Value in input that we treat as null in the loader. */
  private final String nullWord;

  /** Whether to map null input to "unset" */
  private final boolean nullToUnset;

  /** Static that represents a converted value that we do not want to insert. */
  private static final Object UNSET = new Object();

  private final BoundStatementFactory boundStatementFactory;

  public RecordMapper(
      PreparedStatement insertStatement, Mapping mapping, String nullWord, boolean nullToUnset) {
    this(
        insertStatement,
        mapping,
        nullWord,
        nullToUnset,
        (mappedRecord, statement) -> new BulkBoundStatement<>(mappedRecord, insertStatement));
  }

  RecordMapper(
      PreparedStatement insertStatement,
      Mapping mapping,
      String nullWord,
      boolean nullToUnset,
      BoundStatementFactory boundStatementFactory) {
    this.insertStatement = insertStatement;
    this.mapping = mapping;
    this.nullWord = nullWord;
    this.nullToUnset = nullToUnset;
    this.boundStatementFactory = boundStatementFactory;
  }

  public Statement map(Record record) throws MalformedURLException {
    Object field = null;
    String variable = null;
    try {
      if (record instanceof MappedRecord) {
        MappedRecord mappedRecord = (MappedRecord) record;
        BoundStatement bs =
            boundStatementFactory.createBoundStatement(mappedRecord, insertStatement);
        for (Object f : mappedRecord.fields()) {
          field = f;
          Object raw = mappedRecord.getFieldValue(field);
          variable = mapping.map(field);
          if (variable != null) {
            bindColumn(bs, maybe_convert_null(raw), variable);
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

  @TestOnly
  public Mapping getMapping() {
    return mapping;
  }

  @TestOnly
  public String getNullWord() {
    return nullWord;
  }

  @TestOnly
  public boolean getNullToUnset() {
    return nullToUnset;
  }

  @TestOnly
  interface BoundStatementFactory {
    BoundStatement createBoundStatement(MappedRecord mappedRecord, PreparedStatement statement);
  }

  private Object maybe_convert_null(Object raw) {
    // If the raw value is the null-word, the input represents null. Set result accordingly.
    Object result = null;
    if (raw != null && !(nullWord != null && nullWord.equals(raw))) {
      result = raw;
    }

    // Account for nullToUnset.
    if (result == null && nullToUnset) {
      result = UNSET;
    }
    return result;
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

  private static void bindColumn(BoundStatement bs, Object convertedValue, String variable) {
    if (convertedValue == UNSET) {
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
