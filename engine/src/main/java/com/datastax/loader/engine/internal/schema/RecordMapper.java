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
import com.datastax.loader.executor.api.statement.BulkBoundStatement;
import com.datastax.loader.executor.api.statement.BulkSimpleStatement;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

/** */
public class RecordMapper {

  private final PreparedStatement insertStatement;

  private final Mapping mapping;

  public RecordMapper(PreparedStatement insertStatement, Mapping mapping) {
    this.insertStatement = insertStatement;
    this.mapping = mapping;
  }

  public Mapping getMapping() {
    return mapping;
  }

  public Statement map(Record record) throws MalformedURLException {
    Object field = null;
    Object variable = null;
    try {
      if (record instanceof MappedRecord) {
        MappedRecord mappedRecord = (MappedRecord) record;
        BoundStatement bs = new BulkBoundStatement<>(mappedRecord, insertStatement);
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

  private static void bindColumn(BoundStatement bs, Object raw, Object variable) {
    if (raw == null) {
      // TODO null -> unset
      if (variable instanceof String) bs.setToNull((String) variable);
      else bs.setToNull((int) variable);
    } else {
      @SuppressWarnings("unchecked")
      Class<Object> targetClass = (Class<Object>) raw.getClass();
      if (variable instanceof String) bs.set(((String) variable), raw, targetClass);
      else bs.set(((int) variable), raw, targetClass);
    }
  }
}
