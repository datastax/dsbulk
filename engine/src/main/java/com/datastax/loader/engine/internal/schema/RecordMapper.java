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
import com.datastax.loader.connectors.api.IndexedRecord;
import com.datastax.loader.connectors.api.MappedRecord;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.executor.api.statement.BulkBoundStatement;
import com.datastax.loader.executor.api.statement.BulkSimpleStatement;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

/** */
public class RecordMapper {

  private final PreparedStatement insertStatement;

  private final Map<String, String> mapping;

  public RecordMapper(PreparedStatement insertStatement, Map<String, String> mapping) {
    this.insertStatement = insertStatement;
    this.mapping = mapping;
  }

  public Map<String, String> getMapping() {
    return mapping;
  }

  public Statement map(Record record) throws MalformedURLException {
    String fieldName = null;
    String paramName = null;
    try {
      if (record instanceof MappedRecord) {
        MappedRecord mappedRecord = (MappedRecord) record;
        BoundStatement bs = new BulkBoundStatement<>(mappedRecord, insertStatement);
        for (String name : mappedRecord.fieldNames()) {
          fieldName = name;
          Object raw = mappedRecord.getValue(fieldName);
          paramName = mapping.get(fieldName);
          if (paramName != null) {
            bindColumn(bs, raw, paramName);
          }
        }
        return bs;
      } else if (record instanceof IndexedRecord) {
        IndexedRecord indexedRecord = (IndexedRecord) record;
        BoundStatement bs = new BulkBoundStatement<>(indexedRecord, insertStatement);
        for (int i = 0; i < indexedRecord.size(); i++) {
          fieldName = Integer.toString(i);
          Object raw = indexedRecord.getValue(i);
          paramName = mapping.get(fieldName);
          if (paramName != null) {
            bindColumn(bs, raw, paramName);
          }
        }
        return bs;
      } else if (record instanceof CQLRecord) {
        CQLRecord cqlRecord = (CQLRecord) record;
        return new BulkSimpleStatement<>(cqlRecord, cqlRecord.getQueryString());
      }
    } catch (Exception e) {
      return new UnmappableStatement(getLocation(record, fieldName, paramName), record, e);
    }
    throw new IllegalStateException("Unknown Record implementation: " + record.getClass());
  }

  private static URL getLocation(Record record, String fieldName, String paramName) {
    URL location = record.getLocation();
    try {
      if (fieldName != null) {
        location =
            new URL(
                location.toExternalForm()
                    + (location.getQuery() == null ? '?' : '&')
                    + "field="
                    + URLEncoder.encode(fieldName, "UTF-8"));
      }
      if (paramName != null) {
        location =
            new URL(
                location.toExternalForm()
                    + (location.getQuery() == null ? '?' : '&')
                    + "param="
                    + URLEncoder.encode(paramName, "UTF-8"));
      }
    } catch (MalformedURLException | UnsupportedEncodingException ignored) {
    }
    return location;
  }

  private static void bindColumn(BoundStatement bs, Object raw, String columnName) {
    if (raw == null) {
      // TODO null -> unset
      bs.setToNull(columnName);
    } else {
      @SuppressWarnings("unchecked")
      Class<Object> targetClass = (Class<Object>) raw.getClass();
      bs.set(columnName, raw, targetClass);
    }
  }
}
