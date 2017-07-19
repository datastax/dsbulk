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
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.engine.schema.RecordMapper;
import java.util.Map;

/** */
public class DefaultRecordMapper implements RecordMapper {

  private final PreparedStatement insertStatement;

  private final Map<String, String> mapping;

  public DefaultRecordMapper(PreparedStatement insertStatement, Map<String, String> mapping) {
    this.insertStatement = insertStatement;
    this.mapping = mapping;
  }

  public Statement map(Record record) {
    BoundStatement bs = insertStatement.bind();
    for (Map.Entry<Object, Object> cell : record.entrySet()) {
      Object raw = cell.getValue();
      String col = mapping.get(cell.getKey().toString());
      if (col != null) {
        @SuppressWarnings("unchecked")
        Class<Object> targetClass = (Class<Object>) raw.getClass();
        // TODO null -> unset
        bs.set(col, raw, targetClass);
      }
    }
    return bs;
  }

  public PreparedStatement getInsertStatement() {
    return insertStatement;
  }

  public Map<String, String> getMapping() {
    return mapping;
  }
}
