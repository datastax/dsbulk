/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.Statement;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.engine.internal.statement.BulkSimpleStatement;

/** */
public class CQLRecordMapper implements RecordMapper {

  @Override
  public Statement map(Record record) {
    String field = record.fields().iterator().next();
    String queryString = (String) record.getFieldValue(field);
    return new BulkSimpleStatement<>(record, queryString);
  }
}
