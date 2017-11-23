/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.internal.statement.BulkSimpleStatement;

/** */
public class CQLRecordMapper implements RecordMapper {

  @Override
  public Statement map(Record record) {
    String queryString = (String) record.values().iterator().next();
    return new BulkSimpleStatement<>(record, queryString);
  }
}
