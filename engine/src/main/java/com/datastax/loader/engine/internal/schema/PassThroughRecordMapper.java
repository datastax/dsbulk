/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.engine.schema.RecordMapper;

/** */
public class PassThroughRecordMapper implements RecordMapper {

  public Statement map(Record record) {

    // TODO required for CQLConnector, because it already produces Statements
    return new SimpleStatement(record.values().iterator().next().toString());
  }
}
