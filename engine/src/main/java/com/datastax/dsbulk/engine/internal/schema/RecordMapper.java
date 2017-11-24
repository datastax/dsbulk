/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.connectors.api.Record;

/** */
public interface RecordMapper {

  Statement map(Record record);
}
