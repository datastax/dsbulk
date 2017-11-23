/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.executor.api.result.ReadResult;

public interface ReadResultMapper {

  Record map(ReadResult result);
}
