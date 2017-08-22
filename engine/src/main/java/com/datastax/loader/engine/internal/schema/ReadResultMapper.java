/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.executor.api.result.ReadResult;

public interface ReadResultMapper {

  Record map(ReadResult result);
}
