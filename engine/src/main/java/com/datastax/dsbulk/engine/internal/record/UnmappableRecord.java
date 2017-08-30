/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.record;

import com.datastax.dsbulk.connectors.api.Record;

/** */
public interface UnmappableRecord extends Record {

  Throwable getError();
}
