/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.dsbulk.executor.api.reader.ReactiveBulkReader;
import com.datastax.dsbulk.executor.api.writer.ReactiveBulkWriter;

/**
 * An execution unit for {@link ReactiveBulkWriter bulk writes} and {@link ReactiveBulkReader bulk
 * reads} that operates in reactive mode.
 */
public interface ReactiveBulkExecutor extends ReactiveBulkWriter, ReactiveBulkReader {}
