/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api;

import com.datastax.loader.engine.api.reader.AsyncBulkReader;
import com.datastax.loader.engine.api.writer.AsyncBulkWriter;

/**
 * An execution unit for {@link AsyncBulkWriter bulk writes} and {@link AsyncBulkReader bulk reads}
 * that operates in asynchronous mode.
 */
public interface AsyncBulkExecutor extends AsyncBulkWriter, AsyncBulkReader {}
