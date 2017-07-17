/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api;

import com.datastax.loader.engine.api.reader.SyncBulkReader;
import com.datastax.loader.engine.api.writer.SyncBulkWriter;

/**
 * An execution unit for {@link SyncBulkWriter bulk writes} and {@link SyncBulkReader bulk reads}
 * that operates in synchronous mode.
 */
public interface SyncBulkExecutor extends SyncBulkWriter, SyncBulkReader {}
