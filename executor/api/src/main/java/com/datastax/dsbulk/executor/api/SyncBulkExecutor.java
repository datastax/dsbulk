/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.dsbulk.executor.api.reader.SyncBulkReader;
import com.datastax.dsbulk.executor.api.writer.SyncBulkWriter;

/**
 * An execution unit for {@link SyncBulkWriter bulk writes} and {@link SyncBulkReader bulk reads}
 * that operates in synchronous mode.
 */
public interface SyncBulkExecutor extends SyncBulkWriter, SyncBulkReader {}
