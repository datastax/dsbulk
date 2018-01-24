/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.dsbulk.executor.api.reader.SyncBulkReader;
import com.datastax.dsbulk.executor.api.writer.SyncBulkWriter;

/**
 * An execution unit for {@link SyncBulkWriter bulk writes} and {@link SyncBulkReader bulk reads}
 * that operates in synchronous mode.
 */
public interface SyncBulkExecutor extends SyncBulkWriter, SyncBulkReader {}
