/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.dsbulk.executor.api.reader.BulkReader;
import com.datastax.dsbulk.executor.api.writer.BulkWriter;

/**
 * An execution unit for {@link BulkWriter bulk writes} and {@link BulkReader bulk reads} that
 * operates in 3 distinct modes: {@link SyncBulkExecutor synchronous} (blocking), {@link
 * AsyncBulkExecutor asynchronous} (non-blocking), and {@link ReactiveBulkExecutor reactive}.
 */
public interface BulkExecutor
    extends BulkWriter, BulkReader, SyncBulkExecutor, AsyncBulkExecutor, ReactiveBulkExecutor {}
