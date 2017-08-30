/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
    extends BulkWriter, BulkReader, SyncBulkExecutor, AsyncBulkExecutor, ReactiveBulkExecutor {

  @Override
  void close();
}
