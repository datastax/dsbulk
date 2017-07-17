/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api.reader;

import com.datastax.driver.core.Statement;
import com.datastax.loader.engine.api.exception.BulkExecutionException;
import com.datastax.loader.engine.api.result.ReadResult;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;

/**
 * A bulk reader that operates in 3 distinct modes:
 *
 * <ol>
 *   <li>{@link SyncBulkReader Synchronous};
 *   <li>{@link AsyncBulkReader Asynchronous};
 *   <li>{@link ReactiveBulkReader Reactive}.
 * </ol>
 */
public interface BulkReader extends SyncBulkReader, AsyncBulkReader, ReactiveBulkReader {

  @Override
  default void readSync(Statement statement, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    try {
      Uninterruptibles.getUninterruptibly(readAsync(statement, consumer));
    } catch (ExecutionException e) {
      throw ((BulkExecutionException) e.getCause());
    }
  }

  @Override
  default void readSync(
      Publisher<? extends Statement> statements, Consumer<? super ReadResult> consumer)
      throws BulkExecutionException {
    try {
      Uninterruptibles.getUninterruptibly(readAsync(statements, consumer));
    } catch (ExecutionException e) {
      throw ((BulkExecutionException) e.getCause());
    }
  }
}
