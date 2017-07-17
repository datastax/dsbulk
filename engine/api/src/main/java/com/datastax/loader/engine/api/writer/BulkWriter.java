/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api.writer;

import com.datastax.driver.core.Statement;
import com.datastax.loader.engine.api.exception.BulkExecutionException;
import com.datastax.loader.engine.api.result.WriteResult;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;

/**
 * A bulk writer that operates in 3 distinct modes:
 *
 * <ol>
 *   <li>{@link SyncBulkWriter Synchronous};
 *   <li>{@link AsyncBulkWriter Asynchronous};
 *   <li>{@link ReactiveBulkWriter Reactive}.
 * </ol>
 */
public interface BulkWriter extends SyncBulkWriter, AsyncBulkWriter, ReactiveBulkWriter {

  @Override
  default WriteResult writeSync(Statement statement) throws BulkExecutionException {
    try {
      return Uninterruptibles.getUninterruptibly(writeAsync(statement));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) throw ((RuntimeException) cause);
      throw new RuntimeException(cause);
    }
  }

  @Override
  default void writeSync(
      Publisher<? extends Statement> statements, Consumer<? super WriteResult> consumer)
      throws BulkExecutionException {
    try {
      Uninterruptibles.getUninterruptibly(writeAsync(statements, consumer));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) throw ((RuntimeException) cause);
      throw new RuntimeException(cause);
    }
  }
}
