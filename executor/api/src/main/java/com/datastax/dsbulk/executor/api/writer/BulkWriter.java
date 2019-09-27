/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.writer;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
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
  default WriteResult writeSync(Statement<?> statement) throws BulkExecutionException {
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
