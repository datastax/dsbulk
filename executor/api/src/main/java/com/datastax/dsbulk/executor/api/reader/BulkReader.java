/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.reader;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.driver.api.core.cql.Statement;
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
