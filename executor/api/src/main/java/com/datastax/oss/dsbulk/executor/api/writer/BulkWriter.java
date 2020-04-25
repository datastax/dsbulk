/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.executor.api.writer;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
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
