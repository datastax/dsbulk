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
package com.datastax.oss.dsbulk.executor.api.subscription;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.oss.dsbulk.executor.api.result.DefaultWriteResult;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class WriteResultSubscription extends ResultSubscription<WriteResult, AsyncResultSet> {

  public WriteResultSubscription(
      @NonNull Subscriber<? super WriteResult> subscriber,
      @NonNull Statement<?> statement,
      @Nullable ExecutionListener listener,
      @Nullable Semaphore maxConcurrentRequests,
      @Nullable Semaphore maxConcurrentQueries,
      @Nullable RateLimiter rateLimiter,
      boolean failFast) {
    super(
        subscriber,
        statement,
        listener,
        maxConcurrentRequests,
        maxConcurrentQueries,
        rateLimiter,
        failFast);
  }

  @Override
  Page toPage(AsyncResultSet rs, ExecutionContext local) {
    Iterator<WriteResult> iterator =
        Collections.<WriteResult>singleton(new DefaultWriteResult(statement, rs)).iterator();
    return new Page(iterator, null);
  }

  @Override
  WriteResult toErrorResult(BulkExecutionException error) {
    return new DefaultWriteResult(error);
  }

  @Override
  void onBeforeRequestStarted() {
    if (rateLimiter != null) {
      rateLimiter.acquire(batchSize);
    }
    super.onBeforeRequestStarted();
  }

  @Override
  void onRequestStarted(ExecutionContext local) {
    if (listener != null) {
      listener.onWriteRequestStarted(statement, local);
    }
  }

  @Override
  void onRequestSuccessful(AsyncResultSet rs, ExecutionContext local) {
    if (listener != null) {
      listener.onWriteRequestSuccessful(statement, local);
    }
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    if (listener != null) {
      listener.onWriteRequestFailed(statement, t, local);
    }
  }
}
