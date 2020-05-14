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
package com.datastax.oss.dsbulk.executor.api;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.concurrent.Semaphore;

/** Base class for implementations of {@link BulkExecutor}. */
public abstract class AbstractBulkExecutor implements BulkExecutor, AutoCloseable {

  /** The default number of maximum in-flight requests. */
  static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 1_000;

  /** The default maximum number of concurrent requests per second. */
  static final int DEFAULT_MAX_REQUESTS_PER_SECOND = 100_000;

  protected final @NonNull CqlSession session;

  protected final boolean failFast;

  protected final @Nullable Semaphore maxConcurrentRequests;

  protected final @Nullable RateLimiter rateLimiter;

  protected final @Nullable ExecutionListener listener;

  protected AbstractBulkExecutor(CqlSession session) {
    this(session, true, DEFAULT_MAX_IN_FLIGHT_REQUESTS, DEFAULT_MAX_REQUESTS_PER_SECOND, null);
  }

  protected AbstractBulkExecutor(AbstractBulkExecutorBuilder<?> builder) {
    this(
        builder.session,
        builder.failFast,
        builder.maxInFlightRequests,
        builder.maxRequestsPerSecond,
        builder.listener);
  }

  private AbstractBulkExecutor(
      @NonNull CqlSession session,
      boolean failFast,
      int maxInFlightRequests,
      int maxRequestsPerSecond,
      @Nullable ExecutionListener listener) {
    Objects.requireNonNull(session, "session cannot be null");
    this.session = session;
    this.failFast = failFast;
    this.maxConcurrentRequests =
        maxInFlightRequests <= 0 ? null : new Semaphore(maxInFlightRequests);
    this.rateLimiter = maxRequestsPerSecond <= 0 ? null : RateLimiter.create(maxRequestsPerSecond);
    this.listener = listener;
  }

  @Override
  public void close() {
    // no-op
  }
}
