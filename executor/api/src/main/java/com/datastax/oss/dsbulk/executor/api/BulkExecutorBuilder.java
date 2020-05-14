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

import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.executor.api.result.Result;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;

/** A builder for {@link BulkExecutor} instances. */
public interface BulkExecutorBuilder<T extends BulkExecutor> {

  /**
   * Switches on fail-safe mode.
   *
   * <p>By default, executors are created in fail-fast mode, i.e. any execution error stops the
   * whole operation. In fail-fast mode, the error is wrapped within a {@link
   * BulkExecutionException} that conveys the failed {@link Statement}. With synchronous operations,
   * the exception is thrown directly; with asynchronous ones, the returned future is completed
   * exceptionally. <b>Important</b>: in fail-fast mode, if a statement execution fails, all pending
   * requests are abandoned; there is no guarantee that all previously submitted statements will
   * complete before the executor stops.
   *
   * <p>In fail-safe mode, the error is converted into a {@link WriteResult} or {@link ReadResult}
   * and passed on to consumers, along with the failed {@link Statement}; then the execution resumes
   * at the next statement. The {@link Result} interface exposes two useful methods when operating
   * in fail-safe mode:
   *
   * <ol>
   *   <li>{@link Result#isSuccess()} tells if the statement was executed successfully.
   *   <li>{@link Result#getError()} can be used to retrieve the error.
   * </ol>
   *
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  BulkExecutorBuilder<T> failSafe();

  /**
   * Sets the maximum number of in-flight requests. In other words, sets the maximum number of
   * concurrent uncompleted requests waiting for a response from the server. If that limit is
   * reached, the executor will block until the number of in-flight requests drops below the
   * threshold. <em>This feature should not be used in a fully non-blocking application</em>.
   *
   * <p>This acts as a safeguard against workflows that generate more requests than they can handle.
   * The default is {@link AbstractBulkExecutor#DEFAULT_MAX_IN_FLIGHT_REQUESTS}. Setting this option
   * to any negative value will disable it.
   *
   * @param maxInFlightRequests the maximum number of in-flight requests.
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  BulkExecutorBuilder<T> withMaxInFlightRequests(int maxInFlightRequests);

  /**
   * Sets the maximum number of concurrent requests per second. If that limit is reached, the
   * executor will block until the number of requests per second drops below the threshold. <em>This
   * feature should not be used in a fully non-blocking application</em>.
   *
   * <p>This acts as a safeguard against workflows that could overwhelm the cluster with more
   * requests than it can handle. The default is {@link
   * AbstractBulkExecutor#DEFAULT_MAX_REQUESTS_PER_SECOND}. Setting this option to any negative
   * value will disable it.
   *
   * @param maxRequestsPerSecond the maximum number of concurrent requests per second.
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  BulkExecutorBuilder<T> withMaxRequestsPerSecond(int maxRequestsPerSecond);

  /**
   * Sets an optional {@link ExecutionListener}.
   *
   * @param listener the {@link ExecutionListener} to use.
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  BulkExecutorBuilder<T> withExecutionListener(ExecutionListener listener);

  /**
   * Builds a new instance.
   *
   * @return the newly-created instance.
   */
  T build();
}
