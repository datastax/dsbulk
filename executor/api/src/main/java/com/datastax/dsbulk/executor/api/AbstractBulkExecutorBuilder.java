/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.Result;
import com.datastax.dsbulk.executor.api.result.WriteResult;

/** A builder for {@link AbstractBulkExecutor} instances. */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractBulkExecutorBuilder<T extends AbstractBulkExecutor> {

  protected final Session session;

  protected boolean failFast = true;

  protected int maxInFlightRequests = AbstractBulkExecutor.DEFAULT_MAX_IN_FLIGHT_REQUESTS;

  protected int maxRequestsPerSecond = AbstractBulkExecutor.DEFAULT_MAX_REQUESTS_PER_SECOND;

  protected ExecutionListener listener;

  protected AbstractBulkExecutorBuilder(Session session) {
    this.session = session;
  }

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
  public AbstractBulkExecutorBuilder<T> failSafe() {
    this.failFast = false;
    return this;
  }

  /**
   * Sets the maximum number of "in-flight" requests. In other words, sets the maximum amount of
   * concurrent uncompleted futures waiting for a response from the server. If that limit is
   * reached, the executor will block until the number of in-flight requests drops below the
   * threshold. <em>This feature should not be used in a fully non-blocking application</em>.
   *
   * <p>This acts as a safeguard against workflows that generate more requests that they can handle.
   * The default is {@link
   * com.datastax.dsbulk.executor.api.AbstractBulkExecutor#DEFAULT_MAX_IN_FLIGHT_REQUESTS}. Setting
   * this option to any negative value will disable it.
   *
   * @param maxInFlightRequests the maximum number of "in-flight" requests.
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withMaxInFlightRequests(int maxInFlightRequests) {
    this.maxInFlightRequests = maxInFlightRequests;
    return this;
  }

  /**
   * Sets the maximum number of concurrent requests per second. If that limit is reached, the
   * executor will block until the number of requests per second drops below the threshold. <em>This
   * feature should not be used in a fully non-blocking application</em>.
   *
   * <p>This acts as a safeguard against workflows that could overwhelm the cluster with more
   * requests that it can handle. The default is {@link
   * com.datastax.dsbulk.executor.api.AbstractBulkExecutor#DEFAULT_MAX_REQUESTS_PER_SECOND}. Setting
   * this option to any negative value will disable it.
   *
   * @param maxRequestsPerSecond the maximum number of concurrent requests per second.
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withMaxRequestsPerSecond(int maxRequestsPerSecond) {
    this.maxRequestsPerSecond = maxRequestsPerSecond;
    return this;
  }

  /**
   * Sets an optional {@link ExecutionListener}.
   *
   * @param listener the {@link ExecutionListener} to use.
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withExecutionListener(ExecutionListener listener) {
    this.listener = listener;
    return this;
  }

  /**
   * Builds a new instance.
   *
   * @return the newly-created instance.
   */
  public abstract T build();
}
