/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.listener.ExecutionListener;
import com.datastax.loader.executor.api.result.ReadResult;
import com.datastax.loader.executor.api.result.Result;
import com.datastax.loader.executor.api.result.WriteResult;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/** A builder for {@link AbstractBulkExecutor} instances. */
public abstract class AbstractBulkExecutorBuilder<T extends AbstractBulkExecutor> {

  final Session session;

  boolean failFast = true;

  int maxInFlightRequests = AbstractBulkExecutor.DEFAULT_MAX_INFLIGHT_REQUESTS;

  int maxRequestsPerSecond = AbstractBulkExecutor.DEFAULT_MAX_REQUESTS_PER_SECOND;

  ExecutionListener listener;

  Executor executor;

  AbstractBulkExecutorBuilder(Session session) {
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
   * concurrent uncompleted futures waiting for a response from the server. This acts as a safeguard
   * against workflows that generate more requests that they can handle. The default is {@link
   * com.datastax.loader.executor.api.AbstractBulkExecutor#DEFAULT_MAX_INFLIGHT_REQUESTS}. Setting
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
   * Sets the maximum number of concurrent requests per second. This acts as a safeguard against
   * workflows that could overwhelm the cluster with more requests that it can handle. The default
   * is {@link
   * com.datastax.loader.executor.api.AbstractBulkExecutor#DEFAULT_MAX_REQUESTS_PER_SECOND}. Setting
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
   * Sets the internal {@link Executor}.
   *
   * <p>The internal executor is responsible for sending requests, reading responses and executing
   * consumers. Note that none of these operations is ever made on a driver internal thread.
   *
   * <p>By default, the internal executor is a {@link ThreadPoolExecutor} configured with 0 threads
   * initially, but the amount of threads is allowed to grow up to 4 times the number of available
   * cores. Its {@link java.util.concurrent.ThreadPoolExecutor#getRejectedExecutionHandler()
   * RejectedExecutionHandler} is {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy},
   * which is a simple way to apply backpressure to upstream producers.
   *
   * @param executor the {@link Executor} to use.
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withExecutor(Executor executor) {
    this.executor = executor;
    return this;
  }

  /**
   * Builds a new instance.
   *
   * @return the newly-created instance.
   */
  public abstract T build();
}
