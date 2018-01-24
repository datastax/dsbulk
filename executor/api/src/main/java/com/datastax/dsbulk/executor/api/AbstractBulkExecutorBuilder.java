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
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

/** A builder for {@link AbstractBulkExecutor} instances. */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractBulkExecutorBuilder<T extends AbstractBulkExecutor> {

  protected final Session session;

  protected boolean failFast = true;

  protected int maxInFlightRequests = AbstractBulkExecutor.DEFAULT_MAX_IN_FLIGHT_REQUESTS;

  protected int maxRequestsPerSecond = AbstractBulkExecutor.DEFAULT_MAX_REQUESTS_PER_SECOND;

  protected ExecutionListener listener;

  protected Supplier<Executor> executor = AbstractBulkExecutor.DEFAULT_EXECUTOR_SUPPLIER;

  protected QueueFactory<ReadResult> queueFactory = null;

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
   * concurrent uncompleted futures waiting for a response from the server. This acts as a safeguard
   * against workflows that generate more requests that they can handle. The default is {@link
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
   * Sets the maximum number of concurrent requests per second. This acts as a safeguard against
   * workflows that could overwhelm the cluster with more requests that it can handle. The default
   * is {@link
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
   * Sets the internal {@link Executor}.
   *
   * <p>The internal executor is used to perform the following tasks:
   *
   * <ol>
   *   <li>In synchronous and asynchronous modes: for sending requests, receiving responses and
   *       executing consumers.
   *   <li>In reactive mode: for receiving responses only (requests are sent on the subscribing
   *       thread).
   * </ol>
   *
   * <p>By default, the internal executor is a {@link ThreadPoolExecutor} configured with 0 threads
   * initially, but the amount of threads is allowed to grow up to 4 times the number of available
   * cores. Its {@link java.util.concurrent.ThreadPoolExecutor#getRejectedExecutionHandler()
   * RejectedExecutionHandler} is {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy},
   * which means that rejected executions will be executed on the driver's internal event loop,
   * which can be seen as a simple way to apply backpressure to upstream producers by slowing down
   * the driver itself.
   *
   * @param executor the {@link Executor} to use, or {@code null} to execute tasks on the calling
   *     thread.
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withExecutor(Executor executor) {
    Objects.requireNonNull(executor, "executor cannot be null");
    this.executor = () -> executor;
    return this;
  }

  /**
   * Disables the use of an internal {@link Executor}.
   *
   * <p>Calling this method will cause the following tasks to be executed on the calling thread:
   *
   * <ol>
   *   <li>Requests and consumers will be executed on the application thread that triggered the
   *       operation.
   *   <li>Responses will be processed on the driver's internal event loop.
   * </ol>
   *
   * <p><strong>IMPORTANT</strong>
   *
   * <p>Disabling the internal executor may increase throughput if consumers and subscribers are
   * fast enough and never block, but can also have the opposite effect, if they are too slow or
   * perform blocking operations.
   *
   * <p>Deadlocks are also possible if downstream subscribers reuse results to execute more
   * requests; for example, the following pseudo-code is likely to deadlock without an internal
   * executor, because the writes will be performed <em>synchronously</em> on the driver's internal
   * event loop:
   *
   * <pre>{@code
   * executor.readReactive("SELECT ...")
   *    .map(result -> "INSERT INTO ...")
   *    .flatMap(executor::writeReactive)
   *    .blockingSubscribe();
   * }</pre>
   *
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withoutExecutor() {
    this.executor = MoreExecutors::directExecutor;
    return this;
  }

  /**
   * Sets the {@link QueueFactory} to use when executing read requests.
   *
   * <p>By default, the queue factory will create {@link ArrayBlockingQueue} instances whose size is
   * 4 times the statement's {@link Statement#getFetchSize() fetch size}.
   *
   * @param queueFactory the {@link QueueFactory} to use; cannot be {@code null}.
   * @return this builder (for method chaining).
   */
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withQueueFactory(QueueFactory<ReadResult> queueFactory) {
    Objects.requireNonNull(queueFactory, "queueFactory must not be null");
    this.queueFactory = queueFactory;
    return this;
  }

  /**
   * Builds a new instance.
   *
   * @return the newly-created instance.
   */
  public abstract T build();
}
