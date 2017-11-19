/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

/** Base class for implementations of {@link BulkExecutor}. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class AbstractBulkExecutor implements BulkExecutor, AutoCloseable {

  /** The default number of maximum in-flight requests. */
  static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 1_000;

  /** The default maximum number of concurrent requests per second. */
  static final int DEFAULT_MAX_REQUESTS_PER_SECOND = 100_000;

  static final Supplier<Executor> DEFAULT_EXECUTOR_SUPPLIER =
      () ->
          new ThreadPoolExecutor(
              0,
              Runtime.getRuntime().availableProcessors() * 4,
              60,
              SECONDS,
              new SynchronousQueue<>(),
              new ThreadFactoryBuilder().setDaemon(true).setNameFormat("bulk-executor-%d").build(),
              new ThreadPoolExecutor.CallerRunsPolicy());

  protected final Session session;

  protected final boolean failFast;

  protected final Optional<Semaphore> requestPermits;

  protected final Optional<RateLimiter> rateLimiter;

  protected final Optional<ExecutionListener> listener;

  protected final Executor executor;

  protected final QueueFactory<ReadResult> queueFactory;

  protected AbstractBulkExecutor(Session session) {
    this(
        session,
        true,
        DEFAULT_MAX_IN_FLIGHT_REQUESTS,
        DEFAULT_MAX_REQUESTS_PER_SECOND,
        null,
        DEFAULT_EXECUTOR_SUPPLIER.get(),
        defaultQueueFactory(session));
  }

  protected AbstractBulkExecutor(AbstractBulkExecutorBuilder<?> builder) {
    this(
        builder.session,
        builder.failFast,
        builder.maxInFlightRequests,
        builder.maxRequestsPerSecond,
        builder.listener,
        builder.executor.get(),
        builder.queueFactory);
  }

  private AbstractBulkExecutor(
      Session session,
      boolean failFast,
      int maxInFlightRequests,
      int maxRequestsPerSecond,
      ExecutionListener listener,
      Executor executor,
      QueueFactory<ReadResult> queueFactory) {
    Objects.requireNonNull(session, "session cannot be null");
    Objects.requireNonNull(executor, "executor cannot be null");
    Objects.requireNonNull(queueFactory, "queueFactory cannot be null");
    this.session = session;
    this.failFast = failFast;
    this.requestPermits =
        maxInFlightRequests < 0
            ? Optional.empty()
            : Optional.of(new Semaphore(maxInFlightRequests));
    this.rateLimiter =
        maxRequestsPerSecond < 0
            ? Optional.empty()
            : Optional.of(RateLimiter.create(maxRequestsPerSecond));
    this.listener = Optional.ofNullable(listener);
    this.executor = executor;
    this.queueFactory = queueFactory;
  }

  private static QueueFactory<ReadResult> defaultQueueFactory(Session session) {
    return statement -> {
      int fetchSize = statement.getFetchSize();
      if (fetchSize <= 0) {
        fetchSize = session.getCluster().getConfiguration().getQueryOptions().getFetchSize();
      }
      return new ArrayBlockingQueue<>(fetchSize * 4);
    };
  }

  @Override
  public void close() throws InterruptedException {
    if (executor instanceof ExecutorService) {
      ExecutorService tpe = (ExecutorService) executor;
      tpe.shutdown();
      tpe.awaitTermination(5, SECONDS);
      tpe.shutdownNow();
    }
  }
}
