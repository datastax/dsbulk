/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.publisher;

import com.datastax.dsbulk.executor.api.internal.subscription.ContinuousReadResultSubscription;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A {@link Publisher} for {@link ReadResult}s that uses continuous paging.
 *
 * @see com.datastax.dsbulk.executor.api.AbstractBulkExecutor#readReactive(Statement)
 */
@SuppressWarnings("UnstableApiUsage")
public class ContinuousReadResultPublisher implements Publisher<ReadResult> {

  private final @NonNull Statement<?> statement;
  private final @NonNull ContinuousSession session;
  private final @Nullable ExecutionListener listener;
  private final @Nullable Semaphore maxConcurrentRequests;
  private final @Nullable Semaphore maxConcurrentQueries;
  private final @Nullable RateLimiter rateLimiter;
  private final boolean failFast;

  /**
   * Creates a new {@link ContinuousReadResultPublisher} with default paging options, without {@link
   * ExecutionListener} and without throughput regulation.
   *
   * @param statement The {@link Statement} to execute.
   * @param session The {@link ContinuousSession} to use.
   * @param failFast whether to fail-fast in case of error.
   */
  public ContinuousReadResultPublisher(
      @NonNull Statement<?> statement, @NonNull ContinuousSession session, boolean failFast) {
    this(statement, session, failFast, null, null, null, null);
  }

  /**
   * Creates a new {@link ContinuousReadResultPublisher}.
   *
   * @param statement The {@link Statement} to execute.
   * @param session The {@link ContinuousSession} to use.
   * @param failFast whether to fail-fast in case of error.
   * @param listener The {@link ExecutionListener} to use.
   * @param maxConcurrentRequests The {@link Semaphore} to use to regulate the amount of in-flight
   *     requests.
   * @param maxConcurrentQueries The {@link Semaphore} to use to regulate the amount of in-flight
   *     queries.
   * @param rateLimiter The {@link RateLimiter} to use to regulate throughput.
   */
  public ContinuousReadResultPublisher(
      @NonNull Statement<?> statement,
      @NonNull ContinuousSession session,
      boolean failFast,
      @Nullable ExecutionListener listener,
      @Nullable Semaphore maxConcurrentRequests,
      @Nullable Semaphore maxConcurrentQueries,
      @Nullable RateLimiter rateLimiter) {
    this.statement = statement;
    this.session = session;
    this.listener = listener;
    this.maxConcurrentRequests = maxConcurrentRequests;
    this.maxConcurrentQueries = maxConcurrentQueries;
    this.rateLimiter = rateLimiter;
    this.failFast = failFast;
  }

  @Override
  public void subscribe(Subscriber<? super ReadResult> subscriber) {
    // As per rule 1.9, we need to throw an NPE if subscriber is null
    Objects.requireNonNull(subscriber, "Subscriber cannot be null");
    // As per rule 1.11, this publisher supports multiple subscribers in a unicast configuration,
    // i.e., each subscriber triggers an independent execution/subscription and gets its own copy
    // of the results.
    ContinuousReadResultSubscription subscription =
        new ContinuousReadResultSubscription(
            subscriber,
            statement,
            listener,
            maxConcurrentRequests,
            maxConcurrentQueries,
            rateLimiter,
            failFast);
    try {
      subscriber.onSubscribe(subscription);
      // must be called after onSubscribe
      subscription.start(() -> session.executeContinuouslyAsync(statement));
    } catch (Throwable t) {
      // As per rule 2.13: In the case that this rule is violated,
      // any associated Subscription to the Subscriber MUST be considered as
      // cancelled, and the caller MUST raise this error condition in a fashion
      // that is adequate for the runtime environment.
      subscription.doOnError(
          new IllegalStateException(
              subscriber
                  + " violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.",
              t));
    }
    // As per 2.13, this method must return normally (i.e. not throw)

  }
}
