/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.publisher;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.internal.subscription.WriteResultSubscription;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A {@link Publisher} for {@link WriteResult}s.
 *
 * @see com.datastax.dsbulk.executor.api.AbstractBulkExecutor#writeReactive(Statement)
 */
@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "UnstableApiUsage"})
public class WriteResultPublisher implements Publisher<WriteResult> {

  private final Statement statement;
  private final Session session;
  private final Optional<ExecutionListener> listener;
  private final Optional<Semaphore> requestPermits;
  private final Optional<RateLimiter> rateLimiter;
  private final boolean failFast;

  /**
   * Creates a new {@link WriteResultPublisher} without {@link ExecutionListener} and without
   * throughput regulation.
   *
   * @param statement The {@link Statement} to execute.
   * @param session The {@link Session} to use.
   * @param failFast whether to fail-fast in case of error.
   */
  public WriteResultPublisher(
      @NotNull Statement statement, @NotNull Session session, boolean failFast) {
    this(statement, session, failFast, Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Creates a new {@link WriteResultPublisher}.
   *
   * @param statement The {@link Statement} to execute.
   * @param session The {@link Session} to use.
   * @param failFast whether to fail-fast in case of error.
   * @param listener The {@link ExecutionListener} to use.
   * @param requestPermits The {@link Semaphore} to use to regulate the amount of in-flight
   *     requests.
   * @param rateLimiter The {@link RateLimiter} to use to regulate throughput.
   */
  public WriteResultPublisher(
      @NotNull Statement statement,
      @NotNull Session session,
      boolean failFast,
      @NotNull Optional<ExecutionListener> listener,
      @NotNull Optional<Semaphore> requestPermits,
      @NotNull Optional<RateLimiter> rateLimiter) {
    this.statement = statement;
    this.session = session;
    this.listener = listener;
    this.requestPermits = requestPermits;
    this.rateLimiter = rateLimiter;
    this.failFast = failFast;
  }

  @Override
  public void subscribe(Subscriber<? super WriteResult> subscriber) {
    // As per rule 1.9, we need to throw an NPE if subscriber is null
    Objects.requireNonNull(subscriber, "Subscriber cannot be null");
    // As per rule 1.11, this publisher supports multiple subscribers in a unicast configuration,
    // i.e., each subscriber triggers an independent execution/subscription and gets its own copy
    // of the results.
    WriteResultSubscription subscription =
        new WriteResultSubscription(
            subscriber, statement, listener, requestPermits, rateLimiter, failFast);
    try {
      subscriber.onSubscribe(subscription);
      // must be called after onSubscribe
      subscription.start(() -> session.executeAsync(statement));
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
