/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.publisher;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.internal.subscription.ContinuousReadResultSubscription;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class ContinuousReadResultPublisher implements Publisher<ReadResult> {

  private final Statement statement;
  private final ContinuousPagingSession session;
  private final ContinuousPagingOptions options;
  private final Optional<ExecutionListener> listener;
  private final Optional<Semaphore> requestPermits;
  private final Optional<RateLimiter> rateLimiter;
  private final boolean failFast;

  public ContinuousReadResultPublisher(
      Statement statement,
      ContinuousPagingSession session,
      ContinuousPagingOptions options,
      Optional<ExecutionListener> listener,
      Optional<Semaphore> requestPermits,
      Optional<RateLimiter> rateLimiter,
      boolean failFast) {
    this.statement = statement;
    this.session = session;
    this.options = options;
    this.listener = listener;
    this.requestPermits = requestPermits;
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
            subscriber, statement, listener, requestPermits, rateLimiter, failFast);
    try {
      subscriber.onSubscribe(subscription);
      // must be called after onSubscribe
      subscription.start(() -> session.executeContinuouslyAsync(statement, options));
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
