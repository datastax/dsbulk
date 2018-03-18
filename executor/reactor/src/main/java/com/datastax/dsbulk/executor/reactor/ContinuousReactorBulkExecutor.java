/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.internal.subscription.ContinuousReadResultSubscription;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

/**
 * An implementation of {@link BulkExecutor} using <a href="https://projectreactor.io">Reactor</a>,
 * that executes all reads using continuous paging. This executor can achieve significant
 * performance improvements for reads, provided that the read statements to be executed can be
 * properly routed to a replica.
 */
public class ContinuousReactorBulkExecutor extends DefaultReactorBulkExecutor
    implements ReactorBulkExecutor {

  /**
   * Creates a new builder for {@link ContinuousReactorBulkExecutor} instances using the given
   * {@link ContinuousPagingSession}.
   *
   * @param session the {@link ContinuousPagingSession} to use.
   * @return a new builder.
   */
  public static ContinuousReactorBulkExecutorBuilder builder(ContinuousPagingSession session) {
    return new ContinuousReactorBulkExecutorBuilder(session);
  }

  private final ContinuousPagingSession continuousPagingSession;
  private final ContinuousPagingOptions options;

  /**
   * Creates a new instance using the given {@link ContinuousPagingSession} and using defaults for
   * all parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(ContinuousPagingSession)
   * builder} method instead.
   *
   * @param continuousPagingSession the {@link ContinuousPagingSession} to use.
   */
  public ContinuousReactorBulkExecutor(ContinuousPagingSession continuousPagingSession) {
    this(continuousPagingSession, ContinuousPagingOptions.builder().build());
  }

  /**
   * Creates a new instance using the given {@link ContinuousPagingSession}, the given {@link
   * ContinuousPagingOptions}, and using defaults for all other parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(ContinuousPagingSession)
   * builder} method instead.
   *
   * @param continuousPagingSession the {@link ContinuousPagingSession} to use.
   * @param options the {@link ContinuousPagingOptions} to use.
   */
  public ContinuousReactorBulkExecutor(
      ContinuousPagingSession continuousPagingSession, ContinuousPagingOptions options) {
    super(continuousPagingSession);
    this.continuousPagingSession = continuousPagingSession;
    this.options = options;
  }

  ContinuousReactorBulkExecutor(ContinuousReactorBulkExecutorBuilder builder) {
    super(builder);
    this.continuousPagingSession = builder.continuousPagingSession;
    this.options = builder.options;
  }

  @Override
  public Flux<ReadResult> readReactive(Statement statement) {
    Objects.requireNonNull(statement);
    return Flux.from(new ContinuousReadResultPublisher(statement));
  }

  private class ContinuousReadResultPublisher implements Publisher<ReadResult> {

    private final Statement statement;

    private ContinuousReadResultPublisher(Statement statement) {
      this.statement = statement;
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
        subscription.start(
            () -> continuousPagingSession.executeContinuouslyAsync(statement, options));
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
}
