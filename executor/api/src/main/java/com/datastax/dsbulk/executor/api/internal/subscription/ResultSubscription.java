/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.listener.DefaultExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.Result;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jctools.queues.SpscArrayQueue;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single-subscriber subscription that executes the provided {@link Statement} with the provided
 * {@link Session} and emits all the rows returned by the query to its {@link Subscriber}.
 *
 * @param <P> the page type, one of {@link com.datastax.driver.core.ResultSet} or {@link
 *     com.datastax.driver.core.AsyncContinuousPagingResult}.
 * @param <R> the result type ({@link com.datastax.dsbulk.executor.api.result.WriteResult} or {@link
 *     com.datastax.dsbulk.executor.api.result.ReadResult}).
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class ResultSubscription<R extends Result, P> implements Subscription {

  private static final Logger LOG = LoggerFactory.getLogger(ResultSubscription.class);

  private static final int MAX_ENQUEUED_PAGES = 4;

  /*
  The following are specific to the present query execution.
   */

  private Subscriber<? super R> subscriber;
  final Statement statement;

  /*
  The following are supplied by the BulkExecutor and
  are shared with other query executions.
   */

  final Optional<ExecutionListener> listener;
  private final Optional<Semaphore> requestPermits;
  private final Optional<RateLimiter> rateLimiter;
  private final boolean failFast;

  /** The number of writes in the batch. 1 for other types of statement. */
  private final int batchSize;

  /** Tracks the number of items requested by the subscriber. */
  private final AtomicLong requested = new AtomicLong(0);

  /** The pages received so far, with a maximum of MAX_ENQUEUED_PAGES elements. */
  private final Queue<Page> pages = new SpscArrayQueue<>(MAX_ENQUEUED_PAGES);

  /**
   * The last page in the queue (i.e., the queue's tail element). We keep a reference to it to avoid
   * using Deques.
   */
  private volatile Page last = null;

  /**
   * We maintain a separate counter as pages.size() is not a constant-time operation, and is not
   * guaranteed to be 100% accurate for spsc queues.
   */
  private final AtomicInteger pagesSize = new AtomicInteger(0);

  /**
   * Used to signal that a thread is currently draining, i.e., emitting items to the subscriber.
   * When it is zero, that means there is no ongoing emission. This mechanism effectively serializes
   * access to the drain() method, and also keeps track of missed attempts to enter it.
   *
   * @see #drain()
   */
  private final AtomicInteger draining = new AtomicInteger(0);

  /**
   * The global execution context, used to record latencies for the execution as a whole.
   *
   * @see #start(Callable)
   * @see #stop(Optional)
   */
  private final DefaultExecutionContext global = new DefaultExecutionContext();

  /**
   * Waited upon by the driver and completed when the subscriber requests its first item.
   *
   * <p>Used to hold off emitting results until the subscriber issues its first request for items.
   * Since this future is only completed from {@link #request(long)}, this effectively conditions
   * the enqueueing of the first page to the reception of the subscriber's first request.
   *
   * <p>This mechanism avoids sending terminal signals before a request is made when the stream is
   * empty. Note that as per 2.9, "a Subscriber MUST be prepared to receive an onComplete signal
   * with or without a preceding Subscription.request(long n) call." However, the TCK considers it
   * as unfair behavior.
   *
   * @see #start(Callable)
   * @see #request(long)
   */
  private final CompletableFuture<Void> initial = new CompletableFuture<>();

  /**
   * Set to true when the subscription is cancelled, or when an error is encountered, or when the
   * result set is fully consumed.
   */
  private volatile boolean cancelled = false;

  ResultSubscription(
      Subscriber<? super R> subscriber,
      Statement statement,
      Optional<ExecutionListener> listener,
      Optional<Semaphore> requestPermits,
      Optional<RateLimiter> rateLimiter,
      boolean failFast) {
    this.statement = statement;
    this.subscriber = subscriber;
    this.listener = listener;
    this.requestPermits = requestPermits;
    this.rateLimiter = rateLimiter;
    this.failFast = failFast;
    if (statement instanceof BatchStatement) {
      batchSize = ((BatchStatement) statement).size();
    } else {
      batchSize = 1;
    }
  }

  /**
   * Must be called immediately after {@link Subscriber#onSubscribe(Subscription)}.
   *
   * @param initial the future that, once complete, will produce the first page.
   */
  public void start(Callable<ListenableFuture<P>> initial) {
    global.start();
    listener.ifPresent(l -> l.onExecutionStarted(statement, global));
    fetchNextPage(new Page(initial));
  }

  @Override
  public void request(long n) {
    // As per 3.6: after the Subscription is cancelled, additional
    // calls to request() MUST be NOPs.
    if (!cancelled) {
      if (n < 1) {
        // Validate request as per rule 3.9
        doOnError(
            new IllegalArgumentException(
                subscriber
                    + " violated the Reactive Streams rule 3.9 by requesting a non-positive number of elements."));
      } else {
        // As per rule 3.17, when demand overflows Long.MAX_VALUE
        // it can be treated as "effectively unbounded"
        Operators.addCap(requested, n);
        // Set the first future to true if not done yet.
        // This will make the first page of results ready for consumption,
        // see start().
        // As per 2.7 it is the subscriber's responsibility to provide
        // external synchronization when calling request(),
        // so the check-then-act idiom below is good enough
        // (and besides, complete() is idempotent).
        if (!initial.isDone()) {
          initial.complete(null);
        }
        drain();
      }
    }
  }

  @Override
  public void cancel() {
    // As per 3.5: Subscription.cancel() MUST respect the responsiveness of
    // its caller by returning in a timely manner, MUST be idempotent and
    // MUST be thread-safe.
    if (!cancelled) {
      cancelled = true;
      if (draining.getAndIncrement() == 0) {
        // If nobody is draining, clear now;
        // otherwise, the draining thread will notice
        // that the cancelled flag was set
        // and will clear for us.
        clear();
      }
    }
  }

  /**
   * May run on a driver IO thread when invoked from {@link #fetchNextPage(Page)}, or on a
   * subscriber thread, when invoked from {@link #request(long)}.
   *
   * <p>The {@link #draining} field guarantees serialized access to it, without locking.
   */
  private void drain() {
    // As per 3.4: this method SHOULD respect the responsiveness
    // of its caller by returning in a timely manner.
    // We accomplish this by a wait-free implementation.
    if (draining.getAndIncrement() != 0) {
      // Someone else is already draining, so do nothing,
      // the other thread will notice that we attempted to drain.
      // This also allows to abide by rule 3.3 and avoid
      // cycles such as request() -> onNext() -> request() etc.
      return;
    }
    int missed = 1;
    // Note: when termination is detected inside this loop,
    // we MUST call clear() manually.
    for (; ; ) {
      // The requested number of items at this point
      long r = requested.get();
      // The number of items emitted thus far
      long emitted = 0L;
      while (emitted != r) {
        if (cancelled) {
          clear();
          return;
        }
        R result = tryNext();
        if (result == null) {
          break;
        }
        if (result.isSuccess() || !failFast) {
          doOnNext(result);
        }
        if (isExhausted()) {
          stop(result.getError());
          clear();
          return;
        }
        emitted++;
      }
      if (isExhausted()) {
        stop(Optional.empty());
        clear();
        return;
      }
      if (cancelled) {
        clear();
        return;
      }
      if (emitted != 0) {
        // if any item was emitted, adjust the requested field
        Operators.subCap(requested, emitted);
      }
      // if another thread tried to call drain() while we were busy,
      // then we should do another drain round.
      missed = draining.addAndGet(-missed);
      if (missed == 0) {
        break;
      }
    }
  }

  /**
   * Tries to return the next item, if one is readily available, and returns {@code null} otherwise.
   *
   * <p>Cannot run concurrently due to the {@link #draining} field.
   */
  private R tryNext() {
    Page current = pages.peek();
    if (current != null) {
      if (current.hasNext()) {
        return current.next();
      } else if (current.hasMorePages()) {
        // Discard current page as it is consumed.
        // Don't discard the last page though as we need it
        // to test isExhausted(). It will be GC'ed when a terminal signal
        // is issued anyway, so that's no big deal.
        current = dequeue();
        // if the next page is readily available,
        // serve its first row now, no need to wait
        // for the next drain.
        if (current != null && current.hasNext()) {
          return current.next();
        }
      }
    }
    // No items available right now.
    return null;
  }

  /**
   * Returns {@code true} when the entire stream has been consumed and no more items can be emitted.
   * When that is the case, a terminal signal is sent.
   *
   * <p>Cannot run concurrently due to the draining field.
   */
  private boolean isExhausted() {
    if (cancelled) {
      return true;
    }
    Page current = pages.peek();
    // Note: current page can only be null when:
    // 1) we are waiting for the first page and it hasn't arrived yet;
    // 2) we just finished the current page, but the next page hasn't arrived yet.
    // In any case, a null here means it is not the last page.
    return current != null && !current.hasNext() && !current.hasMorePages();
  }

  /**
   * Runs on a subscriber thread initially, see {@link #start(Callable)}. Subsequent executions run
   * on the thread that completes the pair of futures [fetchNext, notFull] and enqueues. This can be
   * a driver IO thread or a subscriber thread; in both cases, cannot run concurrently due to the
   * fact that one can only fetch the next page when the current one is arrived and enqueued.
   */
  private void fetchNextPage(Page current) {
    // A local execution context to record metrics for this specific request-response cycle.
    DefaultExecutionContext local = new DefaultExecutionContext();
    rateLimiter.ifPresent(limiter -> limiter.acquire(batchSize));
    requestPermits.ifPresent(permits -> permits.acquireUninterruptibly(1));
    local.start();
    onRequestStarted(local);
    current
        .fetchNext()
        // as soon as the response arrives, notify our listener and
        // update requestPermits.
        .whenComplete(
            (rs, t) -> {
              requestPermits.ifPresent(permits -> permits.release(1));
              local.stop();
              if (t == null) {
                onRequestSuccessful(rs, local);
              } else {
                onRequestFailed(t, local);
              }
            })
        // create the new page
        .handle(
            (rs, t) -> {
              Page page;
              if (t == null) {
                page = toPage(rs, local);
              } else {
                // Unwrap CompletionExceptions created by combined futures
                if (t instanceof CompletionException) {
                  t = t.getCause();
                }
                page = toErrorPage(t);
              }
              return page;
            })
        // wait until there is free space in the queue
        // before enqueueing the new page
        .thenCombine(current.fullyConsumed, (rs, v) -> rs)
        // enqueue the new page
        .thenAccept(
            page -> {
              enqueue(page);
              if (page.hasMorePages() && !cancelled) {
                // preemptively fetch the next page, if available
                fetchNextPage(page);
              }
              drain();
            });
  }

  /*
  The 3 methods below should trigger notifications to our listener,
  using the "local" execution context that records metrics for a single
  request-response round-trip.
  Since the notification type depends on the result type,
  this must be done by subclasses.
  */

  abstract void onRequestStarted(ExecutionContext local);

  abstract void onRequestSuccessful(P result, ExecutionContext local);

  abstract void onRequestFailed(Throwable t, ExecutionContext local);

  /*
  Note: two executions of enqueue() or dequeue()
  cannot happen concurrently, but one execution
  of enqueue() can happen concurrently with an
  execution of dequeue().
  (This, btw, is the reason why it is possible to use an SPSC queue
  to store pages.)
  We don't need to synchronize access to the 3
  connected resources – the page queue, the queue size, and
  each page's fullyConsumed future –  as long as they are modified in proper order.
  */

  private void enqueue(Page page) {
    if (!pages.offer(page)) {
      throw new AssertionError("Queue is full, this should not happen");
    }
    last = page;
    // if there is room for another page, complete the future now,
    // this will allow the enqueueing of the next one.
    if (pagesSize.incrementAndGet() < MAX_ENQUEUED_PAGES) {
      page.fullyConsumed.complete(null);
    }
  }

  private Page dequeue() {
    Page current = pages.poll();
    if (current == null) {
      throw new AssertionError("Queue is empty, this should not happen");
    }
    pagesSize.decrementAndGet();
    // complete the future as the last action, as its
    // completion might trigger a call to enqueue() with the next page
    last.fullyConsumed.complete(null);
    return pages.peek();
  }

  private void doOnNext(R result) {
    try {
      subscriber.onNext(result);
    } catch (Throwable t) {
      LOG.error(
          subscriber
              + " violated the Reactive Streams rule 2.13 by throwing an exception from onNext.",
          t);
      cancel();
    }
  }

  private void stop(Optional<BulkExecutionException> error) {
    global.stop();
    if (error.isPresent()) {
      listener.ifPresent(l -> l.onExecutionFailed(error.get(), global));
    } else {
      listener.ifPresent(l -> l.onExecutionSuccessful(statement, global));
    }
    if (!failFast || !error.isPresent()) {
      doOnComplete();
    } else {
      doOnError(error.get());
    }
  }

  private void doOnComplete() {
    try {
      // Then we signal onComplete as per rules 1.2 and 1.5
      subscriber.onComplete();
    } catch (Throwable t) {
      LOG.error(
          subscriber
              + " violated the Reactive Streams rule 2.13 by throwing an exception from onComplete.",
          t);
    }
    // We need to consider this Subscription as cancelled as per rule 1.6
    cancel();
  }

  // Public because it can be invoked by the publisher if the subscription handshake process fails.
  public void doOnError(Throwable error) {
    try {
      // Then we signal the error downstream, as per rules 1.2 and 1.4.
      subscriber.onError(error);
    } catch (Throwable t) {
      t.addSuppressed(error);
      LOG.error(
          subscriber
              + " violated the Reactive Streams rule 2.13 by throwing an exception from onError.",
          t);
    }
    // We need to consider this Subscription as cancelled as per rule 1.6
    cancel();
  }

  private void clear() {
    // We don't need these pages anymore and should not hold references
    // to them.
    pages.clear();
    // As per 3.13, Subscription.cancel() MUST request the Publisher to
    // eventually drop any references to the corresponding subscriber.
    // Our own publishers do not keep references to this subscription,
    // but downstream processors might do so, which is why we need to
    // defensively clear the subscriber reference when we are done.
    subscriber = null;
  }

  /**
   * Converts the received result object into a {@link Page}.
   *
   * @param rs the results to convert.
   * @param local the local execution context (used to record metrics for rows contained in the
   *     result).
   * @return a new page.
   */
  abstract Page toPage(P rs, ExecutionContext local);

  /** Converts the given error into a {@link Page}, containing the error as its only element. */
  Page toErrorPage(Throwable t) {
    Page page;
    BulkExecutionException error = new BulkExecutionException(t, statement);
    page = new Page(Collections.singleton(toErrorResult(error)).iterator(), null);
    return page;
  }

  /**
   * Creates a result from the given error.
   *
   * @param error The error to convert.
   * @return the error result.
   */
  abstract R toErrorResult(BulkExecutionException error);

  /**
   * Abstracts away the concrete page type, allowing this base class to handle different ones
   * (typically continuous and non-continuous result sets).
   *
   * <p>It contains simply an iterator over the page's results, and a future pointing to the next
   * page, or {@code null} if it's the last page.
   */
  class Page {

    final Iterator<R> iterator;
    final Callable<ListenableFuture<P>> next;
    final CompletableFuture<Void> fullyConsumed;

    /** called only from start() */
    private Page(Callable<ListenableFuture<P>> next) {
      this.next = next;
      this.iterator = Collections.emptyIterator();
      fullyConsumed = initial;
    }

    Page(Iterator<R> iterator, Callable<ListenableFuture<P>> next) {
      this.next = next;
      this.iterator = iterator;
      fullyConsumed = new CompletableFuture<>();
    }

    boolean hasMorePages() {
      return next != null;
    }

    CompletionStage<P> fetchNext() {
      try {
        return toCompletableFuture(next.call());
      } catch (Exception e) {
        // This is a synchronous failure in the driver.
        // We treat it as a failed future.
        CompletableFuture<P> failed = new CompletableFuture<>();
        failed.completeExceptionally(e);
        return failed;
      }
    }

    R next() {
      return iterator.next();
    }

    boolean hasNext() {
      return iterator.hasNext();
    }
  }

  private static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> guavaFuture) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    Futures.addCallback(
        guavaFuture,
        new FutureCallback<T>() {
          @Override
          public void onFailure(Throwable throwable) {
            completableFuture.completeExceptionally(throwable);
          }

          @Override
          public void onSuccess(T t) {
            completableFuture.complete(t);
          }
        },
        MoreExecutors.directExecutor());
    return completableFuture;
  }
}
