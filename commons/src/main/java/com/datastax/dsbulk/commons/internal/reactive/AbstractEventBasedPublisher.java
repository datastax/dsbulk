/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.reactive;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractEventBasedPublisher<T> implements Publisher<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEventBasedPublisher.class);

  private final AtomicReference<State> state = new AtomicReference<>(State.UNSUBSCRIBED);

  @SuppressWarnings("unused")
  private volatile long demand;

  @SuppressWarnings("rawtypes")
  private static final AtomicLongFieldUpdater<AbstractEventBasedPublisher> DEMAND_FIELD_UPDATER =
      AtomicLongFieldUpdater.newUpdater(AbstractEventBasedPublisher.class, "demand");

  @Nullable private Subscriber<? super T> subscriber;

  // Publisher implementation...

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(this.state + " subscribe: " + subscriber);
    }
    this.state.get().subscribe(this, subscriber);
  }

  // Listener delegation methods...

  /** Listeners can call this to notify when reading is possible. */
  public final void onDataAvailable() {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(this.state + " onDataAvailable");
    }
    this.state.get().onDataAvailable(this);
  }

  /** Listeners can call this to notify when all data has been read. */
  public void onAllDataRead() {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(this.state + " onAllDataRead");
    }
    this.state.get().onAllDataRead(this);
  }

  /** Listeners can call this to notify when a read error has occurred. */
  public final void onError(Throwable t) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(this.state + " onError: " + t);
    }
    this.state.get().onError(this, t);
  }

  protected abstract void checkOnDataAvailable();

  /**
   * Reads a data from the input, if possible.
   *
   * @return the data that was read; or {@code null}
   */
  @Nullable
  protected abstract T read() throws IOException;

  /**
   * Read and publish data from the input. Continue till there is no more demand or there is no more
   * data to be read.
   *
   * @return {@code true} if there is more demand; {@code false} otherwise
   */
  private boolean readAndPublish() throws IOException {
    long r;
    while ((r = demand) > 0) {
      T data = read();
      if (data != null) {
        if (r != Long.MAX_VALUE) {
          DEMAND_FIELD_UPDATER.addAndGet(this, -1L);
        }
        Objects.requireNonNull(subscriber, "No subscriber");
        this.subscriber.onNext(data);
      } else {
        return true;
      }
    }
    return false;
  }

  private boolean changeState(State oldState, State newState) {
    return this.state.compareAndSet(oldState, newState);
  }

  private static final class ReadSubscription implements Subscription {

    private final AbstractEventBasedPublisher<?> publisher;

    public ReadSubscription(AbstractEventBasedPublisher<?> publisher) {
      this.publisher = publisher;
    }

    @Override
    public final void request(long n) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(state() + " request: " + n);
      }
      state().request(this.publisher, n);
    }

    @Override
    public final void cancel() {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(state() + " cancel");
      }
      state().cancel(this.publisher);
    }

    private State state() {
      return this.publisher.state.get();
    }
  }

  /**
   * Represents a state for the {@link Publisher} to be in. The following figure indicate the four
   * different states that exist, and the relationships between them.
   *
   * <pre>
   *       UNSUBSCRIBED
   *        |
   *        v
   * NO_DEMAND -------------------> DEMAND
   *    |    ^                      ^    |
   *    |    |                      |    |
   *    |    --------- READING <-----    |
   *    |                 |              |
   *    |                 v              |
   *    ------------> COMPLETED <---------
   * </pre>
   *
   * Refer to the individual states for more information.
   */
  private enum State {

    /**
     * The initial unsubscribed state. Will respond to {@link
     * #subscribe(AbstractEventBasedPublisher, Subscriber)} by changing state to {@link #NO_DEMAND}.
     */
    UNSUBSCRIBED {
      @Override
      <T> void subscribe(
          AbstractEventBasedPublisher<T> publisher, Subscriber<? super T> subscriber) {
        Objects.requireNonNull(publisher, "Publisher must not be null");
        Objects.requireNonNull(subscriber, "Subscriber must not be null");
        if (publisher.changeState(this, NO_DEMAND)) {
          Subscription subscription = new ReadSubscription(publisher);
          publisher.subscriber = subscriber;
          subscriber.onSubscribe(subscription);
        } else {
          throw new IllegalStateException(toString());
        }
      }
    },

    /**
     * State that gets entered when there is no demand. Responds to {@link
     * #request(AbstractEventBasedPublisher, long)} by increasing the demand, changing state to
     * {@link #DEMAND} and will check whether there is data available for reading.
     */
    NO_DEMAND {
      @Override
      <T> void request(AbstractEventBasedPublisher<T> publisher, long n) {
        if (Operators.validate(n)) {
          Operators.addCap(DEMAND_FIELD_UPDATER, publisher, n);
          if (publisher.changeState(this, DEMAND)) {
            publisher.checkOnDataAvailable();
          }
        }
      }
    },

    /**
     * State that gets entered when there is demand. Responds to {@link
     * #onDataAvailable(AbstractEventBasedPublisher)} by reading the available data. The state will
     * be changed to {@link #NO_DEMAND} if there is no demand.
     */
    DEMAND {
      @Override
      <T> void request(AbstractEventBasedPublisher<T> publisher, long n) {
        if (Operators.validate(n)) {
          Operators.addCap(DEMAND_FIELD_UPDATER, publisher, n);
        }
      }

      @Override
      <T> void onDataAvailable(AbstractEventBasedPublisher<T> publisher) {
        if (publisher.changeState(this, READING)) {
          try {
            boolean demandAvailable = publisher.readAndPublish();
            if (demandAvailable) {
              publisher.changeState(READING, DEMAND);
              publisher.checkOnDataAvailable();
            } else {
              publisher.changeState(READING, NO_DEMAND);
            }
          } catch (IOException ex) {
            publisher.onError(ex);
          }
        }
      }
    },

    READING {
      @Override
      <T> void request(AbstractEventBasedPublisher<T> publisher, long n) {
        if (Operators.validate(n)) {
          Operators.addCap(DEMAND_FIELD_UPDATER, publisher, n);
        }
      }
    },

    /** The terminal completed state. Does not respond to any events. */
    COMPLETED {
      @Override
      <T> void request(AbstractEventBasedPublisher<T> publisher, long n) {
        // ignore
      }

      @Override
      <T> void cancel(AbstractEventBasedPublisher<T> publisher) {
        // ignore
      }

      @Override
      <T> void onAllDataRead(AbstractEventBasedPublisher<T> publisher) {
        // ignore
      }

      @Override
      <T> void onError(AbstractEventBasedPublisher<T> publisher, Throwable t) {
        // ignore
      }
    };

    <T> void subscribe(AbstractEventBasedPublisher<T> publisher, Subscriber<? super T> subscriber) {
      throw new IllegalStateException(toString());
    }

    <T> void request(AbstractEventBasedPublisher<T> publisher, long n) {
      throw new IllegalStateException(toString());
    }

    <T> void cancel(AbstractEventBasedPublisher<T> publisher) {
      publisher.changeState(this, COMPLETED);
    }

    <T> void onDataAvailable(AbstractEventBasedPublisher<T> publisher) {
      // ignore
    }

    <T> void onAllDataRead(AbstractEventBasedPublisher<T> publisher) {
      if (publisher.changeState(this, COMPLETED)) {
        if (publisher.subscriber != null) {
          publisher.subscriber.onComplete();
        }
      }
    }

    <T> void onError(AbstractEventBasedPublisher<T> publisher, Throwable t) {
      if (publisher.changeState(this, COMPLETED)) {
        if (publisher.subscriber != null) {
          publisher.subscriber.onError(t);
        }
      }
    }
  }
}
