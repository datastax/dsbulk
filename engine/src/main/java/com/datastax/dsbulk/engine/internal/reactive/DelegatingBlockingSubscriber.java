/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.reactive;

import java.util.Objects;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class DelegatingBlockingSubscriber<T> extends SimpleBlockingSubscriber<T> {

  private final Subscriber<T> subscriber;

  public DelegatingBlockingSubscriber(Subscriber<T> subscriber) {
    this.subscriber = Objects.requireNonNull(subscriber, "subscriber must not be null");
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.s = s;
    subscriber.onSubscribe(s);
  }

  @Override
  public void onNext(T in) {
    subscriber.onNext(in);
  }

  @Override
  public void onComplete() {
    subscriber.onComplete();
    countDown();
  }

  @Override
  public void onError(Throwable t) {
    subscriber.onError(t);
    error = t;
    countDown();
  }
}
