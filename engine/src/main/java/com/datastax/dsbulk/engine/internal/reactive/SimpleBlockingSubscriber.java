/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.reactive;

import java.util.concurrent.CountDownLatch;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class SimpleBlockingSubscriber<T> extends CountDownLatch
    implements Subscriber<T>, Subscription {

  Throwable error;
  Subscription s;

  public SimpleBlockingSubscriber() {
    super(1);
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.s = s;
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(T t) {}

  @Override
  public void onComplete() {
    countDown();
  }

  @Override
  public void onError(Throwable t) {
    error = t;
    countDown();
  }

  @Override
  public void request(long n) {
    s.request(n);
  }

  @Override
  public void cancel() {
    Subscription s = this.s;
    if (s != null) {
      this.s = null;
      s.cancel();
    }
  }

  public void block() throws InterruptedException {
    try {
      await();
    } catch (InterruptedException e) {
      cancel();
      throw e;
    }
    Throwable e = error;
    if (e != null) {
      if (e instanceof RuntimeException) {
        throw ((RuntimeException) e);
      }
      throw new RuntimeException(e);
    }
  }
}
