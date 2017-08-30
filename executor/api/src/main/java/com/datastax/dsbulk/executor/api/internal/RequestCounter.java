/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public final class RequestCounter {

  private static final long UNLIMITED = Long.MAX_VALUE;

  private final Sync sync = new Sync();

  public void signalRequested(long n) {
    sync.tryReleaseShared(n);
  }

  public void awaitRequested() {
    sync.acquireShared(0);
  }

  private static final class Sync extends AbstractQueuedSynchronizer {

    private final AtomicLong count = new AtomicLong(0);

    private void tryReleaseShared(long permits) {
      long next =
          count.accumulateAndGet(
              permits,
              (current, delta) ->
                  current == UNLIMITED || delta == UNLIMITED ? UNLIMITED : current + delta);
      if (next != 0) releaseShared(0);
    }

    @Override
    protected boolean tryReleaseShared(int ignored) {
      return true;
    }

    @Override
    protected int tryAcquireShared(int ignored) {
      for (; ; ) {
        long current = count.get();
        if (current == UNLIMITED) return 1;
        if (current == 0) return -1;
        if (count.compareAndSet(current, current - 1)) return 1;
      }
    }
  }
}
