/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.reactive;

import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

public final class SimpleBackpressureController {

  private final Sync sync = new Sync();

  public void signalRequested(long requests) {
    if (requests < 1) {
      throw new IllegalArgumentException("The number of requests must be positive");
    }
    sync.releaseShared(requests);
  }

  public void awaitRequested(long requests) {
    if (requests < 1) {
      throw new IllegalArgumentException("The number of requests must be positive");
    }
    sync.acquireShared(requests);
  }

  private static final class Sync extends AbstractQueuedLongSynchronizer {

    @Override
    protected boolean tryReleaseShared(long permits) {
      for (; ; ) {
        long current = getState();
        long update = current + permits;
        if (update < 0L) {
          // overflow
          update = Long.MAX_VALUE;
        }
        if (compareAndSetState(current, update)) {
          return update > 0;
        }
      }
    }

    @Override
    protected long tryAcquireShared(long permits) {
      for (; ; ) {
        long current = getState();
        long update = current - permits;
        if (update < 0L) {
          return update;
        }
        if (compareAndSetState(current, update)) {
          return update;
        }
      }
    }
  }
}
