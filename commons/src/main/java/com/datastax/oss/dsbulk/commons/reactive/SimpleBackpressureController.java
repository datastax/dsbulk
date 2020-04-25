/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.commons.reactive;

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
