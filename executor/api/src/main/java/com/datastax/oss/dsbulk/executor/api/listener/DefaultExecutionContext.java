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
package com.datastax.oss.dsbulk.executor.api.listener;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.jctools.maps.NonBlockingHashMap;

public class DefaultExecutionContext implements ExecutionContext {

  private volatile ConcurrentMap<Object, Object> attributes = null;

  private volatile long start = -1;
  private volatile long end = -1;

  @Override
  public void setAttribute(Object key, Object value) {
    getAttributes().put(key, value);
  }

  @Override
  public Optional<Object> getAttribute(Object key) {
    return Optional.ofNullable(getAttributes().get(key));
  }

  @Override
  public long elapsedTimeNanos() {
    return start == -1 || end == -1 ? -1 : end - start;
  }

  public void start() {
    this.start = System.nanoTime();
  }

  public void stop() {
    this.end = System.nanoTime();
  }

  private ConcurrentMap<Object, Object> getAttributes() {
    // Double check locking
    if (attributes == null) {
      synchronized (this) {
        if (attributes == null) {
          @SuppressWarnings("UnnecessaryLocalVariable")
          ConcurrentMap<Object, Object> attributes = new NonBlockingHashMap<>();
          this.attributes = attributes;
        }
      }
    }
    return attributes;
  }
}
