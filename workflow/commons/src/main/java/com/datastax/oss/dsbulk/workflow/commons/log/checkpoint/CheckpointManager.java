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
package com.datastax.oss.dsbulk.workflow.commons.log.checkpoint;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.jcip.annotations.NotThreadSafe;

/**
 * A checkpoint manager that stores checkpoints for various resources being processed.
 *
 * <p>A checkpoint manager is a map of resource URIs to checkpoints. It is typically built from a
 * checkpoint file parsed with {@link #parse(BufferedReader)}.
 *
 * <p>A {@link CheckpointManager} instance is thread-safe only if the underlying map is thread-safe.
 */
@NotThreadSafe
public class CheckpointManager {

  /**
   * Parses a checkpoint file in CSV format and builds a (thread-safe) checkpoint manager containing
   * the checkpoints for all resources listed in the file.
   */
  @NonNull
  public static CheckpointManager parse(@NonNull BufferedReader reader) throws IOException {
    try (Stream<String> lines = reader.lines()) {
      Map<URI, Checkpoint> checkpoints =
          lines
              .map(
                  line -> {
                    int i = line.indexOf(';');
                    URI resource = URI.create(line.substring(0, i));
                    Checkpoint cp = Checkpoint.parse(line.substring(i + 1));
                    return new SimpleEntry<>(resource, cp);
                  })
              .collect(
                  Collectors.toMap(
                      Entry::getKey,
                      Entry::getValue,
                      (a, b) -> {
                        a.merge(b);
                        return a;
                      },
                      // use a concurrent map since this instance will be used in LogManager by
                      // multiple threads.
                      ConcurrentHashMap::new));
      return new CheckpointManager(checkpoints);
    }
  }

  @VisibleForTesting final Map<URI, Checkpoint> checkpoints;

  /** Creates a new, empty, non-thread-safe checkpoint manager. */
  public CheckpointManager() {
    this(new HashMap<>());
  }

  public CheckpointManager(@NonNull Map<URI, Checkpoint> checkpoints) {
    this.checkpoints = Objects.requireNonNull(checkpoints);
  }

  @NonNull
  public Checkpoint getCheckpoint(@NonNull URI resource) {
    return checkpoints.computeIfAbsent(resource, uri -> new Checkpoint());
  }

  public boolean isEmpty() {
    return checkpoints.isEmpty();
  }

  public boolean isComplete(@NonNull ReplayStrategy checkpointReplayStrategy) {
    return checkpoints.values().stream().allMatch(checkpointReplayStrategy::isComplete);
  }

  public void update(@NonNull URI resource, long position, boolean success) {
    if (position > 0) {
      checkpoints.compute(
          resource,
          (res, checkpoint) -> {
            if (checkpoint == null) {
              checkpoint = new Checkpoint();
            }
            checkpoint.updateConsumed(position, success);
            return checkpoint;
          });
    }
  }

  /**
   * Returns the number of items already processed and that won't be processed again. This total may
   * include successful, but also rejected records, if they won't be replayed according to the
   * replay strategy.
   */
  public long getTotalItems(@NonNull ReplayStrategy replayStrategy) {
    return checkpoints.values().stream().mapToLong(replayStrategy::getTotalItems).sum();
  }

  /**
   * Returns the number of rejected items already processed and that won't be processed again
   * according to the replay strategy.
   */
  public long getTotalErrors(@NonNull ReplayStrategy replayStrategy) {
    return checkpoints.values().stream().mapToLong(replayStrategy::getTotalErrors).sum();
  }

  public void merge(@NonNull CheckpointManager other) {
    for (URI resource : other.checkpoints.keySet()) {
      Checkpoint otherCheckpoint = other.checkpoints.get(resource);
      if (otherCheckpoint != null) {
        checkpoints.merge(
            resource,
            otherCheckpoint,
            (cur, next) -> {
              cur.merge(next);
              return cur;
            });
      }
    }
  }

  public void printCsv(@NonNull PrintWriter writer) {
    for (Entry<URI, Checkpoint> entry : checkpoints.entrySet()) {
      writer.print(entry.getKey());
      writer.print(';');
      writer.println(entry.getValue().asCsv());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CheckpointManager)) {
      return false;
    }
    CheckpointManager that = (CheckpointManager) o;
    return checkpoints.equals(that.checkpoints);
  }

  @Override
  public int hashCode() {
    return checkpoints.hashCode();
  }
}
