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

import edu.umd.cs.findbugs.annotations.NonNull;

/** Defines the strategy to use when replaying a failed operation. */
public enum ReplayStrategy {
  /** Only replay new records. */
  resume {
    @Override
    public boolean isComplete(@NonNull Checkpoint checkpoint) {
      return checkpoint.isComplete()
          && checkpoint.getProduced()
              == checkpoint.getConsumedSuccessful().sum() + checkpoint.getConsumedFailed().sum();
    }

    @Override
    public void reset(@NonNull Checkpoint checkpoint) {
      checkpoint.setComplete(false);
      // In practice, produced can be greater than consumed if the operation was interrupted, but it
      // cannot be lesser.
      checkpoint.setProduced(
          checkpoint.getConsumedSuccessful().sum() + checkpoint.getConsumedFailed().sum());
    }

    @Override
    public boolean shouldReplay(@NonNull Checkpoint checkpoint, long position) {
      return !checkpoint.getConsumedSuccessful().contains(position)
          && !checkpoint.getConsumedFailed().contains(position);
    }

    @Override
    public long getTotalItems(@NonNull Checkpoint checkpoint) {
      return checkpoint.getConsumedSuccessful().sum() + checkpoint.getConsumedFailed().sum();
    }

    @Override
    public long getRejectedItems(@NonNull Checkpoint checkpoint) {
      return checkpoint.getConsumedFailed().sum();
    }
  },

  /** Replay new and failed records. */
  retry {
    @Override
    public boolean isComplete(@NonNull Checkpoint checkpoint) {
      return checkpoint.isComplete()
          && checkpoint.getProduced() == checkpoint.getConsumedSuccessful().sum()
          && checkpoint.getConsumedFailed().sum() == 0;
    }

    @Override
    public void reset(@NonNull Checkpoint checkpoint) {
      checkpoint.setComplete(false);
      checkpoint.setProduced(checkpoint.getConsumedSuccessful().sum());
      checkpoint.getConsumedFailed().clear();
    }

    @Override
    public boolean shouldReplay(@NonNull Checkpoint checkpoint, long position) {
      return !checkpoint.getConsumedSuccessful().contains(position);
    }

    @Override
    public long getTotalItems(@NonNull Checkpoint checkpoint) {
      return checkpoint.getConsumedSuccessful().sum();
    }

    @Override
    public long getRejectedItems(@NonNull Checkpoint checkpoint) {
      return 0;
    }
  },

  /** Replay new and failed records, including from completed resources. */
  retryAll {
    @Override
    public boolean isComplete(@NonNull Checkpoint checkpoint) {
      return false;
    }

    @Override
    public void reset(@NonNull Checkpoint checkpoint) {
      checkpoint.setComplete(false);
      checkpoint.setProduced(checkpoint.getConsumedSuccessful().sum());
      checkpoint.getConsumedFailed().clear();
    }

    @Override
    public boolean shouldReplay(@NonNull Checkpoint checkpoint, long position) {
      return !checkpoint.getConsumedSuccessful().contains(position);
    }

    @Override
    public long getTotalItems(@NonNull Checkpoint checkpoint) {
      return checkpoint.getConsumedSuccessful().sum();
    }

    @Override
    public long getRejectedItems(@NonNull Checkpoint checkpoint) {
      return 0;
    }
  };

  /**
   * Returns true if the resource is fully consumed. This may include rejected records if the replay
   * strategy does not replay such records.
   */
  public abstract boolean isComplete(@NonNull Checkpoint checkpoint);

  /**
   * Resets the checkpoint to its initial state. This is called when the resource is about to start
   * being replayed.
   *
   * <p>The status is set to UNFINISHED, and the produced and consumed counters are adjusted to
   * match what {@link #shouldReplay(Checkpoint, long)} will decide for each record to replay.
   */
  public abstract void reset(@NonNull Checkpoint checkpoint);

  /**
   * Returns true if the record at the given position should be replayed. This is called for each
   * record that is about to be produced.
   */
  public abstract boolean shouldReplay(@NonNull Checkpoint checkpoint, long position);

  /**
   * Returns the number of records already processed and that won't be replayed. This total may
   * include successful, but also rejected records, if they won't be replayed according to the
   * replay strategy.
   *
   * <p>This is mainly intended to update record metrics at the beginning of the operation, with
   * numbers computed from the previous checkpoint.
   */
  public abstract long getTotalItems(@NonNull Checkpoint checkpoint);

  /**
   * Returns the number of rejected records that won't be processed again according to the replay
   * strategy.
   *
   * <p>This is mainly intended to update record metrics at the beginning of the operation, with
   * numbers computed from the previous checkpoint.
   */
  public abstract long getRejectedItems(@NonNull Checkpoint checkpoint);
}
