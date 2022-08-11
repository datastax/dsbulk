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
import net.jcip.annotations.NotThreadSafe;

/**
 * A checkpoint holds information about a resource that can be used to resume a previous run.
 *
 * <p>Checkpoint instances are not thread-safe. They should be used by a single thread at a time,
 * then be merged together to get a global result.
 */
@NotThreadSafe
public class Checkpoint {

  @NonNull
  public static Checkpoint parse(@NonNull String line) {
    String[] tokens = line.split(";", -1);
    boolean complete = tokens[0].equals("1");
    long produced = Long.parseLong(tokens[1]);
    RangeSet consumedSuccessful = RangeSet.parse(tokens[2]);
    RangeSet consumedFailed = RangeSet.parse(tokens[3]);
    return new Checkpoint(produced, consumedSuccessful, consumedFailed, complete);
  }

  private long produced;
  private final RangeSet consumedSuccessful;
  private final RangeSet consumedFailed;
  private boolean complete;

  public Checkpoint() {
    this(0, new RangeSet(), new RangeSet(), false);
  }

  Checkpoint(
      long produced,
      @NonNull RangeSet consumedSuccessful,
      @NonNull RangeSet consumedFailed,
      @NonNull boolean complete) {
    this.produced = produced;
    this.consumedSuccessful = consumedSuccessful;
    this.consumedFailed = consumedFailed;
    this.complete = complete;
  }

  public long getProduced() {
    return produced;
  }

  public RangeSet getConsumedSuccessful() {
    return consumedSuccessful;
  }

  public RangeSet getConsumedFailed() {
    return consumedFailed;
  }

  public boolean isComplete() {
    return complete;
  }

  public void incrementProduced() {
    produced++;
  }

  void setProduced(long produced) {
    this.produced = produced;
  }

  public void updateConsumed(long position, boolean success) {
    if (success) {
      consumedSuccessful.update(position);
    } else {
      consumedFailed.update(position);
    }
  }

  public void setComplete(boolean complete) {
    this.complete = complete;
  }

  public void merge(Checkpoint other) {
    produced += other.produced;
    // Note: we don't need to care about duplicate positions appearing both in consumedSuccessful
    // and consumedFailed, since the replay strategy guarantees that we never replay a position
    // already present in any of the range sets.
    consumedSuccessful.merge(other.consumedSuccessful);
    consumedFailed.merge(other.consumedFailed);
    complete |= other.complete;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Checkpoint)) {
      return false;
    }

    Checkpoint that = (Checkpoint) o;

    if (produced != that.produced) {
      return false;
    }
    if (complete != that.complete) {
      return false;
    }
    if (!consumedSuccessful.equals(that.consumedSuccessful)) {
      return false;
    }
    return consumedFailed.equals(that.consumedFailed);
  }

  @Override
  public int hashCode() {
    int result = (int) (produced ^ (produced >>> 32));
    result = 31 * result + consumedSuccessful.hashCode();
    result = 31 * result + consumedFailed.hashCode();
    result = 31 * result + (complete ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Checkpoint{"
        + "produced="
        + produced
        + ", consumedSuccessful="
        + consumedSuccessful
        + ", consumedFailed="
        + consumedFailed
        + ", complete="
        + complete
        + '}';
  }

  @NonNull
  public String asCsv() {
    return (complete ? 1 : 0)
        + ";"
        + produced
        + ";"
        + consumedSuccessful.asText()
        + ";"
        + consumedFailed.asText();
  }
}
