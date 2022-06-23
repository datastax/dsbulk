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

  /** The global status of a checkpoint. The order of this enum's constants matters. */
  public enum Status {
    /**
     * The resource wasn't fully consumed, that is, there might be more records to produce than what
     * was actually produced. Such resources should typically be replayed.
     */
    UNFINISHED,
    /**
     * The resource wasn't fully consumed because of an error such as an I/O error when reading a
     * file. Such resources should typically be replayed.
     */
    FAILED,
    /**
     * The resource was fully consumed, that is, no more records are expected to be produced than
     * those that were already processed. Such resources should typically not be replayed.
     */
    FINISHED,
    ;

    public Status merge(Status that) {
      return this.compareTo(that) >= 0 ? this : that;
    }
  }

  @NonNull
  public static Checkpoint parse(@NonNull String line) {
    String[] tokens = line.split(";", -1);
    Status status = Status.values()[Integer.parseInt(tokens[0])];
    long produced = Long.parseLong(tokens[1]);
    RangeSet consumedSuccessful = RangeSet.parse(tokens[2]);
    RangeSet consumedFailed = RangeSet.parse(tokens[3]);
    return new Checkpoint(produced, consumedSuccessful, consumedFailed, status);
  }

  private long produced;
  private final RangeSet consumedSuccessful;
  private final RangeSet consumedFailed;
  private Status status;

  public Checkpoint() {
    this(0, new RangeSet(), new RangeSet(), Status.UNFINISHED);
  }

  Checkpoint(
      long produced,
      @NonNull RangeSet consumedSuccessful,
      @NonNull RangeSet consumedFailed,
      @NonNull Status status) {
    this.produced = produced;
    this.consumedSuccessful = consumedSuccessful;
    this.consumedFailed = consumedFailed;
    this.status = status;
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

  public Status getStatus() {
    return status;
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

  public void setStatus(Status status) {
    this.status = status;
  }

  public void merge(Checkpoint other) {
    produced += other.produced;
    // Note: we don't need to care about duplicate positions appearing both in consumedSuccessful
    // and consumedFailed, since the replay strategy guarantees that we never replay a position
    // already present in any of the range sets.
    consumedSuccessful.merge(other.consumedSuccessful);
    consumedFailed.merge(other.consumedFailed);
    status = status.merge(other.status);
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
    if (!consumedSuccessful.equals(that.consumedSuccessful)) {
      return false;
    }
    if (!consumedFailed.equals(that.consumedFailed)) {
      return false;
    }
    return status == that.status;
  }

  @Override
  public int hashCode() {
    int result = (int) (produced ^ (produced >>> 32));
    result = 31 * result + consumedSuccessful.hashCode();
    result = 31 * result + consumedFailed.hashCode();
    result = 31 * result + status.hashCode();
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
        + ", status="
        + status
        + '}';
  }

  @NonNull
  public String asCsv() {
    return status.ordinal()
        + ";"
        + produced
        + ";"
        + consumedSuccessful.asText()
        + ";"
        + consumedFailed.asText();
  }
}
