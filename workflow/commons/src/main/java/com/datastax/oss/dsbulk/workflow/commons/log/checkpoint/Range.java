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

/**
 * A closed range, that is, an interval defined by two inclusive bounds.
 *
 * <p>This class is a simplified, mutable version of Guava's {@link
 * com.datastax.oss.driver.shaded.guava.common.collect.Range Range} designed to reduce the
 * allocation rate of Range instances by a {@link CheckpointManager}.
 */
public final class Range {

  public static Range parse(String text) {
    int i = text.indexOf(':');
    if (i == -1) {
      long pos = Long.parseLong(text);
      return new Range(pos, pos);
    } else {
      long lower = Long.parseLong(text.substring(0, i));
      long upper = Long.parseLong(text.substring(i + 1));
      return new Range(lower, upper);
    }
  }

  private long lower;
  private long upper;

  public Range(long lower, long upper) {
    this.lower = lower;
    this.upper = upper;
  }

  public Range(long singleton) {
    this(singleton, singleton);
  }

  public long getLower() {
    return lower;
  }

  public void setLower(long lower) {
    this.lower = lower;
  }

  public long getUpper() {
    return upper;
  }

  public void setUpper(long upper) {
    this.upper = upper;
  }

  public boolean isContiguous(Range other) {
    return this.lower - 1 <= other.upper && this.upper + 1 >= other.lower;
  }

  public void merge(Range other) {
    if (other.getLower() < this.getLower()) {
      lower = other.lower;
    }
    if (other.getUpper() > this.getUpper()) {
      upper = other.upper;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Range)) {
      return false;
    }
    Range range = (Range) o;
    if (lower != range.lower) {
      return false;
    }
    return upper == range.upper;
  }

  @Override
  public int hashCode() {
    int result = (int) (lower ^ (lower >>> 32));
    result = 31 * result + (int) (upper ^ (upper >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return asText();
  }

  @NonNull
  public String asText() {
    return lower == upper ? Long.toString(lower) : lower + ":" + upper;
  }
}
