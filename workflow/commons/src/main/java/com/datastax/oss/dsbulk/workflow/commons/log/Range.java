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
package com.datastax.oss.dsbulk.workflow.commons.log;

import java.util.Objects;

/**
 * A closed range, that is, an interval defined by two inclusive bounds.
 *
 * <p>This class is a simplified, mutable version of Guava's {@link
 * com.datastax.oss.driver.shaded.guava.common.collect.Range Range} designed to reduce the
 * allocation rate of Range instances in the {@link PositionsTracker}.
 */
public final class Range {

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

  public boolean contains(long value) {
    return value >= lower && value <= upper;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Range that = (Range) o;
    return lower == that.lower && upper == that.upper;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lower, upper);
  }

  @Override
  public String toString() {
    return "[" + lower + ',' + upper + ']';
  }
}
