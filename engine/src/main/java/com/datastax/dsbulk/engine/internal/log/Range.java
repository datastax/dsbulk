/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import java.util.Objects;

/**
 * A closed range, that is, an interval defined by two inclusive bounds.
 *
 * <p>This class is a simplified, mutable version of Guava's {@link com.google.common.collect.Range
 * Range} designed to reduce the allocation rate of Range instances in the {@link PositionsTracker}.
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
