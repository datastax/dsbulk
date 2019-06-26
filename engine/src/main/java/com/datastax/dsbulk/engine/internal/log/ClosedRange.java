package com.datastax.dsbulk.engine.internal.log;

import java.util.Objects;

public final class ClosedRange {

  private long lower;
  private long upper;

  public ClosedRange(long lower, long upper) {
    this.lower = lower;
    this.upper = upper;
  }

  public ClosedRange(long singleton) {
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
    ClosedRange that = (ClosedRange) o;
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
