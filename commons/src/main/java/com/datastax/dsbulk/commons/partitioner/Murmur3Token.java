/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

class Murmur3Token implements Token<Long> {

  private final long value;

  Murmur3Token(long value) {
    this.value = value;
  }

  @Override
  @NotNull
  public Long value() {
    return value;
  }

  @Override
  public int compareTo(@NotNull Token<Long> that) {
    return Long.compare(this.value(), that.value());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Murmur3Token that = (Murmur3Token) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }
}
