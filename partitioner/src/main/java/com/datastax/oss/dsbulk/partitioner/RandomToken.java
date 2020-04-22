/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.partitioner;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.util.Objects;

final class RandomToken implements Token<BigInteger> {

  private final BigInteger value;

  RandomToken(@NonNull BigInteger value) {
    this.value = value;
  }

  @NonNull
  @Override
  public BigInteger value() {
    return value;
  }

  @Override
  public int compareTo(@NonNull Token<BigInteger> that) {
    return this.value().compareTo(that.value());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RandomToken that = (RandomToken) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
