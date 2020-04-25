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
