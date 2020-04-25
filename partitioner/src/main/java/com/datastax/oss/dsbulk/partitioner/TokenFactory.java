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

/**
 * A factory for {@link Token} instances.
 *
 * @param <V> The token value type.
 * @param <T> The token type.
 */
public interface TokenFactory<V extends Number, T extends Token<V>> {

  /** Creates a new instance for the given driver token factory. */
  static TokenFactory<?, ?> forDriverTokenFactory(
      com.datastax.oss.driver.internal.core.metadata.token.TokenFactory driverTokenFactory) {
    if (driverTokenFactory
        instanceof com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenFactory) {
      return Murmur3TokenFactory.INSTANCE;
    } else if (driverTokenFactory
        instanceof com.datastax.oss.driver.internal.core.metadata.token.RandomTokenFactory) {
      return RandomTokenFactory.INSTANCE;
    } else {
      throw new IllegalArgumentException("Unknown partitioner: " + driverTokenFactory);
    }
  }

  /** @return The minimum token for this factory. */
  @NonNull
  T minToken();

  /** @return The maximum token for this factory. */
  @NonNull
  T maxToken();

  /** @return Total token count in a ring. */
  @NonNull
  BigInteger totalTokenCount();

  /**
   * Returns the distance between {@code token1} and {@code token2}, that is, the number of tokens
   * in a range from {@code token1} to {@code token2}. If {@code token2 &lt; token1}, then the range
   * wraps around.
   */
  @NonNull
  BigInteger distance(@NonNull T token1, @NonNull T token2);

  /**
   * Returns the fraction of the ring in a range from {@code token1} to {@code token2}. If {@code
   * token2 * &lt; token1}, then the range wraps around. Returns 1.0 for a full ring range, 0.0 for
   * an empty range.
   */
  default double fraction(T token1, T token2) {
    return distance(token1, token2).doubleValue() / totalTokenCount().doubleValue();
  }

  /** Creates a token from its string representation */
  @NonNull
  T tokenFromString(@NonNull String string);

  /** Returns a {@link TokenRangeSplitter} for the type of tokens managed by this token factory. */
  @NonNull
  TokenRangeSplitter<V, T> splitter();
}
