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

/** A {@link TokenFactory} for the Murmur3 Partitioner. */
public class Murmur3TokenFactory implements TokenFactory<Long, Token<Long>> {

  public static final Murmur3TokenFactory INSTANCE = new Murmur3TokenFactory();

  private static final Token<Long> MIN_TOKEN = new Murmur3Token(Long.MIN_VALUE);

  private static final Token<Long> MAX_TOKEN = new Murmur3Token(Long.MAX_VALUE);

  private static final BigInteger TOTAL_TOKEN_COUNT =
      BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.valueOf(Long.MIN_VALUE));

  private Murmur3TokenFactory() {}

  @NonNull
  @Override
  public Token<Long> minToken() {
    return MIN_TOKEN;
  }

  @NonNull
  @Override
  public Token<Long> maxToken() {
    return MAX_TOKEN;
  }

  @NonNull
  @Override
  public BigInteger totalTokenCount() {
    return TOTAL_TOKEN_COUNT;
  }

  @NonNull
  @Override
  public BigInteger distance(@NonNull Token<Long> token1, @NonNull Token<Long> token2) {
    BigInteger left = BigInteger.valueOf(token1.value());
    BigInteger right = BigInteger.valueOf(token2.value());
    if (right.compareTo(left) > 0) {
      return right.subtract(left);
    } else {
      return right.subtract(left).add(totalTokenCount());
    }
  }

  @NonNull
  @Override
  public TokenRangeSplitter<Long, Token<Long>> splitter() {
    return Murmur3TokenRangeSplitter.INSTANCE;
  }

  @NonNull
  @Override
  public Token<Long> tokenFromString(@NonNull String string) {
    return new Murmur3Token(Long.parseLong(string));
  }
}
