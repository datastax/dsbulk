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

/** A {@link TokenFactory} for the Random Partitioner. */
public class RandomTokenFactory implements TokenFactory<BigInteger, Token<BigInteger>> {

  public static final RandomTokenFactory INSTANCE = new RandomTokenFactory();

  private static final Token<BigInteger> MIN_TOKEN = new RandomToken(BigInteger.valueOf(-1));

  static final BigInteger MAX_TOKEN_VALUE = BigInteger.valueOf(2).pow(127);

  private static final Token<BigInteger> MAX_TOKEN = new RandomToken(MAX_TOKEN_VALUE);

  private static final BigInteger TOTAL_TOKEN_COUNT = MAX_TOKEN.value().subtract(MIN_TOKEN.value());

  private RandomTokenFactory() {}

  @NonNull
  @Override
  public Token<BigInteger> minToken() {
    return MIN_TOKEN;
  }

  @NonNull
  @Override
  public Token<BigInteger> maxToken() {
    return MAX_TOKEN;
  }

  @NonNull
  @Override
  public BigInteger totalTokenCount() {
    return TOTAL_TOKEN_COUNT;
  }

  @NonNull
  @Override
  public BigInteger distance(@NonNull Token<BigInteger> token1, @NonNull Token<BigInteger> token2) {
    BigInteger left = token1.value();
    BigInteger right = token2.value();
    if (right.compareTo(left) > 0) {
      return right.subtract(left);
    } else {
      return right.subtract(left).add(totalTokenCount());
    }
  }

  @NonNull
  @Override
  public TokenRangeSplitter<BigInteger, Token<BigInteger>> splitter() {
    return RandomTokenRangeSplitter.INSTANCE;
  }

  @NonNull
  @Override
  public RandomToken tokenFromString(@NonNull String string) {
    return new RandomToken(new BigInteger(string));
  }
}
