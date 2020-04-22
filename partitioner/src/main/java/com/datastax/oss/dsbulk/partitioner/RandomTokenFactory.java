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
