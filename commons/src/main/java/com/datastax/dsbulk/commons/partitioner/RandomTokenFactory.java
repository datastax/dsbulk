/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import java.math.BigInteger;
import org.jetbrains.annotations.NotNull;

/** A {@link TokenFactory} for the Random Partitioner. */
public class RandomTokenFactory implements TokenFactory<BigInteger, Token<BigInteger>> {

  public static final RandomTokenFactory INSTANCE = new RandomTokenFactory();

  private static final Token<BigInteger> MIN_TOKEN = new RandomToken(BigInteger.valueOf(-1));

  static final BigInteger MAX_TOKEN_VALUE = BigInteger.valueOf(2).pow(127);

  private static final Token<BigInteger> MAX_TOKEN = new RandomToken(MAX_TOKEN_VALUE);

  private static final BigInteger TOTAL_TOKEN_COUNT = MAX_TOKEN.value().subtract(MIN_TOKEN.value());

  private RandomTokenFactory() {}

  @NotNull
  @Override
  public Token<BigInteger> minToken() {
    return MIN_TOKEN;
  }

  @NotNull
  @Override
  public Token<BigInteger> maxToken() {
    return MAX_TOKEN;
  }

  @NotNull
  @Override
  public BigInteger totalTokenCount() {
    return TOTAL_TOKEN_COUNT;
  }

  @NotNull
  @Override
  public BigInteger distance(@NotNull Token<BigInteger> token1, @NotNull Token<BigInteger> token2) {
    BigInteger left = token1.value();
    BigInteger right = token2.value();
    if (right.compareTo(left) > 0) {
      return right.subtract(left);
    } else {
      return right.subtract(left).add(totalTokenCount());
    }
  }

  @NotNull
  @Override
  public TokenRangeSplitter<BigInteger, Token<BigInteger>> splitter() {
    return RandomTokenRangeSplitter.INSTANCE;
  }

  @NotNull
  @Override
  public RandomToken tokenFromString(@NotNull String string) {
    return new RandomToken(new BigInteger(string));
  }
}
