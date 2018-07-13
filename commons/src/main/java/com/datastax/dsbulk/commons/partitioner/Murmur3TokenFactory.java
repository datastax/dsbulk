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

/** A {@link TokenFactory} for the Murmur3 Partitioner. */
public class Murmur3TokenFactory implements TokenFactory<Long, Token<Long>> {

  public static final Murmur3TokenFactory INSTANCE = new Murmur3TokenFactory();

  private static final Token<Long> MIN_TOKEN = new Murmur3Token(Long.MIN_VALUE);

  private static final Token<Long> MAX_TOKEN = new Murmur3Token(Long.MAX_VALUE);

  private static final BigInteger TOTAL_TOKEN_COUNT =
      BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.valueOf(Long.MIN_VALUE));

  private Murmur3TokenFactory() {}

  @NotNull
  @Override
  public Token<Long> minToken() {
    return MIN_TOKEN;
  }

  @NotNull
  @Override
  public Token<Long> maxToken() {
    return MAX_TOKEN;
  }

  @NotNull
  @Override
  public BigInteger totalTokenCount() {
    return TOTAL_TOKEN_COUNT;
  }

  @NotNull
  @Override
  public BigInteger distance(@NotNull Token<Long> token1, @NotNull Token<Long> token2) {
    BigInteger left = BigInteger.valueOf(token1.value());
    BigInteger right = BigInteger.valueOf(token2.value());
    if (right.compareTo(left) > 0) {
      return right.subtract(left);
    } else {
      return right.subtract(left).add(totalTokenCount());
    }
  }

  @NotNull
  @Override
  public TokenRangeSplitter<Long, Token<Long>> splitter() {
    return Murmur3TokenRangeSplitter.INSTANCE;
  }

  @NotNull
  @Override
  public Token<Long> tokenFromString(@NotNull String string) {
    return new Murmur3Token(Long.parseLong(string));
  }
}
