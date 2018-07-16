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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class RandomTokenRangeSplitter implements TokenRangeSplitter<BigInteger, Token<BigInteger>> {

  public static final RandomTokenRangeSplitter INSTANCE = new RandomTokenRangeSplitter();

  private RandomTokenRangeSplitter() {}

  @Override
  public List<TokenRange<BigInteger, Token<BigInteger>>> split(
      TokenRange<BigInteger, Token<BigInteger>> tokenRange, int splitCount) {
    BigInteger rangeSize = tokenRange.size();
    int splitPointsCount =
        (rangeSize.compareTo(new BigInteger(String.valueOf(splitCount))) < 0)
            ? rangeSize.intValueExact()
            : splitCount;
    List<Token<BigInteger>> splitPoints =
        IntStream.range(0, splitPointsCount)
            .mapToObj(
                i -> {
                  BigInteger nextToken =
                      tokenRange
                          .start()
                          .value()
                          .add(
                              rangeSize
                                  .multiply(BigInteger.valueOf(i))
                                  .divide(BigInteger.valueOf(splitPointsCount)));
                  return new RandomToken(wrap(nextToken));
                })
            .collect(Collectors.toList());
    splitPoints.add(tokenRange.end());
    return IntStream.range(0, splitPoints.size() - 1)
        .mapToObj(i -> splitPoints.subList(i, i + 2))
        .map(
            window ->
                new TokenRange<>(
                    window.get(0), window.get(1), tokenRange.replicas(), tokenRange.tokenFactory()))
        .collect(Collectors.toList());
  }

  private BigInteger wrap(BigInteger token) {
    return token.compareTo(RandomTokenFactory.MAX_TOKEN_VALUE) <= 0
        ? token
        : token.subtract(RandomTokenFactory.MAX_TOKEN_VALUE);
  }
}
