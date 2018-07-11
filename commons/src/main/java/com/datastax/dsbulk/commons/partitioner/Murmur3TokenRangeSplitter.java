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

class Murmur3TokenRangeSplitter implements TokenRangeSplitter<Long, Token<Long>> {

  public static final Murmur3TokenRangeSplitter INSTANCE = new Murmur3TokenRangeSplitter();

  private Murmur3TokenRangeSplitter() {}

  @Override
  public List<TokenRange<Long, Token<Long>>> split(
      TokenRange<Long, Token<Long>> tokenRange, int splitCount) {

    BigInteger rangeSize = tokenRange.size();
    // If the number of splits is lesser than the range size,
    // use the range size as number of splits and yield (size-of-range) splits of size 1
    int splitPointsCount =
        rangeSize.compareTo(BigInteger.valueOf(splitCount)) < 0
            ? rangeSize.intValueExact()
            : splitCount;
    List<Token<Long>> splitPoints =
        IntStream.range(0, splitPointsCount)
            .mapToObj(
                i ->
                    new Murmur3Token(
                        BigInteger.valueOf(tokenRange.start().value())
                            .add(
                                // instead of applying a fix increment we multiply and
                                // divide again at each step to compensate for non-integral
                                // increment sizes and thus to create splits of sizes as even as
                                // possible (iow, to minimize the split sizes variance).
                                rangeSize
                                    .multiply(BigInteger.valueOf(i))
                                    .divide(BigInteger.valueOf(splitPointsCount)))
                            // only convert to long at the end of the computation to avoid
                            // long overflows.
                            .longValueExact()))
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
}
