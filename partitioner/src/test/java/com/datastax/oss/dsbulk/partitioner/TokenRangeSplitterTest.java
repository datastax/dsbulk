/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.partitioner;

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TokenRangeSplitterTest {

  private static final BigInteger _2 = BigInteger.valueOf(2);
  private static final BigInteger _4 = BigInteger.valueOf(4);
  private static final BigInteger _8 = BigInteger.valueOf(8);
  private static final BigInteger _10 = BigInteger.valueOf(10);
  private static final BigInteger _100 = BigInteger.valueOf(100);
  private static final BigInteger _200 = BigInteger.valueOf(200);
  private static final BigInteger _332 = BigInteger.valueOf(332);
  private static final BigInteger _334 = BigInteger.valueOf(334);

  @ParameterizedTest(name = "{index}: {1} in {2} splits = {3} splits of size {4} to {5} ({0})")
  @MethodSource("singleRangeSplitCases")
  <V extends Number, T extends Token<V>> void should_split_single_range(
      TokenRangeSplitter<V, T> splitter,
      TokenRange<V, T> range,
      int splitCount,
      int expectedSplitCount,
      BigInteger expectedMinSplitSize,
      BigInteger expectedMaxSplitSize) {
    // when
    List<TokenRange<V, T>> splits = splitter.split(range, splitCount);
    // then
    // check number of splits
    assertThat(splits.size()).isEqualTo(expectedSplitCount);
    assertThat(splits.get(0).start()).isEqualTo(range.start());
    assertThat(splits.get(splits.size() - 1).end()).isEqualTo(range.end());
    // check each split's replicas & size
    for (TokenRange<V, T> split : splits) {
      assertThat(split.replicas()).containsExactlyElementsOf(range.replicas());
      assertThat(split.size()).isBetween(expectedMinSplitSize, expectedMaxSplitSize);
    }
    // check sum(split.size) = range.size
    assertThat(splits.stream().map(TokenRange::size).reduce(ZERO, BigInteger::add))
        .isEqualTo(range.size());
    // check sum(split.fraction) - range.fraction
    assertThat(splits.stream().map(TokenRange::fraction).reduce(0d, Double::sum))
        .isEqualTo(range.fraction(), Assertions.offset(.000000001));
    // check range contiguity
    for (int i = 0; i < splits.size() - 1; i++) {
      TokenRange<V, T> split1 = splits.get(i);
      TokenRange<V, T> split2 = splits.get(i + 1);
      assertThat(split1.end()).isEqualTo(split2.start());
    }
  }

  @ParameterizedTest(name = "{index}: N ranges in {2} splits = {3} splits of size {4} to {5} ({0})")
  @MethodSource("multipleRangesSplitCases")
  <V extends Number, T extends Token<V>> void should_split_multiple_ranges(
      TokenRangeSplitter<V, T> splitter,
      List<TokenRange<V, T>> ranges,
      int splitCount,
      int expectedSplitCount,
      BigInteger expectedMinSplitSize,
      BigInteger expectedMaxSplitSize) {
    // when
    List<TokenRange<V, T>> splits = splitter.split(ranges, splitCount);
    // then
    // check number of splits
    assertThat(splits.size()).isEqualTo(expectedSplitCount);
    assertThat(splits.get(0).start()).isEqualTo(ranges.get(0).start());
    assertThat(splits.get(splits.size() - 1).end()).isEqualTo(ranges.get(ranges.size() - 1).end());
    // check each split's size
    for (TokenRange<V, T> split : splits) {
      assertThat(split.size()).isBetween(expectedMinSplitSize, expectedMaxSplitSize);
    }
    // check sum(split.size) = sum(range.size)
    assertThat(splits.stream().map(TokenRange::size).reduce(ZERO, BigInteger::add))
        .isEqualTo(ranges.stream().map(TokenRange::size).reduce(ZERO, BigInteger::add));
    // check sum(split.fraction) - range.fraction
    assertThat(splits.stream().map(TokenRange::fraction).reduce(0d, Double::sum))
        .isEqualTo(1.0, Assertions.offset(.000000001));
    // check range contiguity
    for (int i = 0; i < splits.size() - 1; i++) {
      TokenRange<V, T> range1 = splits.get(i);
      TokenRange<V, T> range2 = splits.get(i + 1);
      assertThat(range1.end()).isEqualTo(range2.start());
    }
  }

  @SuppressWarnings("Unused")
  private static Stream<Arguments> singleRangeSplitCases() {
    return Stream.concat(
        singleRangeSplitCases(Murmur3TokenFactory.INSTANCE),
        singleRangeSplitCases(RandomTokenFactory.INSTANCE));
  }

  @SuppressWarnings("Unused")
  private static Stream<Arguments> multipleRangesSplitCases() {
    return Stream.concat(
        multipleRangesSplitCases(Murmur3TokenFactory.INSTANCE),
        multipleRangesSplitCases(RandomTokenFactory.INSTANCE));
  }

  @NonNull
  private static <V extends Number, T extends Token<V>> Stream<Arguments> singleRangeSplitCases(
      TokenFactory<V, T> tokenFactory) {
    TokenRangeSplitter<V, T> splitter = tokenFactory.splitter();
    List<TokenRange<V, T>> hugeRanges = splitWholeRingIn(10, tokenFactory);
    TokenRange<V, T> entireRing = splitWholeRingIn(1, tokenFactory).get(0);
    TokenRange<V, T> firstHugeRange = hugeRanges.get(0);
    TokenRange<V, T> lastHugeRange = hugeRanges.get(hugeRanges.size() - 1);
    return Stream.of(
        Arguments.of(splitter, range(0, 1, tokenFactory), 1, 1, ONE, ONE),
        Arguments.of(splitter, range(0, 10, tokenFactory), 1, 1, _10, _10),
        Arguments.of(splitter, range(0, 1, tokenFactory), 10, 1, ONE, ONE),
        Arguments.of(splitter, range(0, 9, tokenFactory), 10, 9, ONE, ONE),
        Arguments.of(splitter, range(0, 10, tokenFactory), 10, 10, ONE, ONE),
        Arguments.of(splitter, range(0, 11, tokenFactory), 10, 10, ZERO, _2),
        Arguments.of(splitter, range(10, 50, tokenFactory), 10, 10, _4, _4),
        Arguments.of(splitter, range(0, 1000, tokenFactory), 3, 3, _332, _334),
        Arguments.of(
            splitter,
            firstHugeRange,
            100,
            100,
            firstHugeRange.size().divide(_100).subtract(_4),
            firstHugeRange.size().divide(_100).add(_4)),
        Arguments.of(
            splitter,
            lastHugeRange,
            100,
            100,
            lastHugeRange.size().divide(_100).subtract(_4),
            lastHugeRange.size().divide(_100).add(_4)),
        Arguments.of(
            splitter,
            entireRing,
            8,
            8,
            entireRing.size().divide(_8).subtract(ONE),
            entireRing.size().divide(_8).add(ONE)));
  }

  @NonNull
  private static <V extends Number, T extends Token<V>> Stream<Arguments> multipleRangesSplitCases(
      TokenFactory<V, T> tokenFactory) {
    TokenRangeSplitter<V, T> splitter = tokenFactory.splitter();
    List<TokenRange<V, T>> mediumRanges = splitWholeRingIn(100, tokenFactory);
    BigInteger wholeRingSize =
        mediumRanges.stream().map(TokenRange::size).reduce(ZERO, BigInteger::add);
    return Stream.of(
        // we have 100 ranges, so 100 splits is minimum
        Arguments.of(
            splitter, mediumRanges, 3, 100, wholeRingSize.divide(_100), wholeRingSize.divide(_100)),
        Arguments.of(
            splitter,
            mediumRanges,
            100,
            100,
            wholeRingSize.divide(_100),
            wholeRingSize.divide(_100)),
        Arguments.of(
            splitter,
            mediumRanges,
            101,
            100,
            wholeRingSize.divide(_100),
            wholeRingSize.divide(_100)),
        Arguments.of(
            splitter,
            mediumRanges,
            149,
            100,
            wholeRingSize.divide(_100),
            wholeRingSize.divide(_100)),
        Arguments.of(
            splitter,
            mediumRanges,
            150,
            200,
            wholeRingSize.divide(_200).subtract(ONE),
            wholeRingSize.divide(_200).add(ONE)),
        Arguments.of(
            splitter,
            mediumRanges,
            151,
            200,
            wholeRingSize.divide(_200).subtract(ONE),
            wholeRingSize.divide(_200).add(ONE)),
        Arguments.of(
            splitter,
            mediumRanges,
            199,
            200,
            wholeRingSize.divide(_200).subtract(ONE),
            wholeRingSize.divide(_200).add(ONE)),
        Arguments.of(
            splitter,
            mediumRanges,
            200,
            200,
            wholeRingSize.divide(_200).subtract(ONE),
            wholeRingSize.divide(_200).add(ONE)),
        Arguments.of(
            splitter,
            mediumRanges,
            201,
            200,
            wholeRingSize.divide(_200).subtract(ONE),
            wholeRingSize.divide(_200).add(ONE)));
  }

  private static <V extends Number, T extends Token<V>> List<TokenRange<V, T>> splitWholeRingIn(
      int count, TokenFactory<V, T> tokenFactory) {
    BigInteger hugeTokensIncrement =
        tokenFactory.totalTokenCount().divide(BigInteger.valueOf(count));
    List<TokenRange<V, T>> ranges = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ranges.add(
          range(
              new BigInteger(tokenFactory.minToken().value().toString())
                  .add(hugeTokensIncrement.multiply(BigInteger.valueOf(i))),
              new BigInteger(tokenFactory.minToken().value().toString())
                  .add(hugeTokensIncrement.multiply(BigInteger.valueOf(i + 1))),
              tokenFactory));
    }
    return ranges;
  }

  private static <V extends Number, T extends Token<V>> TokenRange<V, T> range(
      BigInteger start, BigInteger end, TokenFactory<V, T> tokenFactory) {
    return new TokenRange<>(
        tokenFactory.tokenFromString(start.toString()),
        tokenFactory.tokenFromString(end.toString()),
        Collections.singleton(
            new DefaultEndPoint(InetSocketAddress.createUnresolved("127.0.0.1", 9042))),
        tokenFactory);
  }

  private static <V extends Number, T extends Token<V>> TokenRange<V, T> range(
      long start, long end, TokenFactory<V, T> tokenFactory) {
    return range(BigInteger.valueOf(start), BigInteger.valueOf(end), tokenFactory);
  }
}
