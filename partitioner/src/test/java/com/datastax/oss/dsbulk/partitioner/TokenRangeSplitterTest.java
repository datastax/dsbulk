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

import static com.datastax.oss.dsbulk.commons.utils.TokenUtils.getTokenValue;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.dsbulk.partitioner.murmur3.Murmur3BulkTokenFactory;
import com.datastax.oss.dsbulk.partitioner.random.RandomBulkTokenFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
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
  void should_split_single_range(
      TokenRangeSplitter splitter,
      BulkTokenRange range,
      int splitCount,
      int expectedSplitCount,
      BigInteger expectedMinSplitSize,
      BigInteger expectedMaxSplitSize) {
    // when
    List<BulkTokenRange> splits = splitter.split(range, splitCount);
    // then
    // check number of splits
    assertThat(splits.size()).isEqualTo(expectedSplitCount);
    assertThat(splits.get(0).getStart()).isEqualTo(range.getStart());
    assertThat(splits.get(splits.size() - 1).getEnd()).isEqualTo(range.getEnd());
    // check each split's replicas & size
    for (BulkTokenRange split : splits) {
      assertThat(split.replicas()).containsExactlyElementsOf(range.replicas());
      assertThat(split.size()).isBetween(expectedMinSplitSize, expectedMaxSplitSize);
    }
    // check sum(split.size) = range.size
    assertThat(splits.stream().map(BulkTokenRange::size).reduce(ZERO, BigInteger::add))
        .isEqualTo(range.size());
    // check sum(split.fraction) - range.fraction
    assertThat(splits.stream().map(BulkTokenRange::fraction).reduce(0d, Double::sum))
        .isEqualTo(range.fraction(), offset(.000000001));
    // check range contiguity
    for (int i = 0; i < splits.size() - 1; i++) {
      BulkTokenRange split1 = splits.get(i);
      BulkTokenRange split2 = splits.get(i + 1);
      assertThat(split1.getEnd()).isEqualTo(split2.getStart());
    }
  }

  @ParameterizedTest(name = "{index}: N ranges in {2} splits = {3} splits of size {4} to {5} ({0})")
  @MethodSource("multipleRangesSplitCases")
  void should_split_multiple_ranges(
      TokenRangeSplitter splitter,
      List<BulkTokenRange> ranges,
      int splitCount,
      int expectedSplitCount,
      BigInteger expectedMinSplitSize,
      BigInteger expectedMaxSplitSize) {
    // when
    List<BulkTokenRange> splits = splitter.split(ranges, splitCount);
    // then
    // check number of splits
    assertThat(splits.size()).isEqualTo(expectedSplitCount);
    assertThat(splits.get(0).getStart()).isEqualTo(ranges.get(0).getStart());
    assertThat(splits.get(splits.size() - 1).getEnd())
        .isEqualTo(ranges.get(ranges.size() - 1).getEnd());
    // check each split's size
    for (BulkTokenRange split : splits) {
      assertThat(split.size()).isBetween(expectedMinSplitSize, expectedMaxSplitSize);
    }
    // check sum(split.size) = sum(range.size)
    assertThat(splits.stream().map(BulkTokenRange::size).reduce(ZERO, BigInteger::add))
        .isEqualTo(ranges.stream().map(BulkTokenRange::size).reduce(ZERO, BigInteger::add));
    // check sum(split.fraction) - range.fraction
    assertThat(splits.stream().map(BulkTokenRange::fraction).reduce(0d, Double::sum))
        .isEqualTo(1.0, offset(.000000001));
    // check range contiguity
    for (int i = 0; i < splits.size() - 1; i++) {
      BulkTokenRange range1 = splits.get(i);
      BulkTokenRange range2 = splits.get(i + 1);
      assertThat(range1.getEnd()).isEqualTo(range2.getStart());
    }
  }

  @SuppressWarnings("Unused")
  private static Stream<Arguments> singleRangeSplitCases() {
    return Stream.concat(
        singleRangeSplitCases(new Murmur3BulkTokenFactory()),
        singleRangeSplitCases(new RandomBulkTokenFactory()));
  }

  @SuppressWarnings("Unused")
  private static Stream<Arguments> multipleRangesSplitCases() {
    return Stream.concat(
        multipleRangesSplitCases(new Murmur3BulkTokenFactory()),
        multipleRangesSplitCases(new RandomBulkTokenFactory()));
  }

  @NonNull
  private static Stream<Arguments> singleRangeSplitCases(BulkTokenFactory tokenFactory) {
    TokenRangeSplitter splitter = tokenFactory.splitter();
    List<BulkTokenRange> hugeRanges = splitWholeRingIn(10, tokenFactory);
    BulkTokenRange entireRing = splitWholeRingIn(1, tokenFactory).get(0);
    BulkTokenRange firstHugeRange = hugeRanges.get(0);
    BulkTokenRange lastHugeRange = hugeRanges.get(hugeRanges.size() - 1);
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
  private static Stream<Arguments> multipleRangesSplitCases(BulkTokenFactory tokenFactory) {
    TokenRangeSplitter splitter = tokenFactory.splitter();
    List<BulkTokenRange> mediumRanges = splitWholeRingIn(100, tokenFactory);
    BigInteger wholeRingSize =
        mediumRanges.stream().map(BulkTokenRange::size).reduce(ZERO, BigInteger::add);
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

  private static List<BulkTokenRange> splitWholeRingIn(int count, BulkTokenFactory tokenFactory) {
    BigInteger hugeTokensIncrement =
        tokenFactory.totalTokenCount().divide(BigInteger.valueOf(count));
    List<BulkTokenRange> ranges = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ranges.add(
          range(
              new BigInteger(getTokenValue(tokenFactory.minToken()).toString())
                  .add(hugeTokensIncrement.multiply(BigInteger.valueOf(i))),
              new BigInteger(getTokenValue(tokenFactory.minToken()).toString())
                  .add(hugeTokensIncrement.multiply(BigInteger.valueOf(i + 1))),
              tokenFactory));
    }
    return ranges;
  }

  private static BulkTokenRange range(
      BigInteger start, BigInteger end, BulkTokenFactory tokenFactory) {
    return tokenFactory.range(
        tokenFactory.parse(start.toString()),
        tokenFactory.parse(end.toString()),
        Collections.singleton(
            new DefaultEndPoint(InetSocketAddress.createUnresolved("127.0.0.1", 9042))));
  }

  private static BulkTokenRange range(long start, long end, BulkTokenFactory tokenFactory) {
    return range(BigInteger.valueOf(start), BigInteger.valueOf(end), tokenFactory);
  }
}
