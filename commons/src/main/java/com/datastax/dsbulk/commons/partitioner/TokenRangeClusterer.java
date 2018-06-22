/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import com.datastax.oss.driver.shaded.guava.common.collect.ComparisonChain;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Groups small, contiguous token ranges sharing the same replicas in order to reduce the total
 * number of splits.
 *
 * <p>Useful mostly with virtual nodes, which may create lots of small token range splits.
 */
class TokenRangeClusterer<V extends Number, T extends Token<V>> {

  private final int maxGroupSize;
  private final double ringFractionPerGroup;

  TokenRangeClusterer(int groupCount) {
    this(groupCount, Integer.MAX_VALUE);
  }

  TokenRangeClusterer(int groupCount, int maxGroupSize) {
    ringFractionPerGroup = 1.0d / groupCount;
    this.maxGroupSize = maxGroupSize;
  }

  /**
   * Groups contiguous ranges together as long as they are contiguous and share the same replicas.
   *
   * <p>Note: the grouping algorithm used in DSBulk is different from the one used by the Spark
   * connector: the former favors groups of contiguous ranges sharing the same replicas (in order to
   * make it possible to route a range read to a coordinator that is also a replica), whereas the
   * latter favors data locality (i.e. groups even non-continguous ranges as long as they share at
   * least one common replica).
   */
  @NonNull
  List<TokenRange<V, T>> group(List<TokenRange<V, T>> ranges) {
    Deque<TokenRange<V, T>> sorted = sortRanges(ranges);
    List<TokenRange<V, T>> grouped = new ArrayList<>();
    if (sorted.isEmpty()) {
      return grouped;
    }
    while (!sorted.isEmpty()) {
      TokenRange<V, T> head = sorted.peek();
      assert head != null;
      double ringFractionLimit =
          Math.max(
              ringFractionPerGroup,
              head.fraction()); // make sure first element will be always included
      double cumulativeRingFraction = 0;
      T end = head.start();
      for (int i = 0; i < Math.max(1, maxGroupSize) && !sorted.isEmpty(); i++) {
        TokenRange<V, T> current = sorted.peek();
        assert current != null;
        cumulativeRingFraction += current.fraction();
        // keep grouping ranges as long as they share the same replicas and the resulting
        // range is contiguous.
        if (cumulativeRingFraction > ringFractionLimit
            || !head.replicas().equals(current.replicas())
            || !end.equals(current.start())) {
          break;
        }
        sorted.pop();
        end = current.end();
      }
      grouped.add(new TokenRange<>(head.start(), end, head.replicas(), head.tokenFactory()));
    }
    return grouped.stream().flatMap(tr -> tr.unwrap().stream()).collect(Collectors.toList());
  }

  private Deque<TokenRange<V, T>> sortRanges(Iterable<TokenRange<V, T>> ranges) {
    LinkedList<TokenRange<V, T>> sorted = Lists.newLinkedList(ranges);
    sorted.sort(
        (tr1, tr2) ->
            ComparisonChain.start()
                .compare(tr1.start(), tr2.start())
                .compare(tr1.end(), tr2.end())
                .result());
    return sorted;
  }
}
