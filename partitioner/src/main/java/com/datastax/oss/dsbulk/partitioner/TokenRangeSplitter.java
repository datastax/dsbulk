/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.partitioner;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Splits token ranges into smaller sub-ranges. */
interface TokenRangeSplitter<V extends Number, T extends Token<V>> {

  /**
   * Splits the given ranges uniformly into (smaller) {@code splitCount} chunks.
   *
   * <p>Note that the algorithm is size-based and doesn't guarantee exact split count.
   *
   * @param tokenRanges The ranges to split.
   * @param splitCount The desired number of resulting chunks.
   * @return A list of ranges of approximately {@code splitCount} chunks.
   */
  default List<TokenRange<V, T>> split(Iterable<TokenRange<V, T>> tokenRanges, int splitCount) {
    double ringFractionPerSplit = 1.0 / (double) splitCount;
    return StreamSupport.stream(tokenRanges.spliterator(), false)
        .flatMap(
            range -> {
              int splits = (int) Math.max(1, Math.rint(range.fraction() / ringFractionPerSplit));
              List<TokenRange<V, T>> split =
                  splits == 1 ? Collections.singletonList(range) : split(range, splits);
              return split.stream();
            })
        .collect(Collectors.toList());
  }

  /**
   * Splits the given token range uniformly into sub-ranges with the given desired split count.
   *
   * <p>Note that the algorithm is size-based and doesn't guarantee exact split count.
   *
   * @param tokenRange The range to split.
   * @param splitCount The desired number of resulting chunks.
   * @return A list of ranges of approximately {@code splitCount} chunks.
   */
  List<TokenRange<V, T>> split(TokenRange<V, T> tokenRange, int splitCount);
}
