/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionGenerator<V extends Number, T extends Token<V>> {

  private final KeyspaceMetadata keyspace;
  private final Metadata metadata;
  private final TokenFactory<V, T> tokenFactory;

  public PartitionGenerator(
      KeyspaceMetadata keyspace, Metadata metadata, TokenFactory<V, T> tokenFactory) {
    this.keyspace = keyspace;
    this.metadata = metadata;
    this.tokenFactory = tokenFactory;
  }

  /**
   * Partitions the entire ring into approximately {@code splitCount} splits.
   *
   * @param splitCount The desired number of splits.
   */
  public List<TokenRange<V, T>> partition(int splitCount) {
    List<TokenRange<V, T>> tokenRanges = describeRing(splitCount);
    int endpointCount = (int) tokenRanges.stream().map(TokenRange::replicas).distinct().count();
    int maxGroupSize = tokenRanges.size() / endpointCount;
    TokenRangeSplitter<V, T> splitter = tokenFactory.splitter();
    List<TokenRange<V, T>> splits = splitter.split(tokenRanges, splitCount);
    checkRing(splits);
    TokenRangeClusterer<V, T> clusterer = new TokenRangeClusterer<>(splitCount, maxGroupSize);
    List<TokenRange<V, T>> groups = clusterer.group(splits);
    checkRing(groups);
    return groups;
  }

  private List<TokenRange<V, T>> describeRing(int splitCount) {
    List<TokenRange<V, T>> ranges =
        metadata.getTokenRanges().stream().map(this::range).collect(Collectors.toList());
    if (splitCount == 1) {
      TokenRange<V, T> r = ranges.get(0);
      return Collections.singletonList(
          new TokenRange<>(
              tokenFactory.minToken(), tokenFactory.minToken(), r.replicas(), tokenFactory));
    } else {
      return ranges;
    }
  }

  private TokenRange<V, T> range(com.datastax.driver.core.TokenRange range) {
    T startToken = tokenFactory.tokenFromString(range.getStart().getValue().toString());
    T endToken = tokenFactory.tokenFromString(range.getEnd().getValue().toString());
    Set<InetSocketAddress> replicas =
        metadata.getReplicas(Metadata.quoteIfNecessary(keyspace.getName()), range).stream()
            .map(Host::getSocketAddress)
            .collect(Collectors.toSet());
    return new TokenRange<>(startToken, endToken, replicas, tokenFactory);
  }

  private void checkRing(List<TokenRange<V, T>> splits) {
    double sum = splits.stream().map(TokenRange::fraction).reduce(0d, (f1, f2) -> f1 + f2);
    if (Math.rint(sum) != 1.0d) {
      throw new IllegalStateException(
          String.format(
              "Incomplete ring partition detected: %1.3f. "
                  + "This is likely a bug in DSBulk, please report. "
                  + "Generated splits: %s.",
              sum, splits));
    }
  }
}
