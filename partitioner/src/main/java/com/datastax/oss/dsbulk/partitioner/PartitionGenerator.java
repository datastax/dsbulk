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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionGenerator {

  private final CqlIdentifier keyspace;
  private final BulkTokenFactory tokenFactory;
  private final TokenMap tokenMap;

  public PartitionGenerator(
      CqlIdentifier keyspace, TokenMap tokenMap, BulkTokenFactory tokenFactory) {
    this.keyspace = keyspace;
    this.tokenMap = tokenMap;
    this.tokenFactory = tokenFactory;
  }

  /**
   * Partitions the entire ring into approximately {@code splitCount} splits.
   *
   * @param splitCount The desired number of splits.
   */
  @NonNull
  public List<BulkTokenRange> partition(int splitCount) {
    List<BulkTokenRange> tokenRanges = describeRing(splitCount);
    int endpointCount = (int) tokenRanges.stream().map(BulkTokenRange::replicas).distinct().count();
    int maxGroupSize = tokenRanges.size() / endpointCount;
    TokenRangeSplitter splitter = tokenFactory.splitter();
    List<BulkTokenRange> splits = splitter.split(tokenRanges, splitCount);
    checkRing(splits);
    TokenRangeClusterer clusterer = tokenFactory.clusterer();
    List<BulkTokenRange> groups = clusterer.group(splits, splitCount, maxGroupSize);
    checkRing(groups);
    return groups;
  }

  private List<BulkTokenRange> describeRing(int splitCount) {
    List<BulkTokenRange> ranges =
        tokenMap.getTokenRanges().stream().map(this::toBulkRange).collect(Collectors.toList());
    if (splitCount == 1) {
      BulkTokenRange r = ranges.get(0);
      return Collections.singletonList(
          tokenFactory.range(tokenFactory.minToken(), tokenFactory.minToken(), r.replicas()));
    } else {
      return ranges;
    }
  }

  private BulkTokenRange toBulkRange(TokenRange range) {
    Set<EndPoint> replicas =
        tokenMap.getReplicas(keyspace, range).stream()
            .map(Node::getEndPoint)
            .collect(Collectors.toSet());
    return tokenFactory.range(range.getStart(), range.getEnd(), replicas);
  }

  private void checkRing(List<BulkTokenRange> splits) {
    double sum = splits.stream().map(BulkTokenRange::fraction).reduce(0d, Double::sum);
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
