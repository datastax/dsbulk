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
package com.datastax.oss.dsbulk.workflow.commons.schema;

import static com.datastax.oss.driver.shaded.guava.common.base.Functions.identity;
import static java.util.stream.Collectors.toMap;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.partitioner.utils.TokenUtils;
import com.datastax.oss.dsbulk.workflow.commons.settings.StatsSettings.StatisticsMode;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

public class DefaultReadResultCounter implements ReadResultCounter {

  private static final BiFunction<Long, Long, Long> SUM = Long::sum;

  private final int numPartitions;
  private final ProtocolVersion protocolVersion;
  private final ConvertingCodecFactory codecFactory;

  private final TokenMap tokenMap;
  private final Set<TokenRange> allTokenRanges;
  private final Set<EndPoint> allAddresses;
  private final Token[] ring;
  private final ReplicaSet[] replicaSets;

  private final CopyOnWriteArrayList<DefaultCountingUnit> units = new CopyOnWriteArrayList<>();

  private final boolean countGlobal;
  private final boolean countNodes;
  private final boolean countRanges;
  private final boolean countPartitions;
  private final boolean multiCount;

  @VisibleForTesting long totalRows;
  @VisibleForTesting Map<TokenRange, Long> totalsByRange;
  @VisibleForTesting Map<EndPoint, Long> totalsByNode;
  @VisibleForTesting List<PartitionKeyCount> totalsByPartitionKey;

  public DefaultReadResultCounter(
      CqlIdentifier keyspace,
      Metadata metadata,
      EnumSet<StatisticsMode> modes,
      int numPartitions,
      ProtocolVersion protocolVersion,
      ConvertingCodecFactory codecFactory) {
    this.tokenMap =
        metadata
            .getTokenMap()
            .orElseThrow(() -> new IllegalStateException("Token metadata not present"));
    this.numPartitions = numPartitions;
    this.protocolVersion = protocolVersion;
    this.codecFactory = codecFactory;
    countGlobal = modes.contains(StatisticsMode.global);
    countNodes = modes.contains(StatisticsMode.hosts);
    countRanges = modes.contains(StatisticsMode.ranges);
    countPartitions = modes.contains(StatisticsMode.partitions);
    multiCount = modes.size() > 1;
    if (countNodes || countRanges) {
      // Store required metadata in two data structures that will speed up lookups by token:
      // 1) 'ring' stores the range start tokens of all ranges, contents are identical to
      // metadata.tokenMap.ring and are designed to allow binary searches by token.
      // 2) 'replicaSets' stores a) a given range and b) all of its replicas for the given keyspace,
      // contents are identical to metadata.tokenMap.tokenToNodesByKeyspace.
      // Both arrays are filled so that ring[i] == replicaSets[i].range.end,
      // thus allowing to easily locate the range and replicas of a given token.
      Set<TokenRange> ranges =
          metadata.getTokenMap().map(TokenMap::getTokenRanges).orElse(Collections.emptySet());
      ring = new Token[ranges.size()];
      replicaSets = new ReplicaSet[ranges.size()];
      int i = 0;
      Map<Token, TokenRange> rangesByEndingToken =
          ranges.stream().collect(toMap(TokenRange::getEnd, identity()));
      for (TokenRange r1 : ranges) {
        ring[i] = r1.getStart();
        TokenRange r2 = rangesByEndingToken.get(r1.getStart());
        replicaSets[i] = new ReplicaSet(r2, tokenMap.getReplicas(keyspace, r2));
        i++;
      }
      // 'allTokenRanges' and 'allAddresses' are sorted structures that will only serve when
      // printing final totals.
      if (countRanges) {
        allTokenRanges = new TreeSet<>(ranges);
      } else {
        allTokenRanges = null;
      }
      if (countNodes) {
        allAddresses = new TreeSet<>(Comparator.comparing(EndPoint::toString));
        metadata.getNodes().values().stream().map(Node::getEndPoint).forEach(allAddresses::add);
      } else {
        allAddresses = null;
      }
    } else {
      ring = null;
      replicaSets = null;
      allTokenRanges = null;
      allAddresses = null;
    }
  }

  @Override
  public DefaultCountingUnit newCountingUnit() {
    DefaultCountingUnit unit = new DefaultCountingUnit();
    units.add(unit);
    return unit;
  }

  @Override
  public void close() {
    consolidateUnitCounts();
  }

  @VisibleForTesting
  void consolidateUnitCounts() {
    totalRows = 0;
    totalsByRange = new HashMap<>();
    totalsByNode = new HashMap<>();
    totalsByPartitionKey = new ArrayList<>();
    for (DefaultCountingUnit unit : units) {
      unit.close();
      totalRows += unit.total;
      unit.totalsByNode.forEach((key, value) -> totalsByNode.merge(key, value, SUM));
      unit.totalsByRange.forEach((key, value) -> totalsByRange.merge(key, value, SUM));
      totalsByPartitionKey.addAll(unit.totalsByPartitionKey);
    }
    totalsByPartitionKey.sort(Collections.reverseOrder());
    totalsByPartitionKey =
        totalsByPartitionKey.size() > numPartitions
            ? totalsByPartitionKey.subList(0, numPartitions)
            : totalsByPartitionKey;
  }

  @Override
  public void reportTotals() {
    PrintStream out = System.out;
    if (countGlobal) {
      if (multiCount) {
        out.println("Total rows:");
      }
      out.println(totalRows);
    }
    if (countNodes) {
      if (multiCount) {
        out.println("Total rows per node:");
      }
      allAddresses.forEach(
          node -> {
            long totalPerNode = totalsByNode.containsKey(node) ? totalsByNode.get(node) : 0;
            float percentage = (float) totalPerNode / (float) totalRows * 100f;
            out.printf("%s %d %.2f%n", node, totalPerNode, percentage);
          });
    }
    if (countRanges) {
      if (multiCount) {
        out.println("Total rows per token range:");
      }
      allTokenRanges.forEach(
          range -> {
            long totalPerRange = totalsByRange.containsKey(range) ? totalsByRange.get(range) : 0;
            float percentage = (float) totalPerRange / (float) totalRows * 100f;
            out.printf(
                "%s %s %d %.2f%n",
                TokenUtils.getTokenValue(range.getStart()),
                TokenUtils.getTokenValue(range.getEnd()),
                totalPerRange,
                percentage);
          });
    }
    if (countPartitions) {
      if (multiCount) {
        out.println("Total rows per partition:");
      }
      totalsByPartitionKey.forEach(
          count -> {
            float percentage = (float) count.count / (float) totalRows * 100f;
            out.printf("%s %d %.2f%n", count.pk, count.count, percentage);
          });
    }
  }

  /**
   * A counting unit.
   *
   * <p>Each counting unit is meant to be accessed by one single thread at a time, and thus its
   * internals do not require synchronization or concurrent structures.
   *
   * <p>Each thread/counting unit counts its own portion of the result set, then at the end, their
   * results are consolidated.
   */
  @VisibleForTesting
  class DefaultCountingUnit implements CountingUnit {

    long total = 0;
    final Map<TokenRange, Long> totalsByRange = new HashMap<>();
    final Map<EndPoint, Long> totalsByNode = new HashMap<>();
    final List<PartitionKeyCount> totalsByPartitionKey = new ArrayList<>(numPartitions + 1);
    long currentPkCount = 0;
    PartitionKey currentPk;

    @Override
    public void update(ReadResult result) {
      Row row = result.getRow().orElseThrow(IllegalStateException::new);
      // First compute the partition key and the token for this row.
      Token token = null;
      PartitionKey pk = null;
      if (countPartitions) {
        // When counting partitions, the result set is expected to contain
        // the row's partition key, in proper order
        int size = row.getColumnDefinitions().size();
        ByteBuffer[] bbs = new ByteBuffer[size];
        for (int i = 0; i < size; i++) {
          bbs[i] = row.getBytesUnsafe(i);
        }
        if (countRanges || countNodes) {
          // compute the token client-side from the partition keys
          token = tokenMap.newToken(bbs);
        }
        pk = new PartitionKey(row.getColumnDefinitions(), bbs);
      } else if (countRanges || countNodes) {
        // When counting hosts or ranges, without counting partitions,
        // the result set is expected to contain one single column containing
        // the partition key's token
        token = row.getToken(0);
      }
      // Then increment required counters.
      // Note: we need to always increment the global counter because it's used to compute
      // percentages for other stats.
      total++;
      if (countRanges || countNodes) {
        ReplicaSet replicaSet = getReplicaSet(token);
        if (countRanges) {
          totalsByRange.merge(replicaSet.range, 1L, SUM);
        }
        if (countNodes) {
          for (EndPoint address : replicaSet.addresses) {
            totalsByNode.merge(address, 1L, SUM);
          }
        }
      }
      if (countPartitions) {
        if (currentPk == null) {
          currentPk = pk;
        }
        // Note: the counting algorithm relies on the fact that any given
        // partition will be entirely counted by the same unit,
        // and that partitions will be returned in order, i.e.,
        // all rows belonging to the same partition will appear in sequence.
        if (!currentPk.equals(pk)) {
          rotatePk();
          currentPk = pk;
          currentPkCount = 1;
        } else {
          currentPkCount++;
        }
      }
    }

    @Override
    public void close() {
      rotatePk();
    }

    /**
     * Locate the end token of the range containing the given token then use it to lookup the entire
     * range and its replicas. This search is identical to the search performed by
     * Metadata.TokenMap.getReplicas(String keyspace, Token token). Only used when counting ranges
     * or nodes.
     */
    private ReplicaSet getReplicaSet(Token token) {
      assert ring != null;
      assert replicaSets != null;
      int i = Arrays.binarySearch(ring, token);
      if (i < 0) {
        i = -i - 1;
        if (i >= ring.length) {
          i = 0;
        }
      }
      return replicaSets[i];
    }

    /**
     * Computes the total for the current partition key, stores it in 'totalsByPartitionKey' if the
     * count is big enough to be included, otherwise discards it.
     */
    void rotatePk() {
      if (currentPk != null) {
        long lowestPkCount = totalsByPartitionKey.isEmpty() ? 0 : totalsByPartitionKey.get(0).count;
        // Include the count if
        // 1) it's count is bigger than the lowest count in the list, or
        // 2) if the list is not full yet.
        if (currentPkCount > lowestPkCount || totalsByPartitionKey.size() < numPartitions) {
          PartitionKeyCount pkc = new PartitionKeyCount(currentPk, currentPkCount);
          int pos = Collections.binarySearch(totalsByPartitionKey, pkc);
          if (pos < 0) {
            pos = -pos - 1;
          }
          totalsByPartitionKey.add(pos, pkc);
          // If this addition caused the list to grow past the max, remove the lowest element.
          if (totalsByPartitionKey.size() > numPartitions) {
            totalsByPartitionKey.remove(0);
          }
        }
        currentPk = null;
      }
    }
  }

  @VisibleForTesting
  class PartitionKey {

    final ByteBuffer[] components;
    final DataType[] types;
    final int hashCode;

    PartitionKey(ColumnDefinitions definitions, ByteBuffer... components) {
      this.components = components;
      hashCode = Arrays.hashCode(components);
      types = new DataType[components.length];
      for (int i = 0; i < components.length; i++) {
        types[i] = definitions.get(i).getType();
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartitionKey)) {
        return false;
      }
      PartitionKey that = (PartitionKey) o;
      if (this.hashCode != that.hashCode) {
        return false;
      }
      return Arrays.equals(components, that.components);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < components.length; i++) {
        if (i > 0) {
          sb.append('|');
        }
        TypeCodec<Object> codec = codecFactory.getCodecRegistry().codecFor(types[i]);
        Object o = codec.decode(components[i], protocolVersion);
        sb.append(codec.format(o));
      }
      // Remove spaces from output to preserve the number of columns
      return sb.toString().replace(' ', '_');
    }
  }

  @VisibleForTesting
  static class PartitionKeyCount implements Comparable<PartitionKeyCount> {

    final PartitionKey pk;
    final long count;

    PartitionKeyCount(PartitionKey pk, long currentCount) {
      this.pk = pk;
      count = currentCount;
    }

    @Override
    public int compareTo(@NonNull DefaultReadResultCounter.PartitionKeyCount that) {
      return Long.compare(this.count, that.count);
    }
  }

  private static class ReplicaSet {

    final TokenRange range;
    final EndPoint[] addresses;

    ReplicaSet(TokenRange range, Set<Node> addresses) {
      this.range = range;
      this.addresses = addresses.stream().map(Node::getEndPoint).toArray(EndPoint[]::new);
    }
  }
}
