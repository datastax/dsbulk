/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.global;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.ranges;
import static com.google.common.base.Functions.identity;
import static java.util.stream.Collectors.toMap;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.settings.StatsSettings;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.annotations.VisibleForTesting;
import java.io.PrintStream;
import java.net.InetSocketAddress;
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
import org.jetbrains.annotations.NotNull;

public class DefaultReadResultCounter implements ReadResultCounter {

  private static final BiFunction<Long, Long, Long> SUM = (v1, v2) -> v1 + v2;

  private final int numPartitions;
  private final ProtocolVersion protocolVersion;
  private final ExtendedCodecRegistry codecRegistry;

  private final Metadata metadata;
  private final Set<TokenRange> allTokenRanges;
  private final Set<InetSocketAddress> allAddresses;
  private final Token[] ring;
  private final ReplicaSet[] replicaSets;

  private final CopyOnWriteArrayList<DefaultCountingUnit> units = new CopyOnWriteArrayList<>();

  private final boolean countGlobal;
  private final boolean countHosts;
  private final boolean countRanges;
  private final boolean countPartitions;
  private final boolean multiCount;

  @VisibleForTesting long totalRows;
  @VisibleForTesting Map<TokenRange, Long> totalsByRange;
  @VisibleForTesting Map<InetSocketAddress, Long> totalsByHost;
  @VisibleForTesting List<PartitionKeyCount> totalsByPartitionKey;

  public DefaultReadResultCounter(
      String keyspace,
      Metadata metadata,
      EnumSet<StatsSettings.StatisticsMode> modes,
      int numPartitions,
      ProtocolVersion protocolVersion,
      ExtendedCodecRegistry codecRegistry) {
    this.metadata = metadata;
    this.numPartitions = numPartitions;
    this.protocolVersion = protocolVersion;
    this.codecRegistry = codecRegistry;
    countGlobal = modes.contains(global);
    countHosts = modes.contains(hosts);
    countRanges = modes.contains(ranges);
    countPartitions = modes.contains(partitions);
    multiCount = modes.size() > 1;
    if (countHosts || countRanges) {
      // Store required metadata in two data structures that will speed up lookups by token:
      // 1) 'ring' stores the range start tokens of all ranges, contents are identical to
      // metadata.tokenMap.ring and are designed to allow binary searches by token.
      // 2) 'replicaSets' stores a) a given range and b) all of its replicas for the given keyspace,
      // contents are identical to metadata.tokenMap.tokenToHostsByKeyspace.
      // Both arrays are filled so that ring[i] == replicaSets[i].range.end,
      // thus allowing to easily locate the range and replicas of a given token.
      Set<TokenRange> ranges = metadata.getTokenRanges();
      ring = new Token[ranges.size()];
      replicaSets = new ReplicaSet[ranges.size()];
      int i = 0;
      Map<Token, TokenRange> rangesByEndingToken =
          ranges.stream().collect(toMap(TokenRange::getEnd, identity()));
      for (TokenRange r1 : ranges) {
        ring[i] = r1.getStart();
        TokenRange r2 = rangesByEndingToken.get(r1.getStart());
        replicaSets[i] = new ReplicaSet(r2, metadata.getReplicas(keyspace, r2));
        i++;
      }
      // 'allTokenRanges' and 'allAddresses' are sorted structures that will only serve when
      // printing final totals.
      if (countRanges) {
        allTokenRanges = new TreeSet<>(ranges);
      } else {
        allTokenRanges = null;
      }
      if (countHosts) {
        allAddresses = new TreeSet<>(Comparator.comparing(InetSocketAddress::toString));
        metadata.getAllHosts().stream().map(Host::getSocketAddress).forEach(allAddresses::add);
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
    totalsByHost = new HashMap<>();
    totalsByPartitionKey = new ArrayList<>();
    for (DefaultCountingUnit unit : units) {
      unit.close();
      totalRows += unit.total;
      unit.totalsByHost.forEach((key, value) -> totalsByHost.merge(key, value, SUM));
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
    if (countHosts) {
      if (multiCount) {
        out.println("Total rows per host:");
      }
      allAddresses.forEach(
          host -> {
            long totalPerHost = totalsByHost.containsKey(host) ? totalsByHost.get(host) : 0;
            float percentage = (float) totalPerHost / (float) totalRows * 100f;
            out.printf("%s %d %.2f%n", host, totalPerHost, percentage);
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
                "%s %s %d %.2f%n", range.getStart(), range.getEnd(), totalPerRange, percentage);
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
    final Map<InetSocketAddress, Long> totalsByHost = new HashMap<>();
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
        if (countRanges || countHosts) {
          // compute the token client-side from the partition keys
          token = metadata.newToken(bbs);
        }
        pk = new PartitionKey(row.getColumnDefinitions(), bbs);
      } else if (countRanges || countHosts) {
        // When counting hosts or ranges, without counting partitions,
        // the result set is expected to contain one single column containing
        // the partition key's token
        token = row.getToken(0);
      }
      // Then increment required counters.
      // Note: we need to always increment the global counter because it's used to compute
      // percentages for other stats.
      total++;
      if (countRanges || countHosts) {
        ReplicaSet replicaSet = getReplicaSet(token);
        if (countRanges) {
          totalsByRange.merge(replicaSet.range, 1L, SUM);
        }
        if (countHosts) {
          for (InetSocketAddress address : replicaSet.addresses) {
            totalsByHost.merge(address, 1L, SUM);
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
     * or hosts.
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
        types[i] = definitions.getType(i);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
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
        @SuppressWarnings("unchecked")
        TypeCodec<Object> codec = (TypeCodec<Object>) codecRegistry.codecFor(types[i]);
        Object o = codec.deserialize(components[i], protocolVersion);
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
    public int compareTo(@NotNull DefaultReadResultCounter.PartitionKeyCount that) {
      return Long.compare(this.count, that.count);
    }
  }

  private static class ReplicaSet {

    final TokenRange range;
    final InetSocketAddress[] addresses;

    ReplicaSet(TokenRange range, Set<Host> addresses) {
      this.range = range;
      this.addresses =
          addresses.stream().map(Host::getSocketAddress).toArray(InetSocketAddress[]::new);
    }
  }
}
