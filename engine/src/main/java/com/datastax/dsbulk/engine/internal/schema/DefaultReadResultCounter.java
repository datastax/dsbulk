/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.settings.EngineSettings.StatisticsMode.all;
import static com.datastax.dsbulk.engine.internal.settings.EngineSettings.StatisticsMode.global;
import static com.datastax.dsbulk.engine.internal.settings.EngineSettings.StatisticsMode.hosts;
import static com.datastax.dsbulk.engine.internal.settings.EngineSettings.StatisticsMode.ranges;
import static com.google.common.base.Functions.identity;
import static java.util.stream.Collectors.toMap;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.engine.internal.settings.EngineSettings;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.annotations.VisibleForTesting;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.LongAdder;
import org.jctools.maps.NonBlockingHashMap;

public class DefaultReadResultCounter implements ReadResultCounter {

  @VisibleForTesting final LongAdder total = new LongAdder();
  @VisibleForTesting final Map<TokenRange, LongAdder> totalsByRange = new NonBlockingHashMap<>();

  @VisibleForTesting
  final Map<InetSocketAddress, LongAdder> totalsByHost = new NonBlockingHashMap<>();

  private final Set<TokenRange> tokenRanges;
  private final Set<InetSocketAddress> addresses;
  private final Token[] ring;
  private final ReplicaSet[] replicaSets;
  private final EngineSettings.StatisticsMode statisticsMode;

  public DefaultReadResultCounter(
      String keyspace, Metadata metadata, EngineSettings.StatisticsMode statisticsMode) {
    this.statisticsMode = statisticsMode;
    switch (statisticsMode) {
      case global:
        tokenRanges = null;
        addresses = null;
        ring = null;
        replicaSets = null;
        break;
      case ranges:
      case hosts:
      case all:
      default:
        // Store required metadata in two data structures that will speed up lookups by token:
        // 1) ring stores the range start tokens of all ranges, contents are identical to
        // metadata.tokenMap.ring and are designed to allow binary searches by token.
        // 2) replicaSets stores a) a given range and b) all of its replicas for the given keyspace,
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
        // these will only serve when printing final totals
        this.tokenRanges = new TreeSet<>(ranges);
        addresses = new TreeSet<>(Comparator.comparing(InetSocketAddress::toString));
        metadata.getAllHosts().stream().map(Host::getSocketAddress).forEach(addresses::add);
        break;
    }
  }

  @Override
  public void update(ReadResult result) {
    Row row = result.getRow().orElseThrow(IllegalStateException::new);
    Token token = row.getToken(0);
    if (statisticsMode == all || statisticsMode == global) {
      total.increment();
    }
    if (statisticsMode == all || statisticsMode == ranges || statisticsMode == hosts) {
      ReplicaSet replicaSet = getReplicaSet(token);
      if (statisticsMode == all || statisticsMode == ranges) {
        update(replicaSet.range);
      }
      if (statisticsMode == all || statisticsMode == hosts) {
        for (InetSocketAddress address : replicaSet.addresses) {
          update(address);
        }
      }
    }
  }

  @Override
  public void reportTotals() {
    long totalRows = total.sum();
    PrintStream out = System.out;
    if (statisticsMode == all || statisticsMode == global) {
      out.printf("Total rows in table: %,d%n", totalRows);
    }
    if (statisticsMode == all || statisticsMode == hosts) {
      addresses.forEach(
          host -> {
            long totalPerHost = totalsByHost.containsKey(host) ? totalsByHost.get(host).sum() : 0;
            float percentage = (float) totalPerHost / (float) totalRows * 100f;
            out.printf("%s\t%d\t%.2f%n", host, totalPerHost, percentage);
          });
    }
    if (statisticsMode == all || statisticsMode == ranges) {
      tokenRanges.forEach(
          range -> {
            long totalPerRange =
                totalsByRange.containsKey(range) ? totalsByRange.get(range).sum() : 0;
            float percentage = (float) totalPerRange / (float) totalRows * 100f;
            out.printf(
                "%s\t%s\t%d\t%.2f%n", range.getStart(), range.getEnd(), totalPerRange, percentage);
          });
    }
  }

  private ReplicaSet getReplicaSet(Token token) {
    // Locate the end token of the range containing the given token
    // then use it to lookup the entire range and its replicas.
    // This search is identical to the search performed by
    // Metadata.TokenMap.getReplicas(String keyspace, Token token).
    int i = Arrays.binarySearch(ring, token);
    if (i < 0) {
      i = -i - 1;
      if (i >= ring.length) {
        i = 0;
      }
    }
    return replicaSets[i];
  }

  private void update(InetSocketAddress address) {
    update(totalsByHost, address);
  }

  private void update(TokenRange range) {
    update(totalsByRange, range);
  }

  private static <T> void update(Map<T, LongAdder> map, T key) {
    map.compute(
        key,
        (k, total) -> {
          if (total == null) {
            total = new LongAdder();
          }
          total.increment();
          return total;
        });
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
