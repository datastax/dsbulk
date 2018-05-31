/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.settings.LogSettings.OPERATION_DIRECTORY_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newBufferedWriter;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.LongAdder;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultReadResultCounter implements ReadResultCounter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultReadResultCounter.class);

  @VisibleForTesting final LongAdder total = new LongAdder();
  @VisibleForTesting final Map<TokenRange, LongAdder> totalsByRange = new NonBlockingHashMap<>();

  @VisibleForTesting
  final Map<InetSocketAddress, LongAdder> totalsByHost = new NonBlockingHashMap<>();

  private final Set<TokenRange> ranges;
  private final Set<InetSocketAddress> hosts;
  private final Token[] ring;
  private final ReplicaSet[] replicaSets;

  public DefaultReadResultCounter(String keyspace, Metadata metadata) {
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
    for (TokenRange r1 : ranges) {
      ring[i] = r1.getStart();
      for (TokenRange r2 : ranges) {
        if (r2.getEnd().equals(r1.getStart())) {
          replicaSets[i] = new ReplicaSet(r2, metadata.getReplicas(keyspace, r2));
          break;
        }
      }
      i++;
    }
    // these will only serve when printing final totals
    this.ranges = new TreeSet<>(ranges);
    hosts = new TreeSet<>(Comparator.comparing(InetSocketAddress::toString));
    metadata.getAllHosts().stream().map(Host::getSocketAddress).forEach(hosts::add);
  }

  @Override
  public void update(ReadResult result) {
    Row row = result.getRow().orElseThrow(IllegalStateException::new);
    Token token = row.getToken(0);
    total.increment();
    ReplicaSet replicaSet = getReplicaSet(token);
    update(replicaSet.range);
    for (InetSocketAddress host : replicaSet.hosts) {
      update(host);
    }
  }

  @Override
  public void reportTotals() throws IOException {
    Path executionDirectory = Paths.get(System.getProperty(OPERATION_DIRECTORY_KEY));
    Path rowsPerRange = executionDirectory.resolve("rows-per-range.csv");
    Path rowsPerHost = executionDirectory.resolve("rows-per-host.csv");
    long totalRows = total.sum();
    LOGGER.info(String.format("Total rows in table: %,d", totalRows));
    if (totalRows > 0) {
      try (PrintWriter rangeWriter = new PrintWriter(newBufferedWriter(rowsPerRange, UTF_8))) {
        ranges.forEach(
            range -> {
              long totalPerRange =
                  totalsByRange.containsKey(range) ? totalsByRange.get(range).sum() : 0;
              float percentage = (float) totalPerRange / (float) totalRows * 100f;
              rangeWriter.printf(
                  "%s\t%s\t%d\t%.2f%n",
                  range.getStart(), range.getEnd(), totalPerRange, percentage);
            });
      }
      try (PrintWriter hostWriter = new PrintWriter(newBufferedWriter(rowsPerHost, UTF_8))) {
        hosts.forEach(
            host -> {
              long totalPerHost = totalsByHost.containsKey(host) ? totalsByHost.get(host).sum() : 0;
              float percentage = (float) totalPerHost / (float) totalRows * 100f;
              hostWriter.printf("%s\t%d\t%.2f%n", host, totalPerHost, percentage);
            });
      }
      LOGGER.info("Totals per range can be found in " + rowsPerRange);
      LOGGER.info("Totals per host can be found in " + rowsPerHost);
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

  private void update(InetSocketAddress host) {
    update(totalsByHost, host);
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
    final InetSocketAddress[] hosts;

    ReplicaSet(TokenRange range, Set<Host> hosts) {
      this.range = range;
      this.hosts = hosts.stream().map(Host::getSocketAddress).toArray(InetSocketAddress[]::new);
    }
  }
}
