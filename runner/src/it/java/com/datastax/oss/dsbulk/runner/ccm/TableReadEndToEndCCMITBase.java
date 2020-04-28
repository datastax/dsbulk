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
package com.datastax.oss.dsbulk.runner.ccm;

import static com.datastax.oss.driver.api.core.DefaultProtocolVersion.V4;
import static com.datastax.oss.driver.internal.core.util.RoutingKey.compose;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_OK;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.assertStatus;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_MINUTE;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.commons.utils.TokenUtils;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus;
import com.datastax.oss.dsbulk.runner.tests.MockConnector;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMRequirements;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import com.datastax.oss.dsbulk.tests.utils.CQLUtils;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import com.datastax.oss.dsbulk.tests.utils.Version;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;

// restrict the matrix to avoid utilizing too many resources on CI
@CCMRequirements(
    compatibleTypes = {Type.OSS, Type.DSE},
    versionRequirements = {
      @CCMVersionRequirement(type = Type.DSE, min = "6.0"),
      @CCMVersionRequirement(type = Type.OSS, min = "3.11")
    })
abstract class TableReadEndToEndCCMITBase extends EndToEndCCMITBase {

  private static final Version V3 = Version.parse("3.0");

  private final LogInterceptor logs;
  private final StreamInterceptor stdout;
  private final StreamInterceptor stderr;

  private AtomicInteger records;
  private int expectedTotal;
  private Map<String, Map<String, Map<TokenRange, Integer>>> allRanges;
  private Map<String, Map<String, Map<Node, Integer>>> allNodes;
  private Map<String, Map<String, Map<String, Integer>>> allBiggestPartitions;

  TableReadEndToEndCCMITBase(
      CCMCluster ccm,
      CqlSession session,
      LogInterceptor logs,
      StreamInterceptor stdout,
      StreamInterceptor stderr) {
    super(ccm, session);
    this.logs = logs;
    this.stdout = stdout;
    this.stderr = stderr;
  }

  @ParameterizedTest(name = "[{index}] unload keyspace {0} table {1}")
  @CsvSource({
    "RF_1,SINGLE_PK",
    "RF_1,composite_pk",
    "rf_2,SINGLE_PK",
    "rf_2,composite_pk",
    "rf_3,SINGLE_PK",
    "rf_3,composite_pk",
  })
  void full_unload(String keyspace, String table) {

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.keyspace");
    args.add(keyspace);
    args.add("--schema.table");
    args.add(table);

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    assertUnload();
  }

  @ParameterizedTest(name = "[{index}] unload keyspace {0} table {1}")
  @CsvSource({
    "RF_1,SINGLE_PK",
    "RF_1,composite_pk",
    "rf_2,SINGLE_PK",
    "rf_2,composite_pk",
    "rf_3,SINGLE_PK",
    "rf_3,composite_pk",
  })
  void full_unload_materialized_view(String keyspace, String table) {

    assumeTrue(ccm.getCassandraVersion().compareTo(V3) >= 0, "Materialized views require C* 3.0+");

    if (table.toLowerCase().contains("single")) {
      session.execute(
          String.format(
              "CREATE MATERIALIZED VIEW IF NOT EXISTS \"%1$s\".\"%2$s_mv\" AS "
                  + "SELECT pk, cc FROM \"%1$s\".\"%2$s\" WHERE cc IS NOT NULL "
                  + "PRIMARY KEY (pk, cc)",
              keyspace, table));
    } else {
      session.execute(
          String.format(
              "CREATE MATERIALIZED VIEW IF NOT EXISTS \"%1$s\".\"%2$s_mv\" AS "
                  + "SELECT \"PK1\", \"PK2\", cc FROM \"%1$s\".\"%2$s\" WHERE cc IS NOT NULL AND \"PK1\" IS NOT NULL AND \"PK2\" IS NOT NULL "
                  + "PRIMARY KEY ((\"PK1\", \"PK2\"), cc)",
              keyspace, table));
    }

    await()
        .atMost(ONE_MINUTE)
        .until(
            () ->
                session
                        .execute(
                            String.format("SELECT * FROM \"%1$s\".\"%2$s_mv\"", keyspace, table))
                        .all()
                        .size()
                    == expectedTotal);

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.keyspace");
    args.add(keyspace);
    args.add("--schema.table");
    args.add(table + "_mv");

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    assertUnload();
  }

  @ParameterizedTest(name = "[{index}] unload keyspace {0} table {1} (custom query)")
  @CsvSource({
    "RF_1,SINGLE_PK",
    "RF_1,composite_pk",
    "rf_2,SINGLE_PK",
    "rf_2,composite_pk",
    "rf_3,SINGLE_PK",
    "rf_3,composite_pk",
  })
  void full_unload_custom_query(String keyspace, String table) {

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.query");
    args.add(StringUtils.quoteJson(String.format("SELECT * FROM \"%s\".\"%s\"", keyspace, table)));

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    assertUnload();
  }

  @ParameterizedTest(name = "[{index}] unload keyspace {0} table {1} (custom result set)")
  @CsvSource({
    "RF_1,SINGLE_PK",
    "RF_1,composite_pk",
    "rf_2,SINGLE_PK",
    "rf_2,composite_pk",
    "rf_3,SINGLE_PK",
    "rf_3,composite_pk",
  })
  void full_unload_custom_result_set(String keyspace, String table) {

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.query");
    if (table.equals("SINGLE_PK")) {
      args.add(
          StringUtils.quoteJson(
              String.format(
                  "SELECT pk, ttl(v), writetime(v), token(pk), now() FROM \"%s\".\"%s\"",
                  keyspace, table)));
    } else {
      args.add(
          StringUtils.quoteJson(
              String.format(
                  "SELECT \"PK1\", ttl(v), writetime(v), token(\"PK1\", \"PK2\"), now() FROM \"%s\".\"%s\"",
                  keyspace, table)));
    }

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    assertUnload();
  }

  @ParameterizedTest(name = "[{index}] count keyspace {0} table {1} modes {2}")
  @ArgumentsSource(CountWorkflowArgumentsProvider.class)
  void full_count(String keyspace, String table, Set<String> modes) {

    List<String> args = new ArrayList<>();
    args.add("count");
    args.add("-stats");
    args.add(String.join(",", modes));
    args.add("--schema.keyspace");
    args.add(keyspace);
    args.add("--schema.table");
    args.add(table);

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    assertCount(keyspace, table, modes);
  }

  @ParameterizedTest(name = "[{index}] count keyspace {0} table {1} modes {2} (custom query)")
  @ArgumentsSource(CountWorkflowCustomQueryArgumentsProvider.class)
  void full_count_custom_query(String keyspace, String table, Set<String> modes) {

    List<String> args = new ArrayList<>();
    args.add("count");
    args.add("-stats");
    args.add(String.join(",", modes));
    args.add("--schema.query");
    args.add(StringUtils.quoteJson(String.format("SELECT * FROM \"%s\".\"%s\"", keyspace, table)));

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    assertCount(keyspace, table, modes);
  }

  private void assertUnload() {
    assertThat(logs).hasMessageContaining(String.format("Reads: total: %,d", expectedTotal));
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    assertThat(records).hasValue(expectedTotal);
  }

  private void assertCount(String keyspace, String table, Set<String> modes) {
    assertThat(logs).hasMessageContaining(String.format("Reads: total: %,d", expectedTotal));
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    List<String> lines = stdout.getStreamLines();
    if (modes.contains("global")) {
      assertThat(lines).contains(Integer.toString(expectedTotal));
    }
    if (modes.contains("ranges")) {
      Map<TokenRange, Integer> ranges = allRanges.get(keyspace).get(table);
      for (Map.Entry<TokenRange, Integer> entry : ranges.entrySet()) {
        assertThat(lines)
            .anyMatch(
                line ->
                    line.startsWith(
                        String.format(
                            "%s %s %s",
                            TokenUtils.getTokenValue(entry.getKey().getStart()),
                            TokenUtils.getTokenValue(entry.getKey().getEnd()),
                            entry.getValue())));
      }
    }
    if (modes.contains("hosts")) {
      Map<Node, Integer> hosts = allNodes.get(keyspace).get(table);
      for (Map.Entry<Node, Integer> entry : hosts.entrySet()) {
        assertThat(lines)
            .anyMatch(
                line -> {
                  EndPoint endPoint = entry.getKey().getEndPoint();
                  InetSocketAddress addr = (InetSocketAddress) endPoint.resolve();
                  String ip = addr.getAddress().getHostAddress();
                  int port = addr.getPort();
                  // Sometimes the end point resolves as "127.0.0.2/127.0.0.2:9042", sometimes
                  // as just "/127.0.0.2:9042"
                  return line.contains(String.format("%s:%d %s", ip, port, entry.getValue()));
                });
      }
    }
    if (modes.contains("partitions")) {
      Map<String, Integer> biggestPartitions = allBiggestPartitions.get(keyspace).get(table);
      for (Map.Entry<String, Integer> entry : biggestPartitions.entrySet()) {
        assertThat(lines)
            .anyMatch(
                line -> line.startsWith(String.format("%s %s", entry.getKey(), entry.getValue())));
      }
    }
  }

  @BeforeAll
  void createKeyspacesAndTables() {
    session.execute(CQLUtils.createKeyspaceSimpleStrategy("RF_1", 1));
    session.execute(CQLUtils.createKeyspaceSimpleStrategy("rf_2", 2));
    session.execute(CQLUtils.createKeyspaceSimpleStrategy("rf_3", 3));

    session.execute(
        "CREATE TABLE \"RF_1\".\"SINGLE_PK\" (pk int, cc int, v int, PRIMARY KEY (pk, cc))");
    session.execute(
        "CREATE TABLE rf_2.\"SINGLE_PK\" (pk int, cc int, v int, PRIMARY KEY (pk, cc))");
    session.execute(
        "CREATE TABLE rf_3.\"SINGLE_PK\" (pk int, cc int, v int, PRIMARY KEY (pk, cc))");

    session.execute(
        "CREATE TABLE \"RF_1\".composite_pk (\"PK1\" int, \"PK2\" int, cc int, v int, PRIMARY KEY ((\"PK1\", \"PK2\"), cc))");
    session.execute(
        "CREATE TABLE rf_2.composite_pk (\"PK1\" int, \"PK2\" int, cc int, v int, PRIMARY KEY ((\"PK1\", \"PK2\"), cc))");
    session.execute(
        "CREATE TABLE rf_3.composite_pk (\"PK1\" int, \"PK2\" int, cc int, v int, PRIMARY KEY ((\"PK1\", \"PK2\"), cc))");

    allRanges = new HashMap<>();
    allNodes = new HashMap<>();
    allBiggestPartitions = new HashMap<>();

    populateSinglePkTable("RF_1");
    populateSinglePkTable("rf_2");
    populateSinglePkTable("rf_3");

    populateCompositePkTable("RF_1");
    populateCompositePkTable("rf_2");
    populateCompositePkTable("rf_3");
  }

  @AfterAll
  void dropKeyspaces() {
    session.execute("DROP KEYSPACE \"RF_1\"");
    session.execute("DROP KEYSPACE rf_2");
    session.execute("DROP KEYSPACE rf_3");
  }

  @BeforeEach
  void setUpConnector() {
    records = MockConnector.mockCountingWrites();
  }

  private void populateSinglePkTable(String keyspace) {
    Map<TokenRange, Integer> ranges = new HashMap<>();
    Map<Node, Integer> nodes = new HashMap<>();
    Metadata metadata = session.getMetadata();
    expectedTotal = 0;
    for (int pk = 0; pk < 100; pk++) {
      for (int cc = 0; cc < 100; cc++) {
        insertIntoSinglePkTable(keyspace, ranges, nodes, metadata, pk, cc);
        expectedTotal++;
      }
    }
    Map<String, Integer> biggestPartitions = new TreeMap<>();
    for (int pk = 100; pk < 110; pk++) {
      int cc = 0;
      for (; cc < pk + 1; cc++) {
        insertIntoSinglePkTable(keyspace, ranges, nodes, metadata, pk, cc);
        expectedTotal++;
      }
      biggestPartitions.put(Integer.toString(pk), cc);
    }
    allBiggestPartitions
        .computeIfAbsent(keyspace, k -> new HashMap<>())
        .put("SINGLE_PK", biggestPartitions);
    allRanges.computeIfAbsent(keyspace, k -> new HashMap<>()).put("SINGLE_PK", ranges);
    allNodes.computeIfAbsent(keyspace, k -> new HashMap<>()).put("SINGLE_PK", nodes);
  }

  private void populateCompositePkTable(String keyspace) {
    Map<TokenRange, Integer> ranges = new HashMap<>();
    Map<Node, Integer> nodes = new HashMap<>();
    Metadata metadata = session.getMetadata();
    expectedTotal = 0;
    for (int pk1 = 0; pk1 < 10; pk1++) {
      for (int pk2 = 0; pk2 < 10; pk2++) {
        for (int cc = 0; cc < 100; cc++) {
          insertIntoCompositePkTable(keyspace, ranges, nodes, metadata, pk1, pk2, cc);
          expectedTotal++;
        }
      }
    }
    Map<String, Integer> biggestPartitions = new TreeMap<>();
    for (int pk1 = 10; pk1 < 20; pk1++) {
      int cc = 0;
      for (; cc < 91 + pk1; cc++) {
        insertIntoCompositePkTable(keyspace, ranges, nodes, metadata, pk1, 0, cc);
        expectedTotal++;
      }
      biggestPartitions.put(pk1 + "|0", cc);
    }
    allBiggestPartitions
        .computeIfAbsent(keyspace, k -> new HashMap<>())
        .put("composite_pk", biggestPartitions);
    allRanges.computeIfAbsent(keyspace, k -> new HashMap<>()).put("composite_pk", ranges);
    allNodes.computeIfAbsent(keyspace, k -> new HashMap<>()).put("composite_pk", nodes);
  }

  private void insertIntoSinglePkTable(
      String keyspace,
      Map<TokenRange, Integer> ranges,
      Map<Node, Integer> nodes,
      Metadata metadata,
      int i,
      int j) {
    ByteBuffer bb1 = TypeCodecs.INT.encode(i, V4);
    TokenMap tokenMap = metadata.getTokenMap().get();
    Set<Node> replicas = tokenMap.getReplicas(keyspace, bb1);
    for (Node replica : replicas) {
      nodes.compute(replica, (r, t) -> t == null ? 1 : t + 1);
    }
    Token token = tokenMap.newToken(bb1);
    TokenRange range =
        tokenMap.getTokenRanges().stream().filter(r -> r.contains(token)).findFirst().orElse(null);
    ranges.compute(range, (r, t) -> t == null ? 1 : t + 1);
    session.execute(
        String.format(
            "INSERT INTO \"%s\".\"SINGLE_PK\" (pk, cc, v) VALUES (%d,%d,42)", keyspace, i, j));
  }

  private void insertIntoCompositePkTable(
      String keyspace,
      Map<TokenRange, Integer> ranges,
      Map<Node, Integer> nodes,
      Metadata metadata,
      int i,
      int j,
      int k) {
    ByteBuffer bb1 = TypeCodecs.INT.encode(i, V4);
    ByteBuffer bb2 = TypeCodecs.INT.encode(j, V4);
    TokenMap tokenMap = metadata.getTokenMap().get();
    Set<Node> replicas = tokenMap.getReplicas(keyspace, compose(bb1, bb2));
    for (Node replica : replicas) {
      nodes.compute(replica, (r, t) -> t == null ? 1 : t + 1);
    }
    Token token = tokenMap.newToken(bb1, bb2);
    TokenRange range =
        tokenMap.getTokenRanges().stream().filter(r -> r.contains(token)).findFirst().orElse(null);
    ranges.compute(range, (r, t) -> t == null ? 1 : t + 1);
    session.execute(
        String.format(
            "INSERT INTO \"%s\".composite_pk (\"PK1\", \"PK2\", cc, v) VALUES (%d,%d,%d,42)",
            keyspace, i, j, k));
  }

  private static class CountWorkflowArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      List<String> tables = Lists.newArrayList("SINGLE_PK", "composite_pk");
      List<String> keyspaces = Lists.newArrayList("RF_1", "rf_2", "rf_3");
      List<Set<String>> modes =
          Lists.newArrayList(
              ImmutableSet.of("global"),
              ImmutableSet.of("hosts"),
              ImmutableSet.of("ranges"),
              ImmutableSet.of("partitions"),
              ImmutableSet.of("global", "hosts"),
              ImmutableSet.of("global", "partitions"),
              ImmutableSet.of("global", "hosts", "partitions"),
              ImmutableSet.of("global", "hosts", "ranges", "partitions"));
      List<Arguments> args = new ArrayList<>();
      for (String keyspace : keyspaces) {
        for (String table : tables) {
          for (Set<String> mode : modes) {
            args.add(Arguments.of(keyspace, table, mode));
          }
        }
      }
      return args.stream();
    }
  }

  private static class CountWorkflowCustomQueryArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      List<String> tables = Lists.newArrayList("SINGLE_PK", "composite_pk");
      List<String> keyspaces = Lists.newArrayList("RF_1", "rf_2", "rf_3");
      List<Arguments> args = new ArrayList<>();
      for (String keyspace : keyspaces) {
        for (String table : tables) {
          args.add(Arguments.of(keyspace, table, ImmutableSet.of("global")));
        }
      }
      return args.stream();
    }
  }
}
