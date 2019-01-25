/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.driver.core.DriverCoreEngineTestHooks.compose;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.driver.core.TypeCodec.cint;
import static com.datastax.dsbulk.commons.tests.utils.CQLUtils.createKeyspaceSimpleStrategy;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.global;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.hosts;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.partitions;
import static com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode.ranges;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.nio.file.Files.createTempDirectory;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_MINUTE;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.internal.settings.StatsSettings.StatisticsMode;
import com.datastax.dsbulk.engine.tests.MockConnector;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
abstract class TableReadEndToEndCCMITBase extends EndToEndCCMITBase {

  private static final Version V3 = Version.parse("3.0");

  private final LogInterceptor logs;
  private final StreamInterceptor stdout;

  private Path logDir;
  private AtomicInteger records;
  private int expectedTotal;
  private Map<String, Map<String, Map<TokenRange, Integer>>> allRanges;
  private Map<String, Map<String, Map<Host, Integer>>> allHosts;
  private Map<String, Map<String, Map<String, Integer>>> allBiggestPartitions;

  TableReadEndToEndCCMITBase(
      CCMCluster ccm, Session session, LogInterceptor logs, StreamInterceptor stdout) {
    super(ccm, session);
    this.logs = logs;
    this.stdout = stdout;
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
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--schema.keyspace");
    args.add(keyspace);
    args.add("--schema.table");
    args.add(table);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

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
                  + "SELECT cc FROM \"%1$s\".\"%2$s\" WHERE cc IS NOT NULL "
                  + "PRIMARY KEY (pk, cc)",
              keyspace, table));
    } else {
      session.execute(
          String.format(
              "CREATE MATERIALIZED VIEW IF NOT EXISTS \"%1$s\".\"%2$s_mv\" AS "
                  + "SELECT cc FROM \"%1$s\".\"%2$s\" WHERE cc IS NOT NULL AND \"PK1\" IS NOT NULL AND \"PK2\" IS NOT NULL "
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
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--schema.keyspace");
    args.add(keyspace);
    args.add("--schema.table");
    args.add(table + "_mv");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

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
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--schema.query");
    args.add(quoteJson(String.format("SELECT * FROM \"%s\".\"%s\"", keyspace, table)));
    args.add(table);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

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
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--schema.query");
    if (table.equals("SINGLE_PK")) {
      args.add(
          quoteJson(
              String.format(
                  "SELECT pk, ttl(v), writetime(v), token(pk), now() FROM \"%s\".\"%s\"",
                  keyspace, table)));
    } else {
      args.add(
          quoteJson(
              String.format(
                  "SELECT \"PK1\", ttl(v), writetime(v), token(\"PK1\", \"PK2\"), now() FROM \"%s\".\"%s\"",
                  keyspace, table)));
    }
    args.add(table);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertUnload();
  }

  @ParameterizedTest(name = "[{index}] count keyspace {0} table {1} modes {2}")
  @ArgumentsSource(CountWorkflowArgumentsProvider.class)
  void full_count(String keyspace, String table, EnumSet<StatisticsMode> modes) {

    List<String> args = new ArrayList<>();
    args.add("count");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("-stats");
    args.add(modes.stream().map(Enum::name).collect(Collectors.joining(",")));
    args.add("--schema.keyspace");
    args.add(keyspace);
    args.add("--schema.table");
    args.add(table);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertCount(keyspace, table, modes);
  }

  @ParameterizedTest(name = "[{index}] count keyspace {0} table {1} modes {2} (custom query)")
  @ArgumentsSource(CountWorkflowArgumentsProvider.class)
  void full_count_custom_query(String keyspace, String table, EnumSet<StatisticsMode> modes) {

    List<String> args = new ArrayList<>();
    args.add("count");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("-stats");
    args.add(modes.stream().map(Enum::name).collect(Collectors.joining(",")));
    args.add("--schema.query");
    args.add(quoteJson(String.format("SELECT * FROM \"%s\".\"%s\"", keyspace, table)));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertCount(keyspace, table, modes);
  }

  private void assertUnload() {
    assertThat(logs).hasMessageContaining(String.format("Reads: total: %,d", expectedTotal));
    assertThat(records).hasValue(expectedTotal);
  }

  private void assertCount(String keyspace, String table, EnumSet<StatisticsMode> modes) {
    assertThat(logs).hasMessageContaining(String.format("Reads: total: %,d", expectedTotal));
    List<String> lines = stdout.getStreamLines();
    if (modes.contains(global)) {
      assertThat(lines).contains(Integer.toString(expectedTotal));
    }
    if (modes.contains(StatisticsMode.ranges)) {
      Map<TokenRange, Integer> ranges = allRanges.get(keyspace).get(table);
      for (Map.Entry<TokenRange, Integer> entry : ranges.entrySet()) {
        assertThat(lines)
            .anyMatch(
                line ->
                    line.startsWith(
                        String.format(
                            "%s %s %s",
                            entry.getKey().getStart(), entry.getKey().getEnd(), entry.getValue())));
      }
    }
    if (modes.contains(StatisticsMode.hosts)) {
      Map<Host, Integer> hosts = allHosts.get(keyspace).get(table);
      for (Map.Entry<Host, Integer> entry : hosts.entrySet()) {
        assertThat(lines)
            .anyMatch(
                line -> line.startsWith(String.format("%s %s", entry.getKey(), entry.getValue())));
      }
    }
    if (modes.contains(StatisticsMode.partitions)) {
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
    session.execute(createKeyspaceSimpleStrategy("RF_1", 1));
    session.execute(createKeyspaceSimpleStrategy("rf_2", 2));
    session.execute(createKeyspaceSimpleStrategy("rf_3", 3));

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
    allHosts = new HashMap<>();
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
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
  }

  @BeforeEach
  void clearLogs() {
    logs.clear();
    stdout.clear();
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
  }

  @BeforeEach
  void setUpConnector() {
    records = MockConnector.mockCountingWrites();
  }

  private void populateSinglePkTable(String keyspace) {
    Map<TokenRange, Integer> ranges = new HashMap<>();
    Map<Host, Integer> hosts = new HashMap<>();
    Metadata metadata = session.getCluster().getMetadata();
    expectedTotal = 0;
    for (int pk = 0; pk < 100; pk++) {
      for (int cc = 0; cc < 100; cc++) {
        insertIntoSinglePkTable(keyspace, ranges, hosts, metadata, pk, cc);
        expectedTotal++;
      }
    }
    Map<String, Integer> biggestPartitions = new TreeMap<>();
    for (int pk = 100; pk < 110; pk++) {
      int cc = 0;
      for (; cc < pk + 1; cc++) {
        insertIntoSinglePkTable(keyspace, ranges, hosts, metadata, pk, cc);
        expectedTotal++;
      }
      biggestPartitions.put(Integer.toString(pk), cc);
    }
    allBiggestPartitions
        .computeIfAbsent(keyspace, k -> new HashMap<>())
        .put("SINGLE_PK", biggestPartitions);
    allRanges.computeIfAbsent(keyspace, k -> new HashMap<>()).put("SINGLE_PK", ranges);
    allHosts.computeIfAbsent(keyspace, k -> new HashMap<>()).put("SINGLE_PK", hosts);
  }

  private void populateCompositePkTable(String keyspace) {
    Map<TokenRange, Integer> ranges = new HashMap<>();
    Map<Host, Integer> hosts = new HashMap<>();
    Metadata metadata = session.getCluster().getMetadata();
    expectedTotal = 0;
    for (int pk1 = 0; pk1 < 10; pk1++) {
      for (int pk2 = 0; pk2 < 10; pk2++) {
        for (int cc = 0; cc < 100; cc++) {
          insertIntoCompositePkTable(keyspace, ranges, hosts, metadata, pk1, pk2, cc);
          expectedTotal++;
        }
      }
    }
    Map<String, Integer> biggestPartitions = new TreeMap<>();
    for (int pk1 = 10; pk1 < 20; pk1++) {
      int cc = 0;
      for (; cc < 91 + pk1; cc++) {
        insertIntoCompositePkTable(keyspace, ranges, hosts, metadata, pk1, 0, cc);
        expectedTotal++;
      }
      biggestPartitions.put(pk1 + "|0", cc);
    }
    allBiggestPartitions
        .computeIfAbsent(keyspace, k -> new HashMap<>())
        .put("composite_pk", biggestPartitions);
    allRanges.computeIfAbsent(keyspace, k -> new HashMap<>()).put("composite_pk", ranges);
    allHosts.computeIfAbsent(keyspace, k -> new HashMap<>()).put("composite_pk", hosts);
  }

  private void insertIntoSinglePkTable(
      String keyspace,
      Map<TokenRange, Integer> ranges,
      Map<Host, Integer> hosts,
      Metadata metadata,
      int i,
      int j) {
    ByteBuffer bb1 = cint().serialize(i, V4);
    Set<Host> replicas = metadata.getReplicas(keyspace, bb1);
    for (Host replica : replicas) {
      hosts.compute(replica, (r, t) -> t == null ? 1 : t + 1);
    }
    Token token = metadata.newToken(bb1);
    TokenRange range =
        metadata.getTokenRanges().stream().filter(r -> r.contains(token)).findFirst().orElse(null);
    ranges.compute(range, (r, t) -> t == null ? 1 : t + 1);
    session.execute(
        String.format(
            "INSERT INTO \"%s\".\"SINGLE_PK\" (pk, cc, v) VALUES (%d,%d,42)", keyspace, i, j));
  }

  private void insertIntoCompositePkTable(
      String keyspace,
      Map<TokenRange, Integer> ranges,
      Map<Host, Integer> hosts,
      Metadata metadata,
      int i,
      int j,
      int k) {
    ByteBuffer bb1 = cint().serialize(i, V4);
    ByteBuffer bb2 = cint().serialize(j, V4);
    Set<Host> replicas = metadata.getReplicas(keyspace, compose(bb1, bb2));
    for (Host replica : replicas) {
      hosts.compute(replica, (r, t) -> t == null ? 1 : t + 1);
    }
    Token token = metadata.newToken(bb1, bb2);
    TokenRange range =
        metadata.getTokenRanges().stream().filter(r -> r.contains(token)).findFirst().orElse(null);
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
      List<EnumSet<StatisticsMode>> modes =
          Lists.newArrayList(
              EnumSet.of(global),
              EnumSet.of(hosts),
              EnumSet.of(ranges),
              EnumSet.of(partitions),
              EnumSet.of(global, hosts),
              EnumSet.of(global, partitions),
              EnumSet.of(global, hosts, partitions),
              EnumSet.allOf(StatisticsMode.class));
      List<Arguments> args = new ArrayList<>();
      for (String keyspace : keyspaces) {
        for (String table : tables) {
          for (EnumSet<StatisticsMode> mode : modes) {
            args.add(Arguments.of(keyspace, table, mode));
          }
        }
      }
      return args.stream();
    }
  }
}
