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
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.nio.file.Files.createTempDirectory;

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
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.tests.MockConnector;
import com.google.common.reflect.TypeToken;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@Tag("ccm")
abstract class TableReadEndToEndCCMITBase extends EndToEndCCMITBase {

  private final LogInterceptor logs;
  private final StreamInterceptor stdout;

  private Path logDir;
  private Map<String, Map<String, Map<TokenRange, Integer>>> allRanges;
  private Map<String, Map<String, Map<Host, Integer>>> allHosts;
  private List<Record> records;

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
    "rf_3,composite_pk",
    "rf_3,SINGLE_PK",
    "rf_3,composite_pk",
  })
  void full_unload(String keyspace, String table) {

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("--schema.keyspace");
    args.add(keyspace);
    args.add("--schema.table");
    args.add(table);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertUnload();
  }

  @ParameterizedTest(name = "[{index}] unload keyspace {0} table {1} (custom query)")
  @CsvSource({
    "RF_1,SINGLE_PK",
    "RF_1,composite_pk",
    "rf_2,SINGLE_PK",
    "rf_3,composite_pk",
    "rf_3,SINGLE_PK",
    "rf_3,composite_pk",
  })
  void full_unload_custom_query(String keyspace, String table) {

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("--schema.query");
    args.add(String.format("\"SELECT * FROM \\\"%s\\\".\\\"%s\\\"\"", keyspace, table));
    args.add(table);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertUnload();
  }

  @ParameterizedTest(name = "[{index}] count keyspace {0} table {1}")
  @CsvSource({
    "RF_1,SINGLE_PK",
    "RF_1,composite_pk",
    "rf_2,SINGLE_PK",
    "rf_3,composite_pk",
    "rf_3,SINGLE_PK",
    "rf_3,composite_pk",
  })
  void full_count(String keyspace, String table) {

    List<String> args = new ArrayList<>();
    args.add("count");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("-stats");
    args.add("all");
    args.add("--schema.keyspace");
    args.add(keyspace);
    args.add("--schema.table");
    args.add(table);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertCount(keyspace, table);
  }

  @ParameterizedTest(name = "[{index}] count keyspace {0} table {1} (custom query)")
  @CsvSource({
    "RF_1,SINGLE_PK",
    "RF_1,composite_pk",
    "rf_2,SINGLE_PK",
    "rf_3,composite_pk",
    "rf_3,SINGLE_PK",
    "rf_3,composite_pk",
  })
  void full_count_custom_query(String keyspace, String table) {

    List<String> args = new ArrayList<>();
    args.add("count");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("-stats");
    args.add("all");
    args.add("--schema.query");
    if (table.equals("SINGLE_PK")) {
      args.add(String.format("\"SELECT pk FROM \\\"%s\\\".\\\"%s\\\"\"", keyspace, table));
    } else {
      args.add(String.format("\"SELECT pk1,pk2 FROM \\\"%s\\\".\\\"%s\\\"\"", keyspace, table));
    }

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertCount(keyspace, table);
  }

  private void assertUnload() {
    assertThat(logs).hasMessageContaining("Reads: total: 10,000");
    assertThat(records).hasSize(10000);
  }

  private void assertCount(String keyspace, String table) {
    assertThat(logs).hasMessageContaining("Reads: total: 10,000");
    assertThat(stdout.getStreamAsString()).contains("Total rows in table: 10,000");
    Map<TokenRange, Integer> ranges = allRanges.get(keyspace).get(table);
    Map<Host, Integer> hosts = allHosts.get(keyspace).get(table);
    List<String> lines = stdout.getStreamLines();
    for (Map.Entry<TokenRange, Integer> entry : ranges.entrySet()) {
      assertThat(lines)
          .anyMatch(
              line ->
                  line.startsWith(
                      String.format(
                          "%s %s %s",
                          entry.getKey().getStart(), entry.getKey().getEnd(), entry.getValue())));
    }
    for (Map.Entry<Host, Integer> entry : hosts.entrySet()) {
      assertThat(lines)
          .anyMatch(
              line -> line.startsWith(String.format("%s %s", entry.getKey(), entry.getValue())));
    }
  }

  @BeforeAll
  void createKeyspacesAndTables() {
    session.execute(createKeyspaceSimpleStrategy("RF_1", 1));
    session.execute(createKeyspaceSimpleStrategy("rf_2", 2));
    session.execute(createKeyspaceSimpleStrategy("rf_3", 3));

    session.execute("CREATE TABLE \"RF_1\".\"SINGLE_PK\" (pk int PRIMARY KEY, v int)");
    session.execute("CREATE TABLE rf_2.\"SINGLE_PK\" (pk int PRIMARY KEY, v int)");
    session.execute("CREATE TABLE rf_3.\"SINGLE_PK\" (pk int PRIMARY KEY, v int)");

    session.execute(
        "CREATE TABLE \"RF_1\".composite_pk (pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2)))");
    session.execute(
        "CREATE TABLE rf_2.composite_pk (pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2)))");
    session.execute(
        "CREATE TABLE rf_3.composite_pk (pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2)))");

    allRanges = new HashMap<>();
    allHosts = new HashMap<>();

    populateSinglePkTable("RF_1");
    populateSinglePkTable("rf_2");
    populateSinglePkTable("rf_3");

    populateCompositePkTable("RF_1");
    populateCompositePkTable("rf_2");
    populateCompositePkTable("rf_3");
  }

  @AfterAll
  void dropKeyspaces() {
    session.execute("DROP KEYSPACE IF EXISTS \"RF_1\"");
    session.execute("DROP KEYSPACE IF EXISTS rf_2");
    session.execute("DROP KEYSPACE IF EXISTS rf_3");
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
  }

  @BeforeEach
  void setUpConnector() {
    records = new ArrayList<>();
    MockConnector.setDelegate(
        new Connector() {

          @Override
          public RecordMetadata getRecordMetadata() {
            return (field, cqlType) -> TypeToken.of(Integer.class);
          }

          @Override
          public Supplier<? extends Publisher<Record>> read() {
            // not used
            return null;
          }

          @Override
          public Supplier<? extends Publisher<Publisher<Record>>> readByResource() {
            // not used
            return null;
          }

          @Override
          public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
            return upstream -> Flux.from(upstream).doOnNext(record -> records.add(record));
          }
        });
  }

  private void populateSinglePkTable(String keyspace) {
    Map<TokenRange, Integer> ranges = new HashMap<>();
    Map<Host, Integer> hosts = new HashMap<>();
    Metadata metadata = session.getCluster().getMetadata();
    for (int i = 0; i < 10_000; i++) {
      ByteBuffer bb1 = cint().serialize(i, V4);
      Set<Host> replicas = metadata.getReplicas(keyspace, bb1);
      for (Host replica : replicas) {
        hosts.compute(replica, (r, t) -> t == null ? 1 : t + 1);
      }
      Token token = metadata.newToken(bb1);
      TokenRange range =
          metadata
              .getTokenRanges()
              .stream()
              .filter(r -> r.contains(token))
              .findFirst()
              .orElse(null);
      ranges.compute(range, (r, t) -> t == null ? 1 : t + 1);
      session.execute(
          String.format("INSERT INTO \"%s\".\"SINGLE_PK\" (pk, v) VALUES (?, ?)", keyspace), i, 42);
    }
    allRanges.computeIfAbsent(keyspace, k -> new HashMap<>()).put("SINGLE_PK", ranges);
    allHosts.computeIfAbsent(keyspace, k -> new HashMap<>()).put("SINGLE_PK", hosts);
  }

  private void populateCompositePkTable(String keyspace) {
    Map<TokenRange, Integer> ranges = new HashMap<>();
    Map<Host, Integer> hosts = new HashMap<>();
    Metadata metadata = session.getCluster().getMetadata();
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        ByteBuffer bb1 = cint().serialize(i, V4);
        ByteBuffer bb2 = cint().serialize(j, V4);
        Set<Host> replicas = metadata.getReplicas(keyspace, compose(bb1, bb2));
        for (Host replica : replicas) {
          hosts.compute(replica, (r, t) -> t == null ? 1 : t + 1);
        }
        Token token = metadata.newToken(bb1, bb2);
        TokenRange range =
            metadata
                .getTokenRanges()
                .stream()
                .filter(r -> r.contains(token))
                .findFirst()
                .orElse(null);
        ranges.compute(range, (r, t) -> t == null ? 1 : t + 1);
        session.execute(
            String.format(
                "INSERT INTO \"%s\".composite_pk (pk1, pk2, v) VALUES (?, ?, ?)", keyspace),
            i,
            j,
            42);
      }
    }
    allRanges.computeIfAbsent(keyspace, k -> new HashMap<>()).put("composite_pk", ranges);
    allHosts.computeIfAbsent(keyspace, k -> new HashMap<>()).put("composite_pk", hosts);
  }
}
