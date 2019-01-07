/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DDAC;
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.readAllLinesInDirectoryAsStream;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.readAllLinesInDirectoryAsStreamExcludingHeaders;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.numberToInstant;
import static com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy.REJECT;
import static com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy.TRUNCATE;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_HEADER;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_SKIP;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_WITH_SPACES;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_CASE_SENSITIVE;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createIpByCountryCaseSensitiveTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createIpByCountryTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createWithSpacesTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateBadOps;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateExceptionsLog;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static java.math.RoundingMode.FLOOR;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.file.Files.createTempDirectory;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assumptions.assumingThat;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.engine.tests.MockConnector;
import com.datastax.dsbulk.engine.tests.utils.RecordUtils;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@CCMConfig(numberOfNodes = 1, config = "enable_user_defined_functions:true")
@Tag("medium")
@CCMRequirements(compatibleTypes = {DSE, DDAC})
class CSVConnectorEndToEndCCMIT extends EndToEndCCMITBase {

  private static final Version V3 = Version.parse("3.0");
  private static final Version V2_2 = Version.parse("2.2");
  private static final Version V2_1 = Version.parse("2.1");

  private final LogInterceptor logs;
  private final StreamInterceptor stderr;
  private Path logDir;
  private Path unloadDir;

  CSVConnectorEndToEndCCMIT(
      CCMCluster ccm,
      Session session,
      @LogCapture LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    super(ccm, session);
    this.logs = logs;
    this.stderr = stderr;
  }

  @BeforeAll
  void createTables() {
    createIpByCountryTable(session);
    createWithSpacesTable(session);
    createIpByCountryCaseSensitiveTable(session);
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @BeforeEach
  void truncateTable() {
    session.execute("TRUNCATE ip_by_country");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
    deleteDirectory(unloadDir);
  }

  @BeforeEach
  void clearLogs() {
    logs.clear();
    stderr.clear();
  }

  /** Simple test case which attempts to load and unload data using ccm. */
  @Test
  void full_load_unload() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /** Simple test case which attempts to load and unload data using ccm and compression (LZ4). */
  @Test
  void full_load_unload_lz4() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--driver.protocol.compression");
    args.add("LZ4");
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--driver.protocol.compression");
    args.add("LZ4");
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /** Simple test case which attempts to load and unload data using ccm and compression (Snappy). */
  @Test
  void full_load_unload_snappy() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /**
   * Attempts to load and unload complex types (Collections, UDTs, etc).
   *
   * @jira_ticket DAT-288
   */
  @Test
  void full_load_unload_complex() throws Exception {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V2_1) >= 0, "UDTs are not compatible with C* < 2.1");

    session.execute("DROP TABLE IF EXISTS complex");
    session.execute("DROP TYPE IF EXISTS contacts");

    session.execute(
        "CREATE TYPE contacts ("
            + "f_tuple frozen<tuple<int, text, float, timestamp>>, "
            + "f_list frozen<list<timestamp>>"
            + ")");
    session.execute(
        "CREATE TABLE complex ("
            + "pk int PRIMARY KEY, "
            + "c_text text, "
            + "c_int int, "
            + "c_tuple frozen<tuple<int, text, float, timestamp>>, "
            + "c_map map<timestamp, varchar>,"
            + "c_list list<timestamp>,"
            + "c_set set<varchar>,"
            + "c_udt frozen<contacts>)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(getClass().getResource("/complex.csv")));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--codec.nullStrings");
    args.add("N/A");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("complex");
    args.add("--schema.mapping");
    args.add("pk,c_text,c_int,c_tuple,c_map,c_list,c_set,c_udt");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertComplexRows(session);

    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--codec.nullStrings");
    args.add("N/A");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("complex");
    args.add("--schema.mapping");
    args.add("pk,c_text,c_int,c_tuple,c_map,c_list,c_set,c_udt");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(2, unloadDir);

    args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--codec.nullStrings");
    args.add("N/A");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("complex");
    args.add("--schema.mapping");
    args.add("pk,c_text,c_int,c_tuple,c_map,c_list,c_set,c_udt");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertComplexRows(session);
  }

  static void assertComplexRows(Session session) {
    Instant i1 = Instant.parse("2018-05-25T11:25:00Z");
    Instant i2 = Instant.parse("2018-05-25T11:26:00Z");
    {
      Row row = session.execute("SELECT * FROM complex WHERE pk = 0").one();
      List<Instant> list = row.getList("c_list", Instant.class);
      Set<String> set = row.getSet("c_set", String.class);
      Map<Instant, String> map = row.getMap("c_map", Instant.class, String.class);
      TupleValue tuple = row.getTupleValue("c_tuple");
      UDTValue udt = row.getUDTValue("c_udt");
      assertThat(row.getString("c_text")).isNull();
      assertThat(row.getInt("c_int")).isEqualTo(42);
      assertThat(list).containsOnly(i1, i2);
      assertThat(set).containsOnly("foo", "");
      assertThat(map).hasSize(1).containsEntry(i1, "");
      assertThat(tuple.getInt(0)).isEqualTo(2);
      assertThat(tuple.getString(1)).isEmpty();
      assertThat(tuple.getFloat(2)).isEqualTo(2.7f);
      assertThat(tuple.get(3, Instant.class)).isEqualTo(i1);
      assertThat(udt.getTupleValue("f_tuple")).isEqualTo(tuple);
      assertThat(udt.getList("f_list", Instant.class)).isEqualTo(list);
    }
    {
      Row row = session.execute("SELECT * FROM complex WHERE pk = 1").one();
      assertThat(row.getString("c_text")).isNull();
      assertThat(row.isNull("c_int")).isTrue();
      assertThat(row.getList("c_list", Instant.class)).isEmpty();
      assertThat(row.getSet("c_set", String.class)).isEmpty();
      assertThat(row.getMap("c_map", Instant.class, String.class)).isEmpty();
      assertThat(row.getTupleValue("c_tuple").isNull(0)).isTrue();
      assertThat(row.getTupleValue("c_tuple").getString(1)).isNull();
      assertThat(row.getTupleValue("c_tuple").isNull(2)).isTrue();
      assertThat(row.getTupleValue("c_tuple").get(3, Instant.class)).isNull();
      assertThat(row.getUDTValue("c_udt").getTupleValue("f_tuple")).isNull();
      assertThat(row.getUDTValue("c_udt").getList("f_list", Instant.class)).isEmpty();
    }
  }

  /**
   * Attempts to load and unload counter types.
   *
   * @jira_ticket DAT-292
   */
  @Test
  void full_load_unload_counters() throws Exception {

    session.execute("DROP TABLE IF EXISTS counters");
    session.execute(
        "CREATE TABLE counters ("
            + "pk1 int, "
            + "\"PK2\" int, "
            + "\"C1\" counter, "
            + "c2 counter, "
            + "c3 counter, "
            + "PRIMARY KEY (pk1, \"PK2\"))");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(getClass().getResource("/counters.csv")));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("counters");
    args.add("--schema.mapping");
    args.add("pk1,PK2,C1,c2");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    Row row =
        session.execute("SELECT \"C1\", c2, c3 FROM counters WHERE pk1 = 1 AND \"PK2\" = 2").one();
    assertThat(row.getLong("\"C1\"")).isEqualTo(42L);
    assertThat(row.getLong("c2")).isZero(); // present in the file
    assertThat(row.isNull("c3")).isTrue(); // not present in the file

    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("counters");
    args.add("--schema.mapping");
    args.add("pk1,PK2,C1,c2,c3");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(1, unloadDir);
    assertThat(readAllLinesInDirectoryAsStream(unloadDir)).containsExactly("1,2,42,0,");
  }

  @Test
  void full_load_unload_counters_custom_query_positional() {

    // the UPDATE SET += syntax is only supported in 5.1+
    assumingThat(
        ccm.getVersion().compareTo(Version.parse("5.1")) >= 0,
        () -> {
          session.execute("DROP TABLE IF EXISTS counters");
          session.execute(
              "CREATE TABLE counters ("
                  + "pk1 int, "
                  + "\"PK2\" int, "
                  + "\"C1\" counter, "
                  + "c2 counter, "
                  + "c3 counter, "
                  + "PRIMARY KEY (pk1, \"PK2\"))");

          List<String> args = new ArrayList<>();
          args.add("load");
          args.add("--log.directory");
          args.add(quoteJson(logDir));
          args.add("--connector.csv.url");
          args.add(quoteJson(getClass().getResource("/counters.csv")));
          args.add("--connector.csv.header");
          args.add("false");
          args.add("--schema.keyspace");
          args.add(session.getLoggedKeyspace());
          args.add("--schema.query");
          args.add(
              quoteJson(
                  "UPDATE counters SET \"C1\" += ?, c2 = c2 + ? WHERE pk1 = ? AND \"PK2\" = ?"));
          args.add("--schema.mapping");
          args.add("pk1,PK2,C1,c2");

          int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
          assertThat(status).isZero();
          Row row =
              session
                  .execute("SELECT \"C1\", c2, c3 FROM counters WHERE pk1 = 1 AND \"PK2\" = 2")
                  .one();
          assertThat(row.getLong("\"C1\"")).isEqualTo(42L);
          assertThat(row.getLong("c2")).isZero(); // present in the file
          assertThat(row.isNull("c3")).isTrue(); // not present in the file

          deleteDirectory(logDir);

          args = new ArrayList<>();
          args.add("unload");
          args.add("--log.directory");
          args.add(quoteJson(logDir));
          args.add("--connector.csv.url");
          args.add(quoteJson(unloadDir));
          args.add("--connector.csv.header");
          args.add("true");
          args.add("--connector.csv.maxConcurrentFiles");
          args.add("1");
          args.add("--schema.keyspace");
          args.add(session.getLoggedKeyspace());
          args.add("--schema.query");
          // Exercise aliased selectors and a custom mapping
          args.add(
              quoteJson(
                  "SELECT pk1 as \"Field A\", \"PK2\" AS \"Field B\", \"C1\" AS \"Field C\", "
                      + "c2 AS \"Field D\", c3 AS \"Field E\" FROM counters"));
          args.add("--schema.mapping");
          args.add(quoteJson("\"Field E\",\"Field D\",\"Field C\",\"Field B\",\"Field A\""));

          status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
          assertThat(status).isZero();
          validateOutputFiles(2, unloadDir);
          assertThat(readAllLinesInDirectoryAsStream(unloadDir))
              .containsExactly("Field E,Field D,Field C,Field B,Field A", ",0,42,2,1");
        });
  }

  @Test
  void full_load_unload_counters_custom_query_named() {

    // the UPDATE SET += syntax is only supported in 5.1+
    assumingThat(
        ccm.getVersion().compareTo(Version.parse("5.1")) >= 0,
        () -> {
          session.execute("DROP TABLE IF EXISTS counters");
          session.execute(
              "CREATE TABLE counters ("
                  + "pk1 int, "
                  + "\"PK2\" int, "
                  + "\"C1\" counter, "
                  + "c2 counter, "
                  + "c3 counter, "
                  + "PRIMARY KEY (pk1, \"PK2\"))");

          List<String> args = new ArrayList<>();
          args.add("load");
          args.add("--log.directory");
          args.add(quoteJson(logDir));
          args.add("--connector.csv.url");
          args.add(quoteJson(getClass().getResource("/counters.csv")));
          args.add("--connector.csv.header");
          args.add("false");
          args.add("--schema.keyspace");
          args.add(session.getLoggedKeyspace());
          args.add("--schema.query");
          args.add(
              quoteJson(
                  "UPDATE counters SET \"C1\" += :\"fieldC\", c2 = c2 + :\"fieldD\" WHERE pk1 = :\"fieldA\" AND \"PK2\" = :\"fieldB\""));
          args.add("--schema.mapping");
          args.add("fieldA,fieldB,fieldC,fieldD");

          int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
          assertThat(status).isZero();
          Row row =
              session
                  .execute("SELECT \"C1\", c2, c3 FROM counters WHERE pk1 = 1 AND \"PK2\" = 2")
                  .one();
          assertThat(row.getLong("\"C1\"")).isEqualTo(42L);
          assertThat(row.getLong("c2")).isZero(); // present in the file
          assertThat(row.isNull("c3")).isTrue(); // not present in the file

          deleteDirectory(logDir);

          args = new ArrayList<>();
          args.add("unload");
          args.add("--log.directory");
          args.add(quoteJson(logDir));
          args.add("--connector.csv.url");
          args.add(quoteJson(unloadDir));
          args.add("--connector.csv.header");
          args.add("false");
          args.add("--connector.csv.maxConcurrentFiles");
          args.add("1");
          args.add("--schema.keyspace");
          args.add(session.getLoggedKeyspace());
          args.add("--schema.query");
          args.add(quoteJson("SELECT pk1, \"PK2\", \"C1\", c2, c3 FROM counters"));

          status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
          assertThat(status).isZero();
          validateOutputFiles(1, unloadDir);
          assertThat(readAllLinesInDirectoryAsStream(unloadDir)).containsExactly("1,2,42,0,");
        });
  }

  /** Attempts to load and unload a larger dataset which can be batched. */
  @Test
  void full_load_unload_large_batches() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS));
    args.add("--connector.csv.header");
    args.add("true");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(500, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(500, unloadDir);
  }

  /**
   * Attempt to load and unload data using ccm for a keyspace and table that is case-sensitive, and
   * with a column name containing spaces. The source data also has a header row containing spaces,
   * and the source data contains a multi-line value.
   */
  @Test
  void full_load_unload_with_spaces() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("-url");
    args.add(quoteJson(CSV_RECORDS_WITH_SPACES));
    args.add("--schema.mapping");
    args.add(quoteJson("key=key,\"my source\"=\"my destination\""));
    args.add("-header");
    args.add("true");
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(1, "SELECT * FROM \"MYKS\".\"WITH_SPACES\"");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("-url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.mapping");
    args.add(quoteJson("key=key,\"my source\"=\"my destination\""));
    args.add("-header");
    args.add("true");
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(3, unloadDir);
  }

  /** Attempts to load and unload data, some of which will be unsuccessful. */
  @Test
  void skip_test_load_unload() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_SKIP));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--connector.csv.skipRecords");
    args.add("3");
    args.add("--connector.csv.maxRecords");
    args.add("24");
    args.add("--schema.allowMissingFields");
    args.add("true");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_COMPLETED_WITH_ERRORS);
    validateResultSetSize(21, "SELECT * FROM ip_by_country");
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateBadOps(3, logPath);
    validateExceptionsLog(3, "Source:", "mapping-errors.log", logPath);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(21, unloadDir);
  }

  @Test
  void load_ttl_timestamp_now_in_mapping_and_unload() throws IOException {

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "table_ttl_timestamp",
            "--schema.mapping",
            "*:*,now()=loaded_at,created_at=__timestamp,time_to_live=__ttl");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestamp();
    deleteDirectory(logDir);

    args =
        Lists.newArrayList(
            "unload",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "table_ttl_timestamp",
            "--schema.mapping",
            "*:*,created_at=writetime(value),time_to_live=ttl(value)",
            "--connector.csv.maxConcurrentFiles",
            "1");
    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(2, unloadDir);
  }

  @Test
  void load_ttl_timestamp_now_in_query() {

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) "
                + "values (:key, :value, now()) "
                + "using ttl :time_to_live and timestamp :created_at");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestamp();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_positional_external_names() {

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (?, ?, now()) using ttl ? and timestamp ?",
            "--schema.mapping",
            "*=*, created_at = __timestamp, time_to_live = __ttl");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestamp();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_positional_internal_names() {

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (?, ?, now()) using ttl ? and timestamp ?",
            "--schema.mapping",
            // using internal names directly in the mapping should work too
            quoteJson("*=*, created_at = \"[timestamp]\", time_to_live = \"[ttl]\""));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestamp();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_real_names() {

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (:key, :value, now()) using ttl :t1 and timestamp :t2",
            "--schema.mapping",
            "*=*, created_at = t2, time_to_live = t1");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestamp();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_external_names() {

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (:key, :value, now()) using ttl :t1 and timestamp :t2",
            "--schema.mapping",
            // using __timestamp and __ttl should work too (although not very useful), they should
            // map to t2 and t1 respectively
            "*=*, created_at = __timestamp, time_to_live = __ttl");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestamp();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_with_keyspace_provided_separately() {

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (:key, :value, now()) using ttl :t1 and timestamp :t2",
            "--schema.mapping",
            "*=*, created_at = t2, time_to_live = t1");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestamp();
  }

  private void assertTTLAndTimestamp() {
    assertThat(session.execute("SELECT COUNT(*) FROM table_ttl_timestamp").one().getLong(0))
        .isEqualTo(1L);

    Row row;

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value), loaded_at FROM table_ttl_timestamp WHERE key = 1")
            .one();
    assertThat(row.getInt(0)).isNotZero().isLessThanOrEqualTo(1000);
    assertThat(row.getLong(1))
        .isEqualTo(
            instantToNumber(
                ZonedDateTime.parse("2017-11-29T14:32:15+02:00").toInstant(), MICROSECONDS, EPOCH));
    assertThat(row.getUUID(2)).isNotNull();
  }

  @Test
  void load_ttl_timestamp_now_in_mapping_and_unload_unset_values() throws IOException {

    assumeTrue(
        protocolVersion.compareTo(ProtocolVersion.V4) > 0,
        "Unset values are not compatible with protocol version < 4");

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp-unset.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "table_ttl_timestamp",
            "--schema.mapping",
            "*:*,now()=loaded_at,created_at=__timestamp,time_to_live=__ttl");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestampUnsetValues();
    deleteDirectory(logDir);

    args =
        Lists.newArrayList(
            "unload",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "table_ttl_timestamp",
            "--schema.mapping",
            "*:*,created_at=writetime(value),time_to_live=ttl(value)",
            "--connector.csv.maxConcurrentFiles",
            "1");
    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(3, unloadDir);
  }

  @Test
  void load_ttl_timestamp_now_in_query_unset_values() {

    assumeTrue(
        protocolVersion.compareTo(ProtocolVersion.V4) > 0,
        "Unset values are not compatible with protocol version < 4");

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp-unset.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) "
                + "values (:key, :value, now()) "
                + "using ttl :time_to_live and timestamp :created_at");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestampUnsetValues();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_positional_external_names_unset_values() {

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp-unset.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (?, ?, now()) using ttl ? and timestamp ?",
            "--schema.mapping",
            "*=*, created_at = __timestamp, time_to_live = __ttl");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestampUnsetValues();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_positional_internal_names_unset_values() {

    assumeTrue(
        protocolVersion.compareTo(ProtocolVersion.V4) > 0,
        "Unset values are not compatible with protocol version < 4");

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp-unset.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (?, ?, now()) using ttl ? and timestamp ?",
            "--schema.mapping",
            // using internal names directly in the mapping should work too
            quoteJson("*=*, created_at = \"[timestamp]\", time_to_live = \"[ttl]\""));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestampUnsetValues();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_real_names_unset_values() {

    assumeTrue(
        protocolVersion.compareTo(ProtocolVersion.V4) > 0,
        "Unset values are not compatible with protocol version < 4");

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp-unset.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (:key, :value, now()) using ttl :t1 and timestamp :t2",
            "--schema.mapping",
            "*=*, created_at = t2, time_to_live = t1");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestampUnsetValues();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_external_names_unset_values() {

    assumeTrue(
        protocolVersion.compareTo(ProtocolVersion.V4) > 0,
        "Unset values are not compatible with protocol version < 4");

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp-unset.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (:key, :value, now()) using ttl :t1 and timestamp :t2",
            "--schema.mapping",
            // using __timestamp and __ttl should work too (although not very useful), they should
            // map to t2 and t1 respectively
            "*=*, created_at = __timestamp, time_to_live = __ttl");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestampUnsetValues();
  }

  @Test
  void
      load_ttl_timestamp_now_in_query_and_mapping_with_keyspace_provided_separately_unset_values() {

    assumeTrue(
        protocolVersion.compareTo(ProtocolVersion.V4) > 0,
        "Unset values are not compatible with protocol version < 4");

    session.execute("DROP TABLE IF EXISTS table_ttl_timestamp");
    session.execute(
        "CREATE TABLE table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.ignoreLeadingWhitespaces",
            "true",
            "--connector.csv.ignoreTrailingWhitespaces",
            "true",
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp-unset.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "insert into table_ttl_timestamp (key, value, loaded_at) values (:key, :value, now()) using ttl :t1 and timestamp :t2",
            "--schema.mapping",
            "*=*, created_at = t2, time_to_live = t1");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTTLAndTimestampUnsetValues();
  }

  private void assertTTLAndTimestampUnsetValues() {
    assertThat(session.execute("SELECT COUNT(*) FROM table_ttl_timestamp").one().getLong(0))
        .isEqualTo(2L);

    Row row;

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value), loaded_at FROM table_ttl_timestamp WHERE key = 1")
            .one();
    assertThat(row.getInt(0)).isZero();
    assertThat(row.getLong(1)).isNotZero(); // cannot assert its true value
    assertThat(row.getUUID(2)).isNotNull();

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value), loaded_at FROM table_ttl_timestamp WHERE key = 2")
            .one();
    assertThat(row.getInt(0)).isNotZero().isLessThanOrEqualTo(1000);
    assertThat(row.getLong(1))
        .isEqualTo(
            instantToNumber(
                ZonedDateTime.parse("2017-11-29T14:32:15+02:00").toInstant(), MICROSECONDS, EPOCH));
    assertThat(row.getUUID(2)).isNotNull();
  }

  @Test
  void unload_and_load_timestamp_ttl() throws IOException {

    session.execute("DROP TABLE IF EXISTS unload_and_load_timestamp_ttl");
    session.execute("CREATE TABLE unload_and_load_timestamp_ttl (key int PRIMARY KEY, value text)");
    session.execute(
        "INSERT INTO unload_and_load_timestamp_ttl (key, value) VALUES (1, 'foo') "
            + "USING TIMESTAMP 123456789 AND TTL 123456789");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.header",
            "true",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "SELECT key, value, writetime(value) AS timestamp, ttl(value) AS ttl "
                + "FROM unload_and_load_timestamp_ttl");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    Stream<String> line = readAllLinesInDirectoryAsStreamExcludingHeaders(unloadDir);
    assertThat(line)
        .hasSize(1)
        .hasOnlyOneElementSatisfying(
            l ->
                assertThat(l)
                    .contains("1,foo,")
                    .contains(numberToInstant(123456789, MICROSECONDS, EPOCH).toString())
                    .containsPattern(",\\d+"));
    deleteDirectory(logDir);
    session.execute("TRUNCATE unload_and_load_timestamp_ttl");

    args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.header",
            "true",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "unload_and_load_timestamp_ttl",
            "--schema.mapping",
            "* = * , timestamp = __timestamp, ttl = __ttl");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    ResultSet rs =
        session.execute(
            "SELECT key, value, writetime(value) AS timestamp, ttl(value) AS ttl "
                + "FROM unload_and_load_timestamp_ttl WHERE key = 1");
    Row row = rs.one();
    assertThat(row.getLong("timestamp")).isEqualTo(123456789L);
    assertThat(row.getInt("ttl")).isLessThanOrEqualTo(123456789);
  }

  @Test
  void unload_and_load_timestamp_ttl_case_sensitive_custom_query() throws IOException {

    session.execute("DROP TABLE IF EXISTS \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\"");
    session.execute(
        "CREATE TABLE \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" (key int PRIMARY KEY, \"My Value\" text)");
    session.execute(
        "INSERT INTO \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" (key, \"My Value\") VALUES (1, 'foo') "
            + "USING TIMESTAMP 123456789 AND TTL 123456789");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.header",
            "true",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            quoteJson(
                "SELECT key, \"My Value\", "
                    + "writetime(\"My Value\"), "
                    + "ttl(\"My Value\") "
                    + "FROM \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\""));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    Stream<String> line = readAllLinesInDirectoryAsStreamExcludingHeaders(unloadDir);
    assertThat(line)
        .hasSize(1)
        .hasOnlyOneElementSatisfying(
            l ->
                assertThat(l)
                    .contains("1,foo,")
                    .contains(numberToInstant(123456789, MICROSECONDS, EPOCH).toString())
                    .containsPattern(",\\d+"));
    deleteDirectory(logDir);
    session.execute("TRUNCATE \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\"");

    args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.header",
            "true",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            quoteJson(
                "INSERT INTO \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" (key, \"My Value\") "
                    + "VALUES (:key, :\"My Value\") "
                    + "USING TIMESTAMP :\"writetime(My Value)\" "
                    + "AND TTL :\"ttl(My Value)\""));

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    ResultSet rs =
        session.execute(
            "SELECT key, \"My Value\", "
                + "writetime(\"My Value\") AS timestamp, "
                + "ttl(\"My Value\") AS ttl "
                + "FROM \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" WHERE key = 1");
    Row row = rs.one();
    assertThat(row.getLong("timestamp")).isEqualTo(123456789L);
    assertThat(row.getInt("ttl")).isLessThanOrEqualTo(123456789);
  }

  @Test
  void unload_and_load_timestamp_ttl_case_sensitive_custom_query_aliased() throws IOException {

    session.execute("DROP TABLE IF EXISTS \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\"");
    session.execute(
        "CREATE TABLE \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" (key int PRIMARY KEY, \"My Value\" text)");
    session.execute(
        "INSERT INTO \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" (key, \"My Value\") VALUES (1, 'foo') "
            + "USING TIMESTAMP 123456789 AND TTL 123456789");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.header",
            "true",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            quoteJson(
                "SELECT key, \"My Value\", "
                    + "writetime(\"My Value\") AS \"MyWritetime\", "
                    + "ttl(\"My Value\") AS \"MyTtl\" "
                    + "FROM \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\""));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    Stream<String> line = readAllLinesInDirectoryAsStreamExcludingHeaders(unloadDir);
    assertThat(line)
        .hasSize(1)
        .hasOnlyOneElementSatisfying(
            l ->
                assertThat(l)
                    .contains("1,foo,")
                    .contains(numberToInstant(123456789, MICROSECONDS, EPOCH).toString())
                    .containsPattern(",\\d+"));
    deleteDirectory(logDir);
    session.execute("TRUNCATE \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\"");

    args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.header",
            "true",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            quoteJson(
                "INSERT INTO \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" (key, \"My Value\") "
                    + "VALUES (:key, :\"My Value\") "
                    + "USING TIMESTAMP :\"MyWritetime\" "
                    + "AND TTL :\"MyTtl\""));

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    ResultSet rs =
        session.execute(
            "SELECT key, \"My Value\", "
                + "writetime(\"My Value\") AS timestamp, "
                + "ttl(\"My Value\") AS ttl "
                + "FROM \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" WHERE key = 1");
    Row row = rs.one();
    assertThat(row.getLong("timestamp")).isEqualTo(123456789L);
    assertThat(row.getInt("ttl")).isLessThanOrEqualTo(123456789);
  }

  @Test
  void unload_and_load_timestamp_ttl_case_sensitive_mapping() throws IOException {

    session.execute("DROP TABLE IF EXISTS \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\"");
    session.execute(
        "CREATE TABLE \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" (key int PRIMARY KEY, \"My Value\" text)");
    session.execute(
        "INSERT INTO \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" (key, \"My Value\") VALUES (1, 'foo') "
            + "USING TIMESTAMP 123456789 AND TTL 123456789");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.header",
            "true",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "UNLOAD_AND_LOAD_TIMESTAMP_TTL",
            "--schema.mapping",
            quoteJson("key, \"My Value\", writetime(\"My Value\"), ttl(\"My Value\")"));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    Stream<String> line = readAllLinesInDirectoryAsStreamExcludingHeaders(unloadDir);
    assertThat(line)
        .hasSize(1)
        .hasOnlyOneElementSatisfying(
            l ->
                assertThat(l)
                    .contains("1,foo,")
                    .contains(numberToInstant(123456789, MICROSECONDS, EPOCH).toString())
                    .containsPattern(",\\d+"));
    deleteDirectory(logDir);
    session.execute("TRUNCATE \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\"");

    args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.header",
            "true",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "UNLOAD_AND_LOAD_TIMESTAMP_TTL",
            "--schema.mapping",
            quoteJson("* = * , \"writetime(My Value)\" = __timestamp, \"ttl(My Value)\" = __ttl"));

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    ResultSet rs =
        session.execute(
            "SELECT key, \"My Value\", "
                + "writetime(\"My Value\") AS timestamp, "
                + "ttl(\"My Value\") AS ttl "
                + "FROM \"UNLOAD_AND_LOAD_TIMESTAMP_TTL\" WHERE key = 1");
    Row row = rs.one();
    assertThat(row.getLong("timestamp")).isEqualTo(123456789L);
    assertThat(row.getInt("ttl")).isLessThanOrEqualTo(123456789);
  }

  @Test
  void duplicate_values() {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_HEADER));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_code");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    validateErrorMessageLogged(
        "Invalid schema.mapping: the following variables are mapped to more than one field: country_code");
  }

  @Test
  void missing_key() {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_HEADER));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,5=country_name");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    validateErrorMessageLogged("Missing required primary key column country_code");
  }

  @Test
  void missing_key_with_custom_query() {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_HEADER));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add(INSERT_INTO_IP_BY_COUNTRY);
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number, 5=country_name");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    validateErrorMessageLogged("Missing required primary key column country_code");
  }

  @Test
  void error_load_primary_key_cannot_be_null_case_sensitive() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--log.maxErrors");
    args.add("9");
    args.add("--log.verbosity");
    args.add("2");
    args.add("--connector.csv.url");
    args.add(quoteJson(getClass().getResource("/ip-by-country-pk-null.csv")));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add("MYKS");
    args.add("--schema.table");
    args.add("IPBYCOUNTRY");
    args.add("--schema.mapping");
    args.add(quoteJson(IP_BY_COUNTRY_MAPPING_CASE_SENSITIVE));
    args.add("--codec.nullStrings");
    args.add("[NULL]");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 9")
        .contains("Records: total: 24, successful: 14, failed: 10");
    // the number of writes may vary due to the abortion
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateBadOps(10, logPath);
    validateExceptionsLog(
        10,
        "Primary key column \"COUNTRY CODE\" cannot be mapped to null",
        "mapping-errors.log",
        logPath);
  }

  @Test
  void extra_mapping() {
    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_HEADER));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,"
            + "4=country_code,5=country_name,6=extra");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    validateErrorMessageLogged("doesn't match any column found in table", "extra");
  }

  @Test
  void extra_mapping_custom_query() {
    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_HEADER));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add(INSERT_INTO_IP_BY_COUNTRY);
    args.add("--schema.mapping");
    args.add(
        "beginning_ip_address,ending_ip_address,beginning_ip_number,ending_ip_number,"
            + "country_code,country_name,extra");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    validateErrorMessageLogged("doesn't match any bound variable found in query", "extra");
  }

  /** Test for DAT-224. */
  @Test
  void should_truncate_and_round() throws Exception {

    session.execute("DROP TABLE IF EXISTS numbers");
    session.execute(
        "CREATE TABLE IF NOT EXISTS numbers (key varchar PRIMARY KEY, vdouble double, vdecimal decimal)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(ClassLoader.getSystemResource("number.csv").toExternalForm());
    args.add("--connector.csv.header");
    args.add("true");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--connector.csv.comment");
    args.add("#");
    args.add("--codec.overflowStrategy");
    args.add("TRUNCATE");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("numbers");
    args.add("--schema.mapping");
    args.add("*=*");

    int loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkNumbersWritten(TRUNCATE, UNNECESSARY, session);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--codec.roundingStrategy");
    args.add("FLOOR");
    args.add("--codec.formatNumbers");
    args.add("true");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("SELECT key, vdouble, vdecimal FROM numbers");

    int unloadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(unloadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkNumbersRead(TRUNCATE, FLOOR, true, unloadDir);
    deleteDirectory(logDir);

    // check we can load from the unloaded dataset
    args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--codec.overflowStrategy");
    args.add("TRUNCATE");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("numbers");
    args.add("--schema.mapping");
    args.add("key,vdouble,vdecimal");

    loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkNumbersWritten(TRUNCATE, FLOOR, session);
  }

  /** Test for DAT-224. */
  @Test
  void should_not_truncate_nor_round() throws Exception {

    session.execute("DROP TABLE IF EXISTS numbers");
    session.execute(
        "CREATE TABLE IF NOT EXISTS numbers (key varchar PRIMARY KEY, vdouble double, vdecimal decimal)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(ClassLoader.getSystemResource("number.csv").toExternalForm());
    args.add("--connector.csv.header");
    args.add("true");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--connector.csv.comment");
    args.add("#");
    args.add("--codec.overflowStrategy");
    args.add("REJECT");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("numbers");
    args.add("--schema.mapping");
    args.add("*=*");

    int loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_COMPLETED_WITH_ERRORS);
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateExceptionsLog(
        1,
        "ArithmeticException: Cannot convert 0.12345678901234567890123456789 from BigDecimal to Double",
        "mapping-errors.log",
        logPath);
    checkNumbersWritten(REJECT, UNNECESSARY, session);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--codec.roundingStrategy");
    args.add("UNNECESSARY");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("SELECT key, vdouble, vdecimal FROM numbers");

    int unloadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(unloadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkNumbersRead(REJECT, UNNECESSARY, false, unloadDir);
    deleteDirectory(logDir);

    // check we can load from the unloaded dataset
    args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--codec.overflowStrategy");
    args.add("REJECT");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("numbers");
    args.add("--schema.mapping");
    args.add("key,vdouble,vdecimal");

    loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkNumbersWritten(REJECT, UNNECESSARY, session);
  }

  @Test
  void delete_row_with_custom_query_positional() {

    session.execute("DROP TABLE IF EXISTS test_delete");
    session.execute(
        "CREATE TABLE IF NOT EXISTS test_delete (pk int, cc int, value int, PRIMARY KEY (pk, cc))");
    session.execute("INSERT INTO test_delete (pk, cc, value) VALUES (1,1,1)");
    session.execute("INSERT INTO test_delete (pk, cc, value) VALUES (1,2,2)");

    MockConnector.mockReads(RecordUtils.mappedCSV("pk", "1", "cc", "1"));

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("DELETE FROM test_delete WHERE pk = ? and cc = ?");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_OK);

    assertThat(session.execute("SELECT * FROM test_delete WHERE pk = 1 AND cc = 1").one()).isNull();
    assertThat(session.execute("SELECT * FROM test_delete WHERE pk = 1 AND cc = 2").one())
        .isNotNull();
  }

  @Test
  void delete_row_with_custom_query_named() {

    session.execute("DROP TABLE IF EXISTS test_delete");
    session.execute(
        "CREATE TABLE IF NOT EXISTS test_delete (\"PK\" int, \"CC\" int, value int, PRIMARY KEY (\"PK\", \"CC\"))");
    session.execute("INSERT INTO test_delete (\"PK\", \"CC\", value) VALUES (1,1,1)");
    session.execute("INSERT INTO test_delete (\"PK\", \"CC\", value) VALUES (1,2,2)");

    MockConnector.mockReads(RecordUtils.mappedCSV("Field A", "1", "Field B", "1"));

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add(
        quoteJson("DELETE FROM test_delete WHERE \"PK\" = :\"Field A\" and \"CC\" = :\"Field B\""));
    args.add("--schema.mapping");
    args.add(quoteJson("\"Field A\",\"Field B\""));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_OK);

    assertThat(session.execute("SELECT * FROM test_delete WHERE \"PK\" = 1 AND \"CC\" = 1").one())
        .isNull();
    assertThat(session.execute("SELECT * FROM test_delete WHERE \"PK\" = 1 AND \"CC\" = 2").one())
        .isNotNull();
  }

  @Test
  void delete_column_with_custom_query() {

    session.execute("DROP TABLE IF EXISTS test_delete");
    session.execute(
        "CREATE TABLE IF NOT EXISTS test_delete (pk int, cc int, value int, PRIMARY KEY (pk, cc))");
    session.execute("INSERT INTO test_delete (pk, cc, value) VALUES (1,1,1)");
    session.execute("INSERT INTO test_delete (pk, cc, value) VALUES (1,2,2)");

    MockConnector.mockReads(RecordUtils.mappedCSV("pk", "1", "cc", "1"));

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("DELETE value FROM test_delete WHERE pk = ? and cc = ?");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_OK);

    ResultSet rs1 = session.execute("SELECT value FROM test_delete WHERE pk = 1 AND cc = 1");
    Row row1 = rs1.one();
    assertThat(row1).isNotNull();
    assertThat(row1.isNull(0)).isTrue();

    ResultSet rs2 = session.execute("SELECT value FROM test_delete WHERE pk = 1 AND cc = 2");
    Row row2 = rs2.one();
    assertThat(row2).isNotNull();
    assertThat(row2.isNull(0)).isFalse();
  }

  @Test
  void delete_column_with_mapping() {

    session.execute("DROP TABLE IF EXISTS test_delete");
    session.execute(
        "CREATE TABLE IF NOT EXISTS test_delete (\"PK\" int, cc int, value int, PRIMARY KEY (\"PK\", cc))");
    session.execute("INSERT INTO test_delete (\"PK\", cc, value) VALUES (1,1,1)");
    session.execute("INSERT INTO test_delete (\"PK\", cc, value) VALUES (1,2,2)");

    MockConnector.mockReads(RecordUtils.indexedCSV("1", "1", ""));

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("test_delete");
    args.add("--schema.mapping");
    args.add("0=PK,1=cc,2=value");
    args.add("--schema.nullToUnset");
    args.add("false");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_OK);

    ResultSet rs1 = session.execute("SELECT value FROM test_delete WHERE \"PK\" = 1 AND cc = 1");
    Row row1 = rs1.one();
    assertThat(row1).isNotNull();
    assertThat(row1.isNull(0)).isTrue();

    ResultSet rs2 = session.execute("SELECT value FROM test_delete WHERE \"PK\" = 1 AND cc = 2");
    Row row2 = rs2.one();
    assertThat(row2).isNotNull();
    assertThat(row2.isNull(0)).isFalse();
  }

  @Test
  void batch_with_custom_query() {

    session.execute("DROP TABLE IF EXISTS test_batch1");
    session.execute("DROP TABLE IF EXISTS test_batch2");

    session.execute(
        "CREATE TABLE IF NOT EXISTS test_batch1 (pk int, cc int, value int, PRIMARY KEY (pk, cc))");
    session.execute(
        "CREATE TABLE IF NOT EXISTS test_batch2 (pk int, cc int, value int, PRIMARY KEY (pk, cc))");

    MockConnector.mockReads(RecordUtils.indexedCSV("1", "2", "3", "4"));

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.mapping");
    args.add("0=pk,1=cc,2=value1,3=value2");
    args.add("--schema.query");
    args.add(
        "BEGIN BATCH "
            + "INSERT INTO test_batch1 (pk, cc, value) VALUES (:pk, :cc, :value1); "
            + "INSERT INTO test_batch2 (pk, cc, value) VALUES (:pk, :cc, :value2); "
            + "APPLY BATCH");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_OK);

    ResultSet rs1 = session.execute("SELECT value FROM test_batch1 WHERE pk = 1 AND cc = 2");
    Row row1 = rs1.one();
    assertThat(row1).isNotNull();
    assertThat(row1.getInt(0)).isEqualTo(3);

    ResultSet rs2 = session.execute("SELECT value FROM test_batch2 WHERE pk = 1 AND cc = 2");
    Row row2 = rs2.one();
    assertThat(row2).isNotNull();
    assertThat(row2.getInt(0)).isEqualTo(4);
  }

  /** Test for DAT-236. */
  @Test
  void temporal_roundtrip() throws Exception {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V3) >= 0,
        "CQL type date is not compatible with C* < 3.0");

    session.execute("DROP TABLE IF EXISTS temporals");
    session.execute(
        "CREATE TABLE IF NOT EXISTS temporals (key int PRIMARY KEY, vdate date, vtime time, vtimestamp timestamp)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.ignoreLeadingWhitespaces");
    args.add("true");
    args.add("--connector.csv.ignoreTrailingWhitespaces");
    args.add("true");
    args.add("--connector.csv.url");
    args.add(ClassLoader.getSystemResource("temporal.csv").toExternalForm());
    args.add("--connector.csv.header");
    args.add("true");
    args.add("--codec.locale");
    args.add("fr_FR");
    args.add("--codec.timeZone");
    args.add("Europe/Paris");
    args.add("--codec.date");
    args.add("cccc, d MMMM uuuu");
    args.add("--codec.time");
    args.add("HHmmssSSS");
    args.add("--codec.timestamp");
    args.add("ISO_ZONED_DATE_TIME");
    args.add("--codec.unit");
    args.add("SECONDS");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("temporals");
    args.add("--schema.mapping");
    args.add("*=*");

    int loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkTemporalsWritten(session);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--codec.locale");
    args.add("fr_FR");
    args.add("--codec.timeZone");
    args.add("Europe/Paris");
    args.add("--codec.date");
    args.add("cccc, d MMMM uuuu");
    args.add("--codec.time");
    args.add("HHmmssSSS");
    args.add("--codec.timestamp");
    args.add("ISO_ZONED_DATE_TIME");
    args.add("--codec.unit");
    args.add("SECONDS");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("SELECT key, vdate, vtime, vtimestamp FROM temporals");

    int unloadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(unloadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkTemporalsRead(unloadDir, false);
    deleteDirectory(logDir);

    // check we can load from the unloaded dataset
    args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--codec.locale");
    args.add("fr_FR");
    args.add("--codec.timeZone");
    args.add("Europe/Paris");
    args.add("--codec.date");
    args.add("cccc, d MMMM uuuu");
    args.add("--codec.time");
    args.add("HHmmssSSS");
    args.add("--codec.timestamp");
    args.add("ISO_ZONED_DATE_TIME");
    args.add("--codec.unit");
    args.add("SECONDS");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("temporals");
    args.add("--schema.mapping");
    args.add("key, vdate, vtime, vtimestamp");

    loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkTemporalsWritten(session);
  }

  /** Test for DAT-364 (numeric timestamps). */
  @Test
  void temporal_roundtrip_numeric() throws Exception {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V3) >= 0,
        "CQL type date is not compatible with C* < 3.0");

    session.execute("DROP TABLE IF EXISTS temporals");
    session.execute(
        "CREATE TABLE IF NOT EXISTS temporals (key int PRIMARY KEY, vdate date, vtime time, vtimestamp timestamp)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.ignoreLeadingWhitespaces");
    args.add("true");
    args.add("--connector.csv.ignoreTrailingWhitespaces");
    args.add("true");
    args.add("--connector.csv.url");
    args.add(ClassLoader.getSystemResource("temporal-numeric.csv").toExternalForm());
    args.add("--connector.csv.header");
    args.add("true");
    args.add("--codec.locale");
    args.add("fr_FR");
    args.add("--codec.timeZone");
    args.add("Europe/Paris");
    args.add("--codec.date");
    args.add("cccc, d MMMM uuuu");
    args.add("--codec.time");
    args.add("HHmmssSSS");
    args.add("--codec.timestamp");
    args.add("UNITS_SINCE_EPOCH");
    args.add("--codec.unit");
    args.add("SECONDS");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("temporals");
    args.add("--schema.mapping");
    args.add("*=*");

    int loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkTemporalsWritten(session);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--codec.locale");
    args.add("fr_FR");
    args.add("--codec.timeZone");
    args.add("Europe/Paris");
    args.add("--codec.date");
    args.add("cccc, d MMMM uuuu");
    args.add("--codec.time");
    args.add("HHmmssSSS");
    args.add("--codec.timestamp");
    args.add("UNITS_SINCE_EPOCH");
    args.add("--codec.unit");
    args.add("SECONDS");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("SELECT key, vdate, vtime, vtimestamp FROM temporals");

    int unloadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(unloadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkTemporalsRead(unloadDir, true);
    deleteDirectory(logDir);

    // check we can load from the unloaded dataset
    args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.delimiter");
    args.add(";");
    args.add("--codec.locale");
    args.add("fr_FR");
    args.add("--codec.timeZone");
    args.add("Europe/Paris");
    args.add("--codec.date");
    args.add("cccc, d MMMM uuuu");
    args.add("--codec.time");
    args.add("HHmmssSSS");
    args.add("--codec.timestamp");
    args.add("UNITS_SINCE_EPOCH");
    args.add("--codec.unit");
    args.add("SECONDS");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("temporals");
    args.add("--schema.mapping");
    args.add("key, vdate, vtime, vtimestamp");

    loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkTemporalsWritten(session);
  }

  /** Test for DAT-253. */
  @Test
  void should_respect_mapping_variables_order() throws Exception {

    session.execute("DROP TABLE IF EXISTS mapping");
    session.execute("CREATE TABLE IF NOT EXISTS mapping (key int PRIMARY KEY, value varchar)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(ClassLoader.getSystemResource("invalid-mapping.csv").toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("mapping");
    args.add("--schema.mapping");
    args.add("value,key");

    int loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_COMPLETED_WITH_ERRORS);
    assertThat(logs)
        .hasMessageContaining(
            "At least 1 record does not match the provided schema.mapping or schema.query");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("mapping");
    args.add("--schema.mapping");
    // note that the entries are not in proper order,
    // the export should still order fields by index, so 'key,value' and not 'value,key'
    args.add("1=value,0=key");

    int unloadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(unloadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    List<String> lines = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    assertThat(lines).contains("1,ok1").contains("2,ok2");
  }

  /** Test for DAT-253. */
  @Test
  void should_respect_query_variables_order() throws Exception {

    session.execute("DROP TABLE IF EXISTS mapping");
    session.execute("CREATE TABLE IF NOT EXISTS mapping (key int PRIMARY KEY, value varchar)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(ClassLoader.getSystemResource("invalid-mapping.csv").toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    // 0 = value, 1 = key
    args.add("INSERT INTO mapping (value, key) VALUES (?, ?)");

    int loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_COMPLETED_WITH_ERRORS);
    assertThat(logs)
        .hasMessageContaining(
            "At least 1 record does not match the provided schema.mapping or schema.query");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    // the columns should be exported as they appear in the SELECT clause, so 'value,key' and not
    // 'key,value'
    args.add("SELECT value, key FROM mapping");

    int unloadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(unloadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    List<String> lines = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    assertThat(lines).contains("ok1,1").contains("ok2,2");
  }

  /** Test for DAT-326. */
  @Test
  void function_mapped_to_primary_key() {

    session.execute("DROP TABLE IF EXISTS dat326a");
    session.execute(
        "CREATE TABLE IF NOT EXISTS dat326a (pk int, cc timeuuid, v int, PRIMARY KEY (pk, cc))");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "true",
            "--connector.csv.url",
            quoteJson(getClass().getResource("/function-pk.csv")),
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "dat326a",
            "--schema.mapping",
            "now()=cc,*=*");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
  }

  /** Test for DAT-326. */
  @Test
  void function_mapped_to_primary_key_with_custom_query() {

    session.execute("DROP TABLE IF EXISTS dat326b");
    session.execute(
        "CREATE TABLE IF NOT EXISTS dat326b (pk int, cc timeuuid, v int, PRIMARY KEY (pk, cc))");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "true",
            "--connector.csv.url",
            quoteJson(getClass().getResource("/function-pk.csv")),
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "INSERT INTO dat326b (pk, cc, v) VALUES (:pk, now(), :v)");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
  }

  /** Test for DAT-326. */
  @Test
  void function_mapped_to_primary_key_with_custom_query_and_positional_variables() {

    session.execute("DROP TABLE IF EXISTS dat326c");
    session.execute(
        "CREATE TABLE IF NOT EXISTS dat326c (pk int, cc timeuuid, v int, PRIMARY KEY (pk, cc))");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "true",
            "--connector.csv.url",
            quoteJson(getClass().getResource("/function-pk.csv")),
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "INSERT INTO dat326c (pk, cc, v) VALUES (?, now(), ?)");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
  }

  @Test
  void unload_with_custom_query_and_function_with_header() throws IOException {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V3) >= 0,
        "CQL function toDate is not compatible with C* < 3.0");

    session.execute("DROP TABLE IF EXISTS unload_with_function1");
    session.execute(
        "CREATE TABLE IF NOT EXISTS unload_with_function1 (pk int, cc timeuuid, v int, PRIMARY KEY (pk, cc))");
    session.execute("INSERT INTO unload_with_function1 (pk, cc, v) values (0, now(), 1)");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "true",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.maxConcurrentFiles",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "SELECT pk, v, toDate(cc) AS date_created FROM unload_with_function1");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    List<String> lines =
        FileUtils.readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    assertThat(lines).hasSize(2);
    assertThat(lines.get(0)).isEqualTo("pk,v,date_created");
    assertThat(lines.get(1)).matches("0,1,\\d{4}-\\d{2}-\\d{2}");
  }

  @Test
  void unload_with_custom_query_and_function_without_header() throws IOException {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V3) >= 0,
        "CQL function toDate is not compatible with C* < 3.0");

    session.execute("DROP TABLE IF EXISTS unload_with_function2");
    session.execute(
        "CREATE TABLE IF NOT EXISTS unload_with_function2 (pk int, cc timeuuid, v int, PRIMARY KEY (pk, cc))");
    session.execute("INSERT INTO unload_with_function2 (pk, cc, v) values (0, now(), 1)");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "false",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.maxConcurrentFiles",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            "SELECT pk, v, toDate(cc) FROM unload_with_function2");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    List<String> lines =
        FileUtils.readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    assertThat(lines).hasSize(1);
    assertThat(lines.get(0)).matches("0,1,\\d{4}-\\d{2}-\\d{2}");
  }

  /** test for DAT-372 exercising custom bound variable names in WHERE clause restrictions */
  @Test
  void unload_token_range_restriction() throws IOException {

    session.execute("DROP TABLE IF EXISTS unload_token_range");
    session.execute(
        "CREATE TABLE unload_token_range (pk int, cc int, v int, PRIMARY KEY (pk, cc))");
    session.execute("INSERT INTO unload_token_range (pk, cc, v) values (0, 1, 2)");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "false",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.maxConcurrentFiles",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            quoteJson(
                "SELECT pk, cc, v FROM unload_token_range "
                    + "WHERE token(pk) > :\"My Start\" AND token(pk) <= :\"My End\""));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    List<String> lines =
        FileUtils.readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    assertThat(lines).hasSize(1).containsExactly("0,1,2");
  }

  @Test
  void unload_token_range_restriction_positional() throws IOException {

    session.execute("DROP TABLE IF EXISTS unload_token_range");
    session.execute(
        "CREATE TABLE unload_token_range (pk int, cc int, v int, PRIMARY KEY (pk, cc))");
    session.execute("INSERT INTO unload_token_range (pk, cc, v) values (0, 1, 2)");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "false",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.maxConcurrentFiles",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            quoteJson(
                "SELECT pk, cc, v FROM unload_token_range "
                    + "WHERE token(pk) > ? AND token(pk) <= ?"));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    List<String> lines =
        FileUtils.readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    assertThat(lines).hasSize(1).containsExactly("0,1,2");
  }

  /** Test for DAT-373. */
  @Test
  void duplicate_mappings() throws IOException {

    session.execute("DROP TABLE IF EXISTS dat373");
    session.execute("CREATE TABLE dat373 (pk int PRIMARY KEY, v1 int, v2 int)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "true",
            "--connector.csv.url",
            quoteJson(getClass().getResource("/duplicates.csv")),
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "dat373",
            "--schema.mapping",
            "*=*, v = v1, v = v2");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    Row row = session.execute("SELECT * FROM dat373").one();
    assertThat(row.getInt("pk")).isOne();
    assertThat(row.getInt("v1")).isEqualTo(42);
    assertThat(row.getInt("v2")).isEqualTo(42);

    args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "true",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.maxConcurrentFiles",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "dat373",
            "--schema.mapping",
            "pk = pk, a = v1, b = v1, c = v2, d = v2");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    List<String> lines = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    assertThat(lines).containsExactly("pk,a,b,c,d", "1,42,42,42,42");
  }

  @Test
  void load_user_defined_functions_custom_query() {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V2_2) >= 0,
        "User-defined functions are not compatible with C* < 2.2");

    session.execute("DROP TABLE IF EXISTS udf_table");
    session.execute(
        "CREATE TABLE udf_table (pk int PRIMARY KEY, \"Value 1\" int, \"Value 2\" int, \"SUM\" int)");

    session.execute("DROP FUNCTION IF EXISTS plus");
    session.execute(
        "CREATE FUNCTION plus(s int, v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;';");

    MockConnector.mockReads(RecordUtils.mappedCSV("pk", "0", "Value 1", "1", "Value 2", "2"));

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            quoteJson(logDir),
            "--connector.name",
            "mock",
            "--connector.csv.maxConcurrentFiles",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            quoteJson(
                "INSERT INTO udf_table "
                    + "(pk, \"Value 1\", \"Value 2\", \"SUM\") "
                    + "VALUES "
                    + "(:pk, :\"Value 1\", :\"Value 2\", plus(:\"Value 1\", :\"Value 2\"))"));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    Row row = session.execute("SELECT * FROM udf_table").one();
    assertThat(row.getInt("pk")).isEqualTo(0);
    assertThat(row.getInt("\"Value 1\"")).isEqualTo(1);
    assertThat(row.getInt("\"Value 2\"")).isEqualTo(2);
    assertThat(row.getInt("\"SUM\"")).isEqualTo(3);
  }

  @Test
  void unload_user_defined_functions_custom_query() throws IOException {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V2_2) >= 0,
        "User-defined functions are not compatible with C* < 2.2");

    session.execute("DROP TABLE IF EXISTS udf_table");
    session.execute(
        "CREATE TABLE udf_table (pk int PRIMARY KEY, \"Value 1\" int, \"Value 2\" int)");
    session.execute("INSERT INTO udf_table (pk, \"Value 1\", \"Value 2\") VALUES (0,1,2)");

    session.execute("DROP FUNCTION IF EXISTS plus");
    session.execute(
        "CREATE FUNCTION plus(s int, v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;';");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "true",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.maxConcurrentFiles",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.query",
            quoteJson(
                "SELECT "
                    + "\"Value 1\", \"Value 2\", "
                    + "plus(\"Value 1\", \"Value 2\") AS \"SUM\""
                    + "FROM udf_table"));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    List<String> lines = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    assertThat(lines).containsExactly("Value 1,Value 2,SUM", "1,2,3");
  }

  @Test
  void unload_user_defined_functions_mapping() throws IOException {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V2_2) >= 0,
        "User-defined functions are not compatible with C* < 2.2");

    session.execute("DROP TABLE IF EXISTS udf_table");
    session.execute(
        "CREATE TABLE udf_table (pk int PRIMARY KEY, \"Value 1\" int, \"Value 2\" int)");
    session.execute("INSERT INTO udf_table (pk, \"Value 1\", \"Value 2\") VALUES (0,1,2)");

    session.execute("DROP FUNCTION IF EXISTS plus");
    session.execute(
        "CREATE FUNCTION plus(s int, v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;';");

    List<String> args =
        Lists.newArrayList(
            "unload",
            "--log.directory",
            quoteJson(logDir),
            "-header",
            "true",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.maxConcurrentFiles",
            "1",
            "--schema.keyspace",
            session.getLoggedKeyspace(),
            "--schema.table",
            "udf_table",
            "--schema.mapping",
            quoteJson("* = [-pk], SUM = plus(\"Value 1\", \"Value 2\")"));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    List<String> lines = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    assertThat(lines).containsExactly("SUM,Value 1,Value 2", "3,1,2");
  }

  static void checkNumbersWritten(
      OverflowStrategy overflowStrategy, RoundingMode roundingMode, Session session) {
    Map<String, Double> doubles = new HashMap<>();
    Map<String, BigDecimal> bigdecimals = new HashMap<>();
    session
        .execute("SELECT * FROM numbers")
        .iterator()
        .forEachRemaining(
            row -> {
              doubles.put(row.getString("key"), row.getDouble("vdouble"));
              bigdecimals.put(row.getString("key"), row.getDecimal("vdecimal"));
            });
    if (roundingMode == UNNECESSARY) {
      checkExactNumbers(doubles, bigdecimals, overflowStrategy);
    } else {
      checkRoundedNumbers(doubles, bigdecimals, overflowStrategy);
    }
  }

  private static void checkNumbersRead(
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      boolean formatted,
      Path unloadDir)
      throws IOException {
    Map<String, String> doubles = new HashMap<>();
    Map<String, String> bigdecimals = new HashMap<>();
    List<String> lines = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    for (String line : lines) {
      List<String> cols = Lists.newArrayList(Splitter.on(';').split(line));
      doubles.put(cols.get(0), cols.get(1));
      bigdecimals.put(cols.get(0), cols.get(2));
    }
    if (formatted) {
      if (roundingMode == UNNECESSARY) {
        checkExactNumbersFormatted(doubles, overflowStrategy, true);
        checkExactNumbersFormatted(bigdecimals, overflowStrategy, false);
      } else {
        checkRoundedNumbersFormatted(doubles, overflowStrategy, true);
        checkRoundedNumbersFormatted(bigdecimals, overflowStrategy, false);
      }
    } else {
      checkExactNumbersUnformatted(doubles, overflowStrategy, true);
      checkExactNumbersUnformatted(bigdecimals, overflowStrategy, false);
    }
  }

  @SuppressWarnings("FloatingPointLiteralPrecision")
  private static void checkExactNumbers(
      Map<String, Double> doubles,
      Map<String, BigDecimal> bigdecimals,
      OverflowStrategy overflowStrategy) {
    assertThat(doubles.get("scientific_notation")).isEqualTo(1e+7d);
    assertThat(doubles.get("regular_notation")).isEqualTo(10000000d); // same as 1e+7d
    assertThat(doubles.get("regular_notation")).isEqualTo(doubles.get("scientific_notation"));
    assertThat(doubles.get("hex_notation")).isEqualTo(Double.MAX_VALUE);
    assertThat(doubles.get("irrational")).isEqualTo(0.1d);
    assertThat(doubles.get("Double.NaN")).isEqualTo(Double.NaN);
    assertThat(doubles.get("Double.POSITIVE_INFINITY")).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(doubles.get("Double.NEGATIVE_INFINITY")).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(doubles.get("Double.MAX_VALUE")).isEqualTo(Double.MAX_VALUE);
    assertThat(doubles.get("Double.MIN_VALUE")).isEqualTo(Double.MIN_VALUE);
    assertThat(doubles.get("Double.MIN_NORMAL")).isEqualTo(Double.MIN_NORMAL);
    // do not compare doubles and floats directly
    assertThat(doubles.get("Float.MAX_VALUE"))
        .isEqualTo(new BigDecimal(Float.toString(Float.MAX_VALUE)).doubleValue());
    assertThat(doubles.get("Float.MIN_VALUE"))
        .isEqualTo(new BigDecimal(Float.toString(Float.MIN_VALUE)).doubleValue());
    if (overflowStrategy == TRUNCATE) {
      // truncated
      assertThat(doubles.get("too_many_digits")).isEqualTo(0.123456789012345678d);
    } else {
      assertThat(doubles.get("too_many_digits")).isNull();
    }
    assertThat(bigdecimals.get("scientific_notation")).isEqualTo("1.0E+7");
    assertThat(bigdecimals.get("regular_notation")).isEqualTo("10000000");
    assertThat(bigdecimals.get("regular_notation"))
        .isNotEqualTo(bigdecimals.get("scientific_notation")); // different scale
    assertThat(bigdecimals.get("regular_notation"))
        .isEqualByComparingTo(bigdecimals.get("scientific_notation")); // different scale
    assertThat(bigdecimals.get("hex_notation")).isEqualTo(BigDecimal.valueOf(Double.MAX_VALUE));
    assertThat(bigdecimals.get("irrational")).isEqualTo("0.1");
    assertThat(bigdecimals.get("Double.MAX_VALUE")).isEqualTo(Double.toString(Double.MAX_VALUE));
    assertThat(bigdecimals.get("Double.MIN_VALUE")).isEqualTo(Double.toString(Double.MIN_VALUE));
    assertThat(bigdecimals.get("Double.MIN_NORMAL")).isEqualTo(Double.toString(Double.MIN_NORMAL));
    // Float.MAX_VALUE was in regular notation, so scale = 0
    assertThat(bigdecimals.get("Float.MAX_VALUE"))
        .isEqualTo(new BigDecimal(Float.toString(Float.MAX_VALUE)).setScale(0, UNNECESSARY));
    assertThat(bigdecimals.get("Float.MIN_VALUE")).isEqualTo(Float.toString(Float.MIN_VALUE));
    if (overflowStrategy == TRUNCATE) {
      // not truncated
      assertThat(bigdecimals.get("too_many_digits")).isEqualTo("0.12345678901234567890123456789");
    } else {
      assertThat(bigdecimals.get("too_many_digits")).isNull();
    }
  }

  @SuppressWarnings("FloatingPointLiteralPrecision")
  private static void checkRoundedNumbers(
      Map<String, Double> doubles,
      Map<String, BigDecimal> bigdecimals,
      OverflowStrategy overflowStrategy) {
    assertThat(doubles.get("scientific_notation")).isEqualTo(1e+7d);
    assertThat(doubles.get("regular_notation")).isEqualTo(10000000d); // same as 1e+7d
    assertThat(doubles.get("regular_notation")).isEqualTo(doubles.get("scientific_notation"));
    assertThat(doubles.get("hex_notation")).isEqualTo(Double.MAX_VALUE);
    assertThat(doubles.get("irrational")).isEqualTo(0.1d);
    assertThat(doubles.get("Double.NaN")).isEqualTo(Double.NaN);
    assertThat(doubles.get("Double.POSITIVE_INFINITY")).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(doubles.get("Double.NEGATIVE_INFINITY")).isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(doubles.get("Double.MAX_VALUE")).isEqualTo(Double.MAX_VALUE);
    assertThat(doubles.get("Double.MIN_VALUE")).isEqualTo(0d); // rounded
    assertThat(doubles.get("Double.MIN_NORMAL")).isEqualTo(0d);
    // do not compare doubles and floats directly
    assertThat(doubles.get("Float.MAX_VALUE"))
        .isEqualTo(new BigDecimal(Float.toString(Float.MAX_VALUE)).doubleValue());
    assertThat(doubles.get("Float.MIN_VALUE")).isEqualTo(0d); // rounded
    if (overflowStrategy == TRUNCATE) {
      // truncated
      assertThat(doubles.get("too_many_digits")).isEqualTo(0.12d); // rounded
    } else {
      assertThat(doubles.get("too_many_digits")).isNull();
    }
    assertThat(bigdecimals.get("scientific_notation")).isEqualTo("10000000");
    assertThat(bigdecimals.get("regular_notation")).isEqualTo("10000000");
    assertThat(bigdecimals.get("hex_notation"))
        .isEqualTo(BigDecimal.valueOf(Double.MAX_VALUE).setScale(0, UNNECESSARY));
    assertThat(bigdecimals.get("irrational")).isEqualTo("0.1");
    assertThat(bigdecimals.get("Double.MAX_VALUE"))
        .isEqualTo(BigDecimal.valueOf(Double.MAX_VALUE).setScale(0, UNNECESSARY));
    assertThat(bigdecimals.get("Double.MIN_VALUE")).isEqualTo("0"); // rounded
    assertThat(bigdecimals.get("Double.MIN_NORMAL")).isEqualTo("0"); // rounded
    assertThat(bigdecimals.get("Float.MAX_VALUE"))
        .isEqualTo(new BigDecimal(Float.toString(Float.MAX_VALUE)).setScale(0, UNNECESSARY));
    assertThat(bigdecimals.get("Float.MIN_VALUE")).isEqualTo("0"); // rounded
    if (overflowStrategy == TRUNCATE) {
      // not truncated
      assertThat(bigdecimals.get("too_many_digits")).isEqualTo("0.12"); // rounded
    } else {
      assertThat(bigdecimals.get("too_many_digits")).isNull();
    }
  }

  private static void checkExactNumbersFormatted(
      Map<String, String> numbers, OverflowStrategy overflowStrategy, boolean isDouble) {
    assertThat(numbers.get("scientific_notation")).isEqualTo("10,000,000");
    assertThat(numbers.get("regular_notation")).isEqualTo("10,000,000");
    assertThat(numbers.get("hex_notation")).startsWith("179,769,313,486,231,570,000,000");
    assertThat(Double.valueOf(numbers.get("hex_notation").replace(",", "")))
        .isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("irrational")).isEqualTo("0.1");
    if (isDouble) {
      assertThat(numbers.get("Double.NaN")).isEqualTo("NaN");
      assertThat(numbers.get("Double.POSITIVE_INFINITY")).isEqualTo("Infinity");
      assertThat(numbers.get("Double.NEGATIVE_INFINITY")).isEqualTo("-Infinity");
    }
    assertThat(numbers.get("Double.MAX_VALUE")).startsWith("179,769,313,486,231,570,000,000,000");
    assertThat(Double.valueOf(numbers.get("Double.MAX_VALUE").replace(",", "")))
        .isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("Double.MIN_VALUE"))
        .startsWith("0.000000000000000000000000000")
        .endsWith("49");
    assertThat(Double.valueOf(numbers.get("Double.MIN_VALUE").replace(",", "")))
        .isEqualTo(Double.MIN_VALUE);
    assertThat(numbers.get("Double.MIN_NORMAL"))
        .startsWith("0.00000000000000000000000000")
        .endsWith("22250738585072014");
    assertThat(Double.valueOf(numbers.get("Double.MIN_NORMAL").replace(",", "")))
        .isEqualTo(Double.MIN_NORMAL);
    assertThat(numbers.get("Float.MAX_VALUE"))
        .isEqualTo("340,282,350,000,000,000,000,000,000,000,000,000,000");
    assertThat(Float.valueOf(numbers.get("Float.MAX_VALUE").replace(",", "")))
        .isEqualTo(Float.MAX_VALUE);
    assertThat(numbers.get("Float.MIN_VALUE"))
        .isEqualTo("0.0000000000000000000000000000000000000000000014"); // rounded
    assertThat(Float.valueOf(numbers.get("Float.MIN_VALUE").replace(",", "")))
        .isEqualTo(Float.MIN_VALUE);
    if (overflowStrategy == TRUNCATE) {
      // truncated
      assertThat(numbers.get("too_many_digits")).isEqualTo("0.12"); // rounded
    } else {
      assertThat(numbers.get("too_many_digits")).isNull();
    }
  }

  private static void checkRoundedNumbersFormatted(
      Map<String, String> numbers, OverflowStrategy overflowStrategy, boolean isDouble) {
    assertThat(numbers.get("scientific_notation")).isEqualTo("10,000,000");
    assertThat(numbers.get("regular_notation")).isEqualTo("10,000,000");
    assertThat(numbers.get("hex_notation")).startsWith("179,769,313,486,231,570,000,000");
    assertThat(Double.valueOf(numbers.get("hex_notation").replace(",", "")))
        .isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("irrational")).isEqualTo("0.1");
    if (isDouble) {
      assertThat(numbers.get("Double.NaN")).isEqualTo("NaN");
      assertThat(numbers.get("Double.POSITIVE_INFINITY")).isEqualTo("Infinity");
      assertThat(numbers.get("Double.NEGATIVE_INFINITY")).isEqualTo("-Infinity");
    }
    assertThat(numbers.get("Double.MAX_VALUE")).startsWith("179,769,313,486,231,570,000,000,000");
    assertThat(Double.valueOf(numbers.get("Double.MAX_VALUE").replace(",", "")))
        .isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("Double.MIN_VALUE")).isEqualTo("0"); // rounded
    assertThat(numbers.get("Double.MIN_NORMAL")).isEqualTo("0"); // rounded
    assertThat(numbers.get("Float.MAX_VALUE"))
        .isEqualTo("340,282,350,000,000,000,000,000,000,000,000,000,000");
    assertThat(Float.valueOf(numbers.get("Float.MAX_VALUE").replace(",", "")))
        .isEqualTo(Float.MAX_VALUE);
    assertThat(numbers.get("Float.MIN_VALUE")).isEqualTo("0"); // rounded
    if (overflowStrategy == TRUNCATE) {
      // truncated
      assertThat(numbers.get("too_many_digits")).isEqualTo("0.12"); // rounded
    } else {
      assertThat(numbers.get("too_many_digits")).isNull();
    }
  }

  @SuppressWarnings("FloatingPointLiteralPrecision")
  private static void checkExactNumbersUnformatted(
      Map<String, String> numbers, OverflowStrategy overflowStrategy, boolean isDouble) {
    if (isDouble) {
      assertThat(numbers.get("scientific_notation")).isEqualTo(Double.toString(10000000d));
      assertThat(numbers.get("regular_notation")).isEqualTo(Double.toString(10000000d));
      assertThat(numbers.get("hex_notation")).isEqualTo(Double.toString(Double.MAX_VALUE));
      assertThat(numbers.get("irrational")).isEqualTo(Double.toString(0.1d));
      assertThat(numbers.get("Double.NaN")).isEqualTo(Double.toString(Double.NaN));
      assertThat(numbers.get("Double.POSITIVE_INFINITY"))
          .isEqualTo(Double.toString(Double.POSITIVE_INFINITY));
      assertThat(numbers.get("Double.NEGATIVE_INFINITY"))
          .isEqualTo(Double.toString(Double.NEGATIVE_INFINITY));
      assertThat(numbers.get("Double.MAX_VALUE")).isEqualTo(Double.toString(Double.MAX_VALUE));
      assertThat(numbers.get("Double.MIN_VALUE")).isEqualTo(Double.toString(Double.MIN_VALUE));
      assertThat(numbers.get("Double.MIN_NORMAL")).isEqualTo(Double.toString(Double.MIN_NORMAL));
      assertThat(numbers.get("Float.MAX_VALUE")).isEqualTo(Float.toString(Float.MAX_VALUE));
      assertThat(numbers.get("Float.MIN_VALUE")).isEqualTo(Float.toString(Float.MIN_VALUE));
      if (overflowStrategy == TRUNCATE) {
        // truncated
        assertThat(numbers.get("too_many_digits"))
            .isEqualTo(Double.toString(0.123456789012345678d));
      } else {
        assertThat(numbers.get("too_many_digits")).isNull();
      }
    } else {
      assertThat(numbers.get("scientific_notation")).isEqualTo(new BigDecimal("1.0E+7").toString());
      assertThat(numbers.get("regular_notation")).isEqualTo(new BigDecimal("10000000").toString());
      assertThat(numbers.get("hex_notation"))
          .isEqualTo(BigDecimal.valueOf(Double.MAX_VALUE).toString());
      assertThat(numbers.get("irrational")).isEqualTo(BigDecimal.valueOf(0.1d).toString());
      assertThat(numbers.get("Double.MAX_VALUE"))
          .isEqualTo(BigDecimal.valueOf(Double.MAX_VALUE).toString());
      assertThat(numbers.get("Double.MIN_VALUE"))
          .isEqualTo(BigDecimal.valueOf(Double.MIN_VALUE).toString());
      assertThat(numbers.get("Double.MIN_NORMAL"))
          .isEqualTo(BigDecimal.valueOf(Double.MIN_NORMAL).toString());
      // Float.MAX_VALUE was in regular notation, so scale = 0
      assertThat(numbers.get("Float.MAX_VALUE"))
          .isEqualTo(
              new BigDecimal(Float.toString(Float.MAX_VALUE)).setScale(0, UNNECESSARY).toString());
      assertThat(numbers.get("Float.MIN_VALUE"))
          .isEqualTo(new BigDecimal(Float.toString(Float.MIN_VALUE)).toString());
      if (overflowStrategy == TRUNCATE) {
        // not truncated
        assertThat(numbers.get("too_many_digits"))
            .isEqualTo(new BigDecimal("0.12345678901234567890123456789").toString());
      } else {
        assertThat(numbers.get("too_many_digits")).isNull();
      }
    }
  }

  /**
   * Test for customType without associated codec. Data should be inserted as the blob. To transform
   * DynamicCompositeType into blob:
   *
   * <pre>{@code
   * ByteBuffer foo = com.datastax.driver.core.TestUtils.serializeForDynamicCompositeType("foo",32);
   * String blobHex = com.datastax.driver.core.utils.Bytes.toHexString(foo.array());
   * }</pre>
   *
   * <p>and uses blobHex to insert into table custom_types_table - c1 column (see custom-type.csv
   * file for actual hex value)
   */
  @Test
  void full_load_unload_load_of_custom_types() throws Exception {

    URL customTypesCsv = ClassLoader.getSystemResource("custom-type.csv");
    session.execute(
        "CREATE TABLE custom_types_table (k int PRIMARY KEY, c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)')");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(customTypesCsv));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("custom_types_table");
    args.add("--schema.mapping");
    args.add("k, c1");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(1, "SELECT * FROM custom_types_table");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("custom_types_table");
    args.add("--schema.mapping");
    args.add("k, c1");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(1, unloadDir);

    args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("custom_types_table");
    args.add("--schema.mapping");
    args.add("k, c1");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(1, "SELECT * FROM custom_types_table");
  }

  static void checkTemporalsWritten(Session session) {
    Row row = session.execute("SELECT * FROM temporals WHERE key = 0").one();
    LocalDate date = row.get("vdate", LocalDateCodec.instance);
    LocalTime time = row.get("vtime", LocalTimeCodec.instance);
    Instant timestamp = row.get("vtimestamp", InstantCodec.instance);
    assertThat(date).isEqualTo(LocalDate.of(2018, 3, 9));
    assertThat(time).isEqualTo(LocalTime.of(17, 12, 32, 584_000_000));
    assertThat(timestamp).isEqualTo(Instant.parse("2018-03-09T16:12:32Z"));
  }

  private static void checkTemporalsRead(Path unloadDir, boolean numeric) throws IOException {
    String line = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList()).get(0);
    List<String> cols = Lists.newArrayList(Splitter.on(';').split(line));
    assertThat(cols).hasSize(4);
    assertThat(cols.get(1)).isEqualTo("vendredi, 9 mars 2018");
    assertThat(cols.get(2)).isEqualTo("171232584");
    if (numeric) {
      assertThat(cols.get(3)).isEqualTo("1520611952");
    } else {
      assertThat(cols.get(3)).isEqualTo("2018-03-09T17:12:32+01:00[Europe/Paris]");
    }
  }

  private void validateErrorMessageLogged(String... msg) {
    assertThat(logs).hasMessageMatching("Operation [A-Z]+_\\d{8}-\\d{6}-\\d{6} failed");
    for (String s : msg) {
      assertThat(logs).hasMessageContaining(s);
    }
    String console = stderr.getStreamAsString();
    assertThat(console).containsPattern("Operation [A-Z]+_\\d{8}-\\d{6}-\\d{6} failed");
    for (String s : msg) {
      assertThat(console).contains(s);
    }
  }
}
