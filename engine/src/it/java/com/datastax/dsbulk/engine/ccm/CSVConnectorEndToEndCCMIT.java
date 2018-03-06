/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.CSV_RECORDS;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.IP_BY_COUNTRY_COMPLEX_MAPPING;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY_COMPLEX;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.createComplexTable;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.createIpByCountryTable;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.createWithSpacesTable;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToNumber;
import static com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy.REJECT;
import static com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy.TRUNCATE;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_COMPLEX;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_HEADER;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_SKIP;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_WITH_SPACES;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateBadOps;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateExceptionsLog;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static java.math.RoundingMode.FLOOR;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.event.Level.ERROR;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
@CCMConfig(numberOfNodes = 1)
@Tag("ccm")
class CSVConnectorEndToEndCCMIT extends EndToEndCCMITBase {

  private Path unloadDir;

  CSVConnectorEndToEndCCMIT(CCMCluster ccm, Session session) {
    super(ccm, session);
  }

  @BeforeAll
  void createTables() {
    createIpByCountryTable(session);
    createComplexTable(session);
    createWithSpacesTable(session);
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    unloadDir = createTempDirectory("test");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(unloadDir);
  }

  /** Simple test case which attempts to load and unload data using ccm. */
  @Test
  void full_load_unload() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_UNIQUE.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, SELECT_FROM_IP_BY_COUNTRY);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /** Simple test case which attempts to load and unload data using ccm and compression (LZ4). */
  @Test
  void full_load_unload_lz4() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("LZ4");
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_UNIQUE.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, SELECT_FROM_IP_BY_COUNTRY);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("LZ4");
    args.add("--connector.csv.url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /** Simple test case which attempts to load and unload data using ccm and compression (Snappy). */
  @Test
  void full_load_unload_snappy() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_UNIQUE.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, SELECT_FROM_IP_BY_COUNTRY);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
    args.add("--connector.csv.url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /** Attempts to load and unload complex types (Collections, UDTs, etc). */
  @Test
  void full_load_unload_complex() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_COMPLEX.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("country_complex");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_COMPLEX_MAPPING);

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(5, SELECT_FROM_IP_BY_COUNTRY_COMPLEX);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("country_complex");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_COMPLEX_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(5, unloadDir);
  }

  /** Attempts to load and unload a larger dataset which can be batched. */
  @Test
  void full_load_unload_large_batches() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS.toExternalForm());
    args.add("--connector.csv.header");
    args.add("true");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(500, SELECT_FROM_IP_BY_COUNTRY);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
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
    args.add(Files.createTempDirectory("test").toString());
    args.add("-url");
    args.add(CSV_RECORDS_WITH_SPACES.toExternalForm());
    args.add("--schema.mapping");
    args.add("key=key,my source=my destination");
    args.add("-header");
    args.add("true");
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(1, SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("-url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.mapping");
    args.add("key=key,my source=my destination");
    args.add("-header");
    args.add("true");
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(3, unloadDir);
  }

  /** Attempts to load and unload data, some of which will be unsuccessful. */
  @Test
  void skip_test_load_unload() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_SKIP.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);
    args.add("--connector.csv.skipRecords");
    args.add("3");
    args.add("--connector.csv.maxRecords");
    args.add("24");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(Main.STATUS_COMPLETED_WITH_ERRORS);
    validateResultSetSize(21, SELECT_FROM_IP_BY_COUNTRY);
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateBadOps(3, logPath);
    validateExceptionsLog(3, "Source  :", "mapping-errors.log", logPath);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(21, unloadDir);
  }

  @Test
  void load_ttl_timestamp_now_in_mapping() throws Exception {
    session.execute(
        "CREATE TABLE IF NOT EXISTS table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            Files.createTempDirectory("test").toString(),
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

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTtlAndTimestamp();
  }

  @Test
  void load_ttl_timestamp_now_in_query() throws Exception {
    session.execute(
        "CREATE TABLE IF NOT EXISTS table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            Files.createTempDirectory("test").toString(),
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.query",
            "insert into "
                + session.getLoggedKeyspace()
                + ".table_ttl_timestamp (key, value, loaded_at) values (:key, :value, now()) using ttl :time_to_live and timestamp :created_at");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTtlAndTimestamp();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping() throws Exception {
    session.execute(
        "CREATE TABLE IF NOT EXISTS table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            Files.createTempDirectory("test").toString(),
            "--connector.csv.url",
            ClassLoader.getSystemResource("ttl-timestamp.csv").toExternalForm(),
            "--driver.pooling.local.connections",
            "1",
            "--schema.query",
            "insert into "
                + session.getLoggedKeyspace()
                + ".table_ttl_timestamp (key, value, loaded_at) values (:key, :value, now()) using ttl :t1 and timestamp :t2",
            "--schema.mapping",
            "*=*, created_at = t2, time_to_live = t1");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTtlAndTimestamp();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping_with_keyspace_provided_separately()
      throws Exception {
    session.execute(
        "CREATE TABLE IF NOT EXISTS table_ttl_timestamp (key int PRIMARY KEY, value text, loaded_at timeuuid)");

    List<String> args =
        Lists.newArrayList(
            "load",
            "--log.directory",
            Files.createTempDirectory("test").toString(),
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

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    assertTtlAndTimestamp();
  }

  private void assertTtlAndTimestamp() {
    assertThat(session.execute("SELECT COUNT(*) FROM table_ttl_timestamp").one().getLong(0))
        .isEqualTo(3L);

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
    assertThat(row.getInt(0)).isNotZero().isLessThanOrEqualTo(200);
    assertThat(row.getLong(1)).isEqualTo(123456000L);
    assertThat(row.getUUID(2)).isNotNull();

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value), loaded_at FROM table_ttl_timestamp WHERE key = 3")
            .one();
    assertThat(row.getInt(0)).isNotZero().isLessThanOrEqualTo(300);
    assertThat(row.getLong(1))
        .isEqualTo(
            instantToNumber(
                ZonedDateTime.parse("2017-11-29T14:32:15+02:00").toInstant(), MICROSECONDS, EPOCH));
    assertThat(row.getUUID(2)).isNotNull();
  }

  @Test
  void duplicate_values(@LogCapture(value = Main.class, level = ERROR) LogInterceptor logs)
      throws IOException {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_HEADER.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_code");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(Main.STATUS_ABORTED_FATAL_ERROR);
    validateErrorMessageLogged(
        logs, "Multiple input values in mapping resolve to column", "country_code");
  }

  @Test
  void missing_key(@LogCapture(value = Main.class, level = ERROR) LogInterceptor logs)
      throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_HEADER.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number, 5=country_name");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(Main.STATUS_ABORTED_FATAL_ERROR);
    validateErrorMessageLogged(logs, "Missing required key column of", "country_code");
  }

  @Test
  void extra_mapping(@LogCapture(value = Main.class, level = ERROR) LogInterceptor logs)
      throws Exception {
    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_HEADER.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code, 5=country_name, 6=extra");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(Main.STATUS_ABORTED_FATAL_ERROR);
    validateErrorMessageLogged(logs, "doesn't match any column found in table", "extra");
  }

  /** Test for DAT-224. */
  @Test
  void should_truncate_and_round() throws Exception {

    session.execute("DROP TABLE IF EXISTS dat224");
    session.execute(
        "CREATE TABLE IF NOT EXISTS dat224 (key varchar PRIMARY KEY, vdouble double, vdecimal decimal)");

    List<String> loadArgs = new ArrayList<>();
    loadArgs.add("load");
    loadArgs.add("--log.directory");
    loadArgs.add(Files.createTempDirectory("test").toString());
    loadArgs.add("--connector.csv.url");
    loadArgs.add(ClassLoader.getSystemResource("number.csv").toExternalForm());
    loadArgs.add("--connector.csv.header");
    loadArgs.add("true");
    loadArgs.add("--connector.csv.delimiter");
    loadArgs.add(";");
    loadArgs.add("--connector.csv.comment");
    loadArgs.add("#");
    loadArgs.add("--codec.overflowStrategy");
    loadArgs.add("TRUNCATE");
    loadArgs.add("--schema.keyspace");
    loadArgs.add(session.getLoggedKeyspace());
    loadArgs.add("--schema.table");
    loadArgs.add("dat224");
    loadArgs.add("--schema.mapping");
    loadArgs.add("*=*");

    int loadStatus = new Main(addContactPointAndPort(loadArgs)).run();
    assertThat(loadStatus).isEqualTo(Main.STATUS_OK);

    checkNumbersWritten(TRUNCATE, UNNECESSARY, session);

    List<String> unloadArgs = new ArrayList<>();
    unloadArgs.add("unload");
    unloadArgs.add("--log.directory");
    unloadArgs.add(Files.createTempDirectory("test").toString());
    unloadArgs.add("--connector.csv.url");
    unloadArgs.add(unloadDir.toString());
    unloadArgs.add("--connector.csv.header");
    unloadArgs.add("false");
    unloadArgs.add("--connector.csv.delimiter");
    unloadArgs.add(";");
    unloadArgs.add("--connector.csv.maxConcurrentFiles");
    unloadArgs.add("1");
    unloadArgs.add("--codec.roundingStrategy");
    unloadArgs.add("FLOOR");
    unloadArgs.add("--codec.formatNumbers");
    unloadArgs.add("true");
    unloadArgs.add("--schema.keyspace");
    unloadArgs.add(session.getLoggedKeyspace());
    unloadArgs.add("--schema.query");
    unloadArgs.add("SELECT key, vdouble, vdecimal FROM dat224");

    int unloadStatus = new Main(addContactPointAndPort(unloadArgs)).run();
    assertThat(unloadStatus).isEqualTo(Main.STATUS_OK);

    checkNumbersRead(TRUNCATE, FLOOR, true, unloadDir);

    // check we can load from the unloaded dataset
    loadArgs = new ArrayList<>();
    loadArgs.add("load");
    loadArgs.add("--log.directory");
    loadArgs.add(Files.createTempDirectory("test").toString());
    loadArgs.add("--connector.csv.url");
    loadArgs.add(unloadDir.toString());
    loadArgs.add("--connector.csv.header");
    loadArgs.add("false");
    loadArgs.add("--connector.csv.delimiter");
    loadArgs.add(";");
    loadArgs.add("--codec.overflowStrategy");
    loadArgs.add("TRUNCATE");
    loadArgs.add("--schema.keyspace");
    loadArgs.add(session.getLoggedKeyspace());
    loadArgs.add("--schema.table");
    loadArgs.add("dat224");
    loadArgs.add("--schema.mapping");
    loadArgs.add("key,vdouble,vdecimal");

    loadStatus = new Main(addContactPointAndPort(loadArgs)).run();
    assertThat(loadStatus).isEqualTo(Main.STATUS_OK);

    checkNumbersWritten(TRUNCATE, FLOOR, session);
  }

  /** Test for DAT-224. */
  @Test
  void should_not_truncate_nor_round() throws Exception {

    session.execute("DROP TABLE IF EXISTS dat224");
    session.execute(
        "CREATE TABLE IF NOT EXISTS dat224 (key varchar PRIMARY KEY, vdouble double, vdecimal decimal)");

    List<String> loadArgs = new ArrayList<>();
    loadArgs.add("load");
    loadArgs.add("--log.directory");
    loadArgs.add(Files.createTempDirectory("test").toString());
    loadArgs.add("--connector.csv.url");
    loadArgs.add(ClassLoader.getSystemResource("number.csv").toExternalForm());
    loadArgs.add("--connector.csv.header");
    loadArgs.add("true");
    loadArgs.add("--connector.csv.delimiter");
    loadArgs.add(";");
    loadArgs.add("--connector.csv.comment");
    loadArgs.add("#");
    loadArgs.add("--codec.overflowStrategy");
    loadArgs.add("REJECT");
    loadArgs.add("--schema.keyspace");
    loadArgs.add(session.getLoggedKeyspace());
    loadArgs.add("--schema.table");
    loadArgs.add("dat224");
    loadArgs.add("--schema.mapping");
    loadArgs.add("*=*");

    int loadStatus = new Main(addContactPointAndPort(loadArgs)).run();
    assertThat(loadStatus).isEqualTo(Main.STATUS_COMPLETED_WITH_ERRORS);

    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateExceptionsLog(
        1,
        "ArithmeticException: Cannot convert 0.12345678901234567890123456789 from BigDecimal to Double",
        "mapping-errors.log",
        logPath);
    checkNumbersWritten(REJECT, UNNECESSARY, session);

    List<String> unloadArgs = new ArrayList<>();
    unloadArgs.add("unload");
    unloadArgs.add("--log.directory");
    unloadArgs.add(Files.createTempDirectory("test").toString());
    unloadArgs.add("--connector.csv.url");
    unloadArgs.add(unloadDir.toString());
    unloadArgs.add("--connector.csv.header");
    unloadArgs.add("false");
    unloadArgs.add("--connector.csv.delimiter");
    unloadArgs.add(";");
    unloadArgs.add("--connector.csv.maxConcurrentFiles");
    unloadArgs.add("1");
    unloadArgs.add("--codec.roundingStrategy");
    unloadArgs.add("UNNECESSARY");
    unloadArgs.add("--schema.keyspace");
    unloadArgs.add(session.getLoggedKeyspace());
    unloadArgs.add("--schema.query");
    unloadArgs.add("SELECT key, vdouble, vdecimal FROM dat224");

    int unloadStatus = new Main(addContactPointAndPort(unloadArgs)).run();
    assertThat(unloadStatus).isEqualTo(Main.STATUS_OK);

    checkNumbersRead(REJECT, UNNECESSARY, false, unloadDir);

    // check we can load from the unloaded dataset
    loadArgs = new ArrayList<>();
    loadArgs.add("load");
    loadArgs.add("--log.directory");
    loadArgs.add(Files.createTempDirectory("test").toString());
    loadArgs.add("--connector.csv.url");
    loadArgs.add(unloadDir.toString());
    loadArgs.add("--connector.csv.header");
    loadArgs.add("false");
    loadArgs.add("--connector.csv.delimiter");
    loadArgs.add(";");
    loadArgs.add("--codec.overflowStrategy");
    loadArgs.add("REJECT");
    loadArgs.add("--schema.keyspace");
    loadArgs.add(session.getLoggedKeyspace());
    loadArgs.add("--schema.table");
    loadArgs.add("dat224");
    loadArgs.add("--schema.mapping");
    loadArgs.add("key,vdouble,vdecimal");

    loadStatus = new Main(addContactPointAndPort(loadArgs)).run();
    assertThat(loadStatus).isEqualTo(Main.STATUS_OK);

    checkNumbersWritten(REJECT, UNNECESSARY, session);
  }

  static void checkNumbersWritten(
      OverflowStrategy overflowStrategy, RoundingMode roundingMode, Session session) {
    Map<String, Double> doubles = new HashMap<>();
    Map<String, BigDecimal> bigdecimals = new HashMap<>();
    session
        .execute("SELECT * FROM dat224")
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
    List<String> lines =
        FileUtils.readAllLinesInDirectoryAsStream(unloadDir, UTF_8).collect(Collectors.toList());
    for (String line : lines) {
      List<String> cols = Splitter.on(';').splitToList(line);
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

  private void validateErrorMessageLogged(LogInterceptor logs, String... msg) {
    CommonsAssertions.assertThat(logs)
        .hasMessageContaining("Load workflow engine execution")
        .hasMessageContaining("aborted");
    for (String s : msg) {
      CommonsAssertions.assertThat(logs).hasMessageContaining(s);
    }
  }
}
