/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils.instantToTimestampSinceEpoch;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_COMPLEX;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_SKIP;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_WITH_SPACES;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_COMPLEX_MAPPING;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY_COMPLEX;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createComplexTable;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createIpByCountryTable;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createWithSpacesTable;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateBadOps;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateExceptionsLog;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateOutputFiles;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.ccm.CCMCluster;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(numberOfNodes = 1)
@Tag("ccm")
class CSVConnectorEndToEndCCMIT extends EndToEndCCMITBase {

  private Path unloadDir;
  private Path outputFile;

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
    outputFile = unloadDir.resolve("output-000001.csv");
  }

  @AfterEach
  void deleteDirs() throws IOException {
    deleteRecursively(unloadDir, ALLOW_INSECURE);
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
    validateOutputFiles(24, outputFile);
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
    validateOutputFiles(24, outputFile);
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
    validateOutputFiles(24, outputFile);
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
    validateOutputFiles(5, outputFile);
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
    validateOutputFiles(500, outputFile);
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
    validateOutputFiles(3, outputFile);
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
    assertThat(status).isZero();
    validateResultSetSize(21, SELECT_FROM_IP_BY_COUNTRY);
    validateBadOps(3);
    validateExceptionsLog(3, "Source  :", "mapping-errors.log");

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
    validateOutputFiles(21, outputFile);
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
            instantToTimestampSinceEpoch(
                ZonedDateTime.parse("2017-11-29T14:32:15+02:00").toInstant(), MICROSECONDS, EPOCH));
    assertThat(row.getUUID(2)).isNotNull();
  }

  @Test
  void load_ttl_timestamp_now_in_query() throws Exception {
    session.execute(
        "CREATE TABLE IF NOT EXISTS table_ttl_timestamp_query (key int PRIMARY KEY, value text, loaded_at timeuuid)");

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
                + ".table_ttl_timestamp_query (key, value, loaded_at) values (:key, :value, now()) using ttl :time_to_live and timestamp :created_at");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertThat(session.execute("SELECT COUNT(*) FROM table_ttl_timestamp_query").one().getLong(0))
        .isEqualTo(3L);

    Row row;

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value), loaded_at FROM table_ttl_timestamp_query WHERE key = 1")
            .one();
    assertThat(row.getInt(0)).isZero();
    assertThat(row.getLong(1)).isNotZero(); // cannot assert its true value
    assertThat(row.getUUID(2)).isNotNull();

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value), loaded_at FROM table_ttl_timestamp_query WHERE key = 2")
            .one();
    assertThat(row.getInt(0)).isNotZero().isLessThanOrEqualTo(200);
    assertThat(row.getLong(1)).isEqualTo(123456000L);
    assertThat(row.getUUID(2)).isNotNull();

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value), loaded_at FROM table_ttl_timestamp_query WHERE key = 3")
            .one();
    assertThat(row.getInt(0)).isNotZero().isLessThanOrEqualTo(300);
    assertThat(row.getLong(1))
        .isEqualTo(
            instantToTimestampSinceEpoch(
                ZonedDateTime.parse("2017-11-29T14:32:15+02:00").toInstant(), MICROSECONDS, EPOCH));
    assertThat(row.getUUID(2)).isNotNull();
  }

  @Test
  void load_ttl_timestamp_now_in_query_and_mapping() throws Exception {
    session.execute(
        "CREATE TABLE IF NOT EXISTS table_ttl_timestamp_query_mapping (key int PRIMARY KEY, value text)");

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
                + ".table_ttl_timestamp_query_mapping (key, value) values (:key, :value) using ttl :t1 and timestamp :t2",
            "--schema.mapping",
            "*=*, created_at = t2, time_to_live = t1");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertThat(
            session
                .execute("SELECT COUNT(*) FROM table_ttl_timestamp_query_mapping")
                .one()
                .getLong(0))
        .isEqualTo(3L);

    Row row;

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value) FROM table_ttl_timestamp_query_mapping WHERE key = 1")
            .one();
    assertThat(row.getInt(0)).isZero();
    assertThat(row.getLong(1)).isNotZero(); // cannot assert its true value

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value) FROM table_ttl_timestamp_query_mapping WHERE key = 2")
            .one();
    assertThat(row.getInt(0)).isNotZero().isLessThanOrEqualTo(200);
    assertThat(row.getLong(1)).isEqualTo(123456000L);

    row =
        session
            .execute(
                "SELECT TTL(value), WRITETIME(value) FROM table_ttl_timestamp_query_mapping WHERE key = 3")
            .one();
    assertThat(row.getInt(0)).isNotZero().isLessThanOrEqualTo(300);
    assertThat(row.getLong(1))
        .isEqualTo(
            instantToTimestampSinceEpoch(
                ZonedDateTime.parse("2017-11-29T14:32:15+02:00").toInstant(), MICROSECONDS, EPOCH));
  }
}
