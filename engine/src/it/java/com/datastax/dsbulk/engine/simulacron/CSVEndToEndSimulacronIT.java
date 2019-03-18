/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.driver.core.DataType.cboolean;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDOUT;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_CRLF;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_ERROR;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_LONG;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_PARTIAL_BAD;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_PARTIAL_BAD_LONG;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_SKIP;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createParameterizedQuery;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createQueryWithError;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createQueryWithResultSet;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createQueryWithResultSetWithQuotes;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createSimpleParameterizedQuery;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.primeIpByCountryTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateExceptionsLog;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateNumberOfBadRecords;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validatePrepare;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateQueryCount;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.QUORUM;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronExtension;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils.Column;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils.Table;
import com.datastax.dsbulk.commons.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.commons.tests.utils.StringUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.csv.CSVConnector;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.engine.tests.MockConnector;
import com.datastax.dsbulk.engine.tests.utils.LogUtils;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.result.FunctionFailureResult;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.result.SyntaxErrorResult;
import com.datastax.oss.simulacron.common.result.UnavailableResult;
import com.datastax.oss.simulacron.common.result.WriteFailureResult;
import com.datastax.oss.simulacron.common.result.WriteTimeoutResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SimulacronConfig(numberOfNodes = 3)
class CSVEndToEndSimulacronIT {

  private final BoundCluster simulacron;
  private final LogInterceptor logs;
  private final StreamInterceptor stdOut;
  private final StreamInterceptor stdErr;
  private final String hostname1;
  private final String port1;
  private final String hostname2;
  private final String hostname3;
  private final BoundNode nodeFirst;
  private final BoundNode nodeSecond;
  private final BoundNode nodeThird;

  private Path unloadDir;
  private Path logDir;

  CSVEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture LogInterceptor logs,
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {
    this.simulacron = simulacron;
    this.logs = logs;
    this.stdOut = stdOut;
    this.stdErr = stdErr;
    nodeFirst = simulacron.dc(0).node(0);
    InetSocketAddress node = nodeFirst.inetSocketAddress();
    nodeSecond = simulacron.dc(0).node(1);
    InetSocketAddress node2 = nodeSecond.inetSocketAddress();
    nodeThird = simulacron.dc(0).node(2);
    InetSocketAddress node3 = nodeThird.inetSocketAddress();

    System.out.println("nodeFirst: " + node);
    hostname1 = node.getHostName();
    port1 = Integer.toString(node.getPort());
    System.out.println("nodeFirst: " + node + " hostname1: " + hostname1 + " port1: " + port1);
    hostname2 = node2.getHostName();
    hostname3 = node3.getHostName();
  }

  @BeforeEach
  void resetPrimes() {
    simulacron.clearPrimes(true);
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
    deleteDirectory(unloadDir);
  }

  @AfterEach
  void resetLogbackConfiguration() throws JoranException {
    LogUtils.resetLogbackConfiguration();
  }

  @Test
  void full_load() throws InterruptedException {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "--log.verbosity",
      "2",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_UNIQUE),
      "--driver.query.consistency",
      "QUORUM",
      "--driver.hosts",
      String.format("%s,%s,%s", hostname1, hostname2, hostname3),
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    nodeFirst.stop();
//    new java.util.Timer()
//        .schedule(
//            new java.util.TimerTask() {
//              @Override
//              public void run() {
//                System.out.println("Stop node");
//                nodeSecond.stop();
//              }
//            },
//            3000);
    nodeSecond.stop();

    int status = new DataStaxBulkLoader(args).run();

    assertThat(status).isZero();
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Batches: total: 24, size: 1.00 mean, 1 min, 1 max")
        .contains("Writes: total: 24, successful: 24, failed: 0");
    validateQueryCount(simulacron, 24, "INSERT INTO ip_by_country", QUORUM);
  }

  @Test
  void full_load_dry_run() {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_UNIQUE),
      "-dryRun",
      "true",
      "--driver.query.consistency",
      "QUORUM",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isZero();
    validateQueryCount(simulacron, 0, "INSERT INTO ip_by_country", ONE);
  }

  @Test
  void full_load_crlf() {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_CRLF),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isZero();
    validateQueryCount(simulacron, 24, "INSERT INTO ip_by_country", ONE);
  }

  @Test
  void partial_load() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_PARTIAL_BAD),
      "--driver.query.consistency",
      "QUORUM",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED,
      "--schema.allowMissingFields",
      "true"
    };

    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_COMPLETED_WITH_ERRORS);
    validateQueryCount(simulacron, 21, "INSERT INTO ip_by_country", LOCAL_ONE);
    validateNumberOfBadRecords(3);
    validateExceptionsLog(3, "Source:", "mapping-errors.log");
  }

  @Test
  void load_errors() throws Exception {

    primeIpByCountryTable(simulacron);

    Map<String, Object> params = new HashMap<>();
    params.put("country_name", "Sweden");
    RequestPrime prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY, params, new SuccessResult(emptyList(), emptyMap()));
    simulacron.prime(new Prime(prime1));

    // recoverable errors only

    params.put("country_name", "France");
    prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY, params, new UnavailableResult(LOCAL_ONE, 1, 0));
    simulacron.prime(new Prime(prime1));

    params.put("country_name", "Gregistan");
    prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY, params, new WriteTimeoutResult(ONE, 0, 0, WriteType.BATCH));
    simulacron.prime(new Prime(prime1));

    params.put("country_name", "Andybaijan");
    prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new WriteFailureResult(ONE, 0, 0, emptyMap(), WriteType.BATCH));
    simulacron.prime(new Prime(prime1));

    params = new HashMap<>();
    params.put("country_name", "United States");
    prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new FunctionFailureResult("keyspace", "function", emptyList(), "bad function call"));
    simulacron.prime(new Prime(prime1));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_ERROR),
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.policy.maxRetries",
      "1",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_COMPLETED_WITH_ERRORS);

    // There are 24 rows of data, but two extra queries due to the retry for the write timeout and
    // the unavailable.
    validateQueryCount(simulacron, 26, "INSERT INTO ip_by_country", LOCAL_ONE);
    validateNumberOfBadRecords(4);
    validateExceptionsLog(4, "Source:", "load-errors.log");
  }

  @Test
  void skip_test_load() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_SKIP),
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--connector.csv.skipRecords",
      "3",
      "--connector.csv.maxRecords",
      "24",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED,
      "--schema.allowMissingFields",
      "true"
    };

    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_COMPLETED_WITH_ERRORS);
    validateQueryCount(simulacron, 21, "INSERT INTO ip_by_country", LOCAL_ONE);
    validateNumberOfBadRecords(3);
    validateExceptionsLog(3, "Source:", "mapping-errors.log");
  }

  @Test
  void load_long_column() {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    // This will attempt to load a CSV file with column longer then 4096 characters.
    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_LONG),
      "--connector.csv.maxCharsPerColumn",
      "10000",
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isZero();
    validateQueryCount(simulacron, 1, "INSERT INTO ip_by_country", LOCAL_ONE);
  }

  @Test
  void error_load_percentage() {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "--log.maxErrors",
      "1%",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_PARTIAL_BAD_LONG),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED,
      "--batch.mode",
      "DISABLED"
    };
    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum percentage allowed is 1.0%");
  }

  @Test
  void error_load_primary_key_cannot_be_null() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "--log.maxErrors",
      "9",
      "--log.verbosity",
      "2",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(getClass().getResource("/ip-by-country-pk-null.csv")),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--codec.nullStrings",
      "[NULL]",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };
    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 9")
        .contains("Records: total: 24, successful: 14, failed: 10");
    // the number of writes may vary due to the abortion
    validateNumberOfBadRecords(10);
    validateExceptionsLog(
        10, "Primary key column country_code cannot be mapped to null", "mapping-errors.log");
  }

  @Test
  void error_load_missing_field() throws Exception {

    SimulacronUtils.primeTables(
        simulacron,
        new SimulacronUtils.Keyspace(
            "ks1",
            new Table(
                "table1",
                new Column("a", cint()),
                new Column("b", varchar()),
                new Column("c", cboolean()),
                new Column("d", cint()))));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "--log.maxErrors",
      "2",
      "--log.verbosity",
      "2",
      "-header",
      "true",
      "--connector.csv.url",
      quoteJson(getClass().getResource("/missing-extra.csv")),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      "INSERT INTO ks1.table1 (a, b, c, d) VALUES (:a, :b, :c, :d)",
      "--schema.mapping",
      "A = a, B = b, C = c, D = d",
      "--schema.allowMissingFields",
      "false"
    };
    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 2")
        .contains("Records: total: 3, successful: 0, failed: 3");
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateNumberOfBadRecords(3);
    validateExceptionsLog(
        3, "Required field D (mapped to column d) was missing from record", "mapping-errors.log");
  }

  @Test
  void error_load_extra_field() throws Exception {

    SimulacronUtils.primeTables(
        simulacron,
        new SimulacronUtils.Keyspace(
            "ks1", new Table("table1", new Column("a", cint()), new Column("b", varchar()))));

    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "--log.maxErrors",
      "2",
      "--log.verbosity",
      "2",
      "-header",
      "true",
      "--connector.csv.url",
      quoteJson(getClass().getResource("/missing-extra.csv")),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      "INSERT INTO ks1.table1 (a, b) VALUES (:a, :b)",
      "--schema.mapping",
      "A = a, B = b",
      "--schema.allowExtraFields",
      "false"
    };
    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 2")
        .contains("Records: total: 3, successful: 0, failed: 3");
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateNumberOfBadRecords(3);
    validateExceptionsLog(3, "Extraneous field C was found in record", "mapping-errors.log");
  }

  @Test
  void full_unload() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime select = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(select));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      quoteJson(logDir),
      "--log.verbosity",
      "2",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(unloadArgs).run();
    assertThat(status).isZero();
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Reads: total: 24, successful: 24, failed: 0");
    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ONE);
    validateOutputFiles(24, unloadDir);
  }

  /**
   * This exercises logic which will replace the delimiter and make sure non-standard quoting is
   * working.
   */
  @Test
  void full_unload_csv_default_modification() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime select = createQueryWithResultSetWithQuotes(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(select));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--connector.csv.delimiter",
      ";",
      "--connector.csv.quote",
      "<",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(unloadArgs).run();
    assertThat(status).isZero();
    verifyDelimiterCount(';', 168);
    verifyDelimiterCount('<', 96);
    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ONE);
    validateOutputFiles(24, unloadDir);
  }

  @Test
  void full_unload_multi_thread() throws Exception {

    primeIpByCountryTable(simulacron);
    // 1000 rows required to fully exercise writing to 4 files
    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 1000);
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "4",
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(unloadArgs).run();
    assertThat(status).isZero();
    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ConsistencyLevel.LOCAL_ONE);
    validateOutputFiles(1000, unloadDir);
  }

  @Test
  void unload_failure_during_read_single_thread() {

    primeIpByCountryTable(simulacron);
    RequestPrime prime =
        createQueryWithError(
            SELECT_FROM_IP_BY_COUNTRY, new SyntaxErrorResult("Invalid table", 0L, false));
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(unloadArgs).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    validateQueryCount(simulacron, 0, SELECT_FROM_IP_BY_COUNTRY, ONE);
    validatePrepare(simulacron, SELECT_FROM_IP_BY_COUNTRY);
  }

  @Test
  void unload_failure_during_read_multi_thread() {

    primeIpByCountryTable(simulacron);
    RequestPrime prime =
        createQueryWithError(
            SELECT_FROM_IP_BY_COUNTRY, new SyntaxErrorResult("Invalid table", 0L, false));
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "4",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(unloadArgs).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    validateQueryCount(simulacron, 0, SELECT_FROM_IP_BY_COUNTRY, ONE);
    validatePrepare(simulacron, SELECT_FROM_IP_BY_COUNTRY);
  }

  @Test
  void unload_write_error() {

    Path file1 = unloadDir.resolve("output-000001.csv");
    Path file2 = unloadDir.resolve("output-000002.csv");
    Path file3 = unloadDir.resolve("output-000003.csv");
    Path file4 = unloadDir.resolve("output-000004.csv");

    MockConnector.setDelegate(
        new CSVConnector() {

          @Override
          public void configure(LoaderConfig settings, boolean read) {
            settings =
                new DefaultLoaderConfig(
                        ConfigFactory.parseString(
                            "url = "
                                + quoteJson(unloadDir)
                                + ", header = false, maxConcurrentFiles = 4"))
                    .withFallback(ConfigFactory.load().getConfig("dsbulk.connector.csv"));
            super.configure(settings, read);
          }

          @Override
          public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
            // will cause the write workers to fail because the files already exist
            try {
              Files.createFile(file1);
              Files.createFile(file2);
              Files.createFile(file3);
              Files.createFile(file4);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
            return super.write();
          }
        });

    primeIpByCountryTable(simulacron);
    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 10);
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      quoteJson(logDir),
      "--connector.name",
      "mock",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(unloadArgs).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    assertThat(stdErr.getStreamAsString())
        .contains("failed")
        .containsPattern("output-00000[1-4].csv");
    assertThat(logs.getAllMessagesAsString())
        .contains("failed")
        .containsPattern("output-00000[1-4].csv");
  }

  @Test
  void validate_stdout() {

    primeIpByCountryTable(simulacron);
    RequestPrime select = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(select));

    String[] args = {
      "unload",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      "-",
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      hostname1,
      "--driver.port",
      port1,
      "--driver.pooling.local.connections",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(args).run();
    assertThat(status).isZero();

    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ONE);
    assertThat(stdOut.getStreamLines().size()).isEqualTo(24);
  }

  private void verifyDelimiterCount(char delimiter, int expected) throws Exception {
    String contents =
        FileUtils.readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.joining("\n"));
    assertThat(StringUtils.countOccurrences(delimiter, contents)).isEqualTo(expected);
  }
}
