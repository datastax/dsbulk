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
package com.datastax.oss.dsbulk.runner.simulacron;

import static com.datastax.oss.driver.api.core.type.DataTypes.BOOLEAN;
import static com.datastax.oss.driver.api.core.type.DataTypes.INT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_ABORTED_FATAL_ERROR;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_ABORTED_TOO_MANY_ERRORS;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_COMPLETED_WITH_ERRORS;
import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_OK;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDERR;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDOUT;
import static com.datastax.oss.dsbulk.tests.utils.FileUtils.createURLFile;
import static com.datastax.oss.dsbulk.tests.utils.StringUtils.quoteJson;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.csv.CSVConnector;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus;
import com.datastax.oss.dsbulk.runner.tests.CsvUtils;
import com.datastax.oss.dsbulk.runner.tests.EndToEndUtils;
import com.datastax.oss.dsbulk.runner.tests.MockConnector;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Column;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Keyspace;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Table;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.result.FunctionFailureResult;
import com.datastax.oss.simulacron.common.result.ReadTimeoutResult;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.result.SyntaxErrorResult;
import com.datastax.oss.simulacron.common.result.WriteFailureResult;
import com.datastax.oss.simulacron.common.result.WriteTimeoutResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.RejectScope;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;

class CSVEndToEndSimulacronIT extends EndToEndSimulacronITBase {

  private Path urlFileTwoFiles;
  private Path urlFileOneFileOneDir;
  private Path urlFileTwoDirs;

  CSVEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture LogInterceptor logs,
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {
    super(simulacron, logs, stdOut, stdErr);
  }

  @BeforeAll
  void setupURLFiles() throws IOException {
    urlFileTwoFiles =
        createURLFile(CsvUtils.CSV_RECORDS_UNIQUE_PART_1, CsvUtils.CSV_RECORDS_UNIQUE_PART_2);
    urlFileOneFileOneDir =
        createURLFile(CsvUtils.CSV_RECORDS_UNIQUE_PART_1_DIR, CsvUtils.CSV_RECORDS_UNIQUE_PART_2);
    urlFileTwoDirs =
        createURLFile(
            CsvUtils.CSV_RECORDS_UNIQUE_PART_1_DIR, CsvUtils.CSV_RECORDS_UNIQUE_PART_2_DIR);
  }

  @AfterAll
  void cleanupURLFiles() throws IOException {
    Files.delete(urlFileTwoFiles);
    Files.delete(urlFileOneFileOneDir);
    Files.delete(urlFileTwoDirs);
  }

  @Test
  void full_load() {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime insert =
        EndToEndUtils.createSimpleParameterizedQuery(EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.verbosity",
      "2",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_UNIQUE),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Batches: total: 24, size: 1.00 mean, 1 min, 1 max")
        .contains("Writes: total: 24, successful: 24, failed: 0");
    EndToEndUtils.validateQueryCount(simulacron, 24, "INSERT INTO ip_by_country", LOCAL_ONE);
  }

  @Test
  void full_load_with_all_nodes_failed_exception() throws Exception {
    // simulate AllNodesFailedException
    simulacron.rejectConnections(0, RejectScope.STOP);
    try {
      String[] args = {
        "load",
        "--log.verbosity",
        "2",
        "-header",
        "false",
        "--connector.csv.url",
        StringUtils.quoteJson(CsvUtils.CSV_RECORDS_UNIQUE),
        "--schema.keyspace",
        "ks1",
        "--schema.query",
        EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
        "--schema.mapping",
        EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
      };

      ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
      EndToEndUtils.assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
      EndToEndUtils.validateExceptionsLog(2, "AllNodesFailedException", "operation.log");
    } finally {
      simulacron.acceptConnections();
    }
  }

  @ParameterizedTest
  @MethodSource("multipleUrlsProvider")
  void full_load_multiple_urls(Path urlfile) {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime insert =
        EndToEndUtils.createSimpleParameterizedQuery(EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.verbosity",
      "2",
      "-header",
      "false",
      "--connector.csv.urlfile",
      StringUtils.quoteJson(urlfile.toAbsolutePath()),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Batches: total: 24, size: 1.00 mean, 1 min, 1 max")
        .contains("Writes: total: 24, successful: 24, failed: 0");
    EndToEndUtils.assertStatus(status, STATUS_OK);
    EndToEndUtils.validateQueryCount(simulacron, 24, "INSERT INTO ip_by_country", LOCAL_ONE);
  }

  @SuppressWarnings("unused")
  private List<Arguments> multipleUrlsProvider() {
    return Lists.newArrayList(
        arguments(urlFileTwoFiles), arguments(urlFileOneFileOneDir), arguments(urlFileTwoDirs));
  }

  @Test
  void full_load_dry_run() {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime insert =
        EndToEndUtils.createSimpleParameterizedQuery(EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_UNIQUE),
      "-dryRun",
      "true",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);
    EndToEndUtils.validateQueryCount(simulacron, 0, "INSERT INTO ip_by_country", ONE);
  }

  @Test
  void full_load_crlf() {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime insert =
        EndToEndUtils.createSimpleParameterizedQuery(EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_CRLF),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);
    EndToEndUtils.validateQueryCount(simulacron, 24, "INSERT INTO ip_by_country", LOCAL_ONE);
  }

  @Test
  void partial_load() throws Exception {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime insert =
        EndToEndUtils.createSimpleParameterizedQuery(EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_PARTIAL_BAD),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED,
      "--schema.allowMissingFields",
      "true"
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);
    EndToEndUtils.validateQueryCount(simulacron, 21, "INSERT INTO ip_by_country", LOCAL_ONE);
    EndToEndUtils.validateNumberOfBadRecords(3);
    EndToEndUtils.validateExceptionsLog(3, "Source:", "mapping-errors.log");
  }

  @Test
  void load_errors() throws Exception {

    EndToEndUtils.primeIpByCountryTable(simulacron);

    Map<String, Object> params = new HashMap<>();
    params.put("country_name", "Sweden");
    RequestPrime prime1 =
        EndToEndUtils.createParameterizedQuery(
            EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
            params,
            new SuccessResult(emptyList(), emptyMap()));
    simulacron.prime(new Prime(prime1));

    // recoverable errors only

    params.put("country_name", "France");
    prime1 =
        EndToEndUtils.createParameterizedQuery(
            EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
            params,
            new ReadTimeoutResult(LOCAL_ONE, 1, 0, false));
    simulacron.prime(new Prime(prime1));

    params.put("country_name", "Gregistan");
    prime1 =
        EndToEndUtils.createParameterizedQuery(
            EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
            params,
            new WriteTimeoutResult(LOCAL_ONE, 0, 0, WriteType.BATCH_LOG));
    simulacron.prime(new Prime(prime1));

    params.put("country_name", "Andybaijan");
    prime1 =
        EndToEndUtils.createParameterizedQuery(
            EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
            params,
            new WriteFailureResult(ONE, 0, 0, emptyMap(), WriteType.BATCH));
    simulacron.prime(new Prime(prime1));

    params = new HashMap<>();
    params.put("country_name", "United States");
    prime1 =
        EndToEndUtils.createParameterizedQuery(
            EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
            params,
            new FunctionFailureResult("keyspace", "function", emptyList(), "bad function call"));
    simulacron.prime(new Prime(prime1));

    String[] args = {
      "load",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_ERROR),
      "--driver.advanced.retry-policy.max-retries",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);

    // There are 24 rows of data, but two extra queries due to the retry for the write timeout and
    // the unavailable.
    EndToEndUtils.validateQueryCount(simulacron, 26, "INSERT INTO ip_by_country", LOCAL_ONE);
    EndToEndUtils.validateNumberOfBadRecords(4);
    EndToEndUtils.validateExceptionsLog(4, "Source:", "load-errors.log");
  }

  @Test
  void skip_test_load() throws Exception {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime insert =
        EndToEndUtils.createSimpleParameterizedQuery(EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_SKIP),
      "--connector.csv.skipRecords",
      "3",
      "--connector.csv.maxRecords",
      "24",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED,
      "--schema.allowMissingFields",
      "true"
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);
    EndToEndUtils.validateQueryCount(simulacron, 21, "INSERT INTO ip_by_country", LOCAL_ONE);
    EndToEndUtils.validateNumberOfBadRecords(3);
    EndToEndUtils.validateExceptionsLog(3, "Source:", "mapping-errors.log");
  }

  @Test
  void load_long_column() {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime insert =
        EndToEndUtils.createSimpleParameterizedQuery(EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    // This will attempt to load a CSV file with column longer then 4096 characters.
    String[] args = {
      "load",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_LONG),
      "--connector.csv.maxCharsPerColumn",
      "10000",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);
    EndToEndUtils.validateQueryCount(simulacron, 1, "INSERT INTO ip_by_country", LOCAL_ONE);
  }

  @Test
  void error_load_percentage() {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime insert =
        EndToEndUtils.createSimpleParameterizedQuery(EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.maxErrors",
      "1%",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_PARTIAL_BAD_LONG),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED,
      "--batch.mode",
      "DISABLED"
    };
    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 1%");
  }

  @Test
  void error_load_primary_key_cannot_be_null() throws Exception {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime insert =
        EndToEndUtils.createSimpleParameterizedQuery(EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.maxErrors",
      "9",
      "--log.verbosity",
      "2",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(getClass().getResource("/ip-by-country-pk-null.csv")),
      "--codec.nullStrings",
      "[NULL]",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };
    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 9")
        .contains("Records: total: 24, successful: 14, failed: 10");
    // the number of writes may vary due to the abortion
    EndToEndUtils.validateNumberOfBadRecords(10);
    EndToEndUtils.validateExceptionsLog(
        10, "Primary key column country_code cannot be mapped to null", "mapping-errors.log");
  }

  @Test
  void error_load_missing_field() throws Exception {

    SimulacronUtils.primeTables(
        simulacron,
        new Keyspace(
            "ks1",
            new Table(
                "table1",
                new Column("a", INT),
                new Column("b", TEXT),
                new Column("c", BOOLEAN),
                new Column("d", INT))));

    String[] args = {
      "load",
      "--log.maxErrors",
      "2",
      "--log.verbosity",
      "2",
      "-header",
      "true",
      "--connector.csv.url",
      StringUtils.quoteJson(getClass().getResource("/missing-extra.csv")),
      "--schema.query",
      "INSERT INTO ks1.table1 (a, b, c, d) VALUES (:a, :b, :c, :d)",
      "--schema.mapping",
      "A = a, B = b, C = c, D = d",
      "--schema.allowMissingFields",
      "false"
    };
    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 2")
        .contains("Records: total: 3, successful: 0, failed: 3");
    EndToEndUtils.validateNumberOfBadRecords(3);
    EndToEndUtils.validateExceptionsLog(
        3, "Required field D (mapped to column d) was missing from record", "mapping-errors.log");
  }

  @Test
  void error_load_extra_field() throws Exception {

    SimulacronUtils.primeTables(
        simulacron,
        new Keyspace("ks1", new Table("table1", new Column("a", INT), new Column("b", TEXT))));

    String[] args = {
      "load",
      "--log.maxErrors",
      "2",
      "--log.verbosity",
      "2",
      "-header",
      "true",
      "--connector.csv.url",
      StringUtils.quoteJson(getClass().getResource("/missing-extra.csv")),
      "--schema.query",
      "INSERT INTO ks1.table1 (a, b) VALUES (:a, :b)",
      "--schema.mapping",
      "A = a, B = b",
      "--schema.allowExtraFields",
      "false"
    };
    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 2")
        .contains("Records: total: 3, successful: 0, failed: 3");
    EndToEndUtils.validateNumberOfBadRecords(3);
    EndToEndUtils.validateExceptionsLog(
        3, "Extraneous field C was found in record", "mapping-errors.log");
  }

  @Test
  void full_unload() throws Exception {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime select =
        EndToEndUtils.createQueryWithResultSet(EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(select));

    String[] args = {
      "unload",
      "--log.verbosity",
      "2",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Reads: total: 24, successful: 24, failed: 0");
    EndToEndUtils.validateQueryCount(
        simulacron, 1, EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
    EndToEndUtils.validateOutputFiles(24, unloadDir);
  }

  /**
   * This exercises logic which will replace the delimiter and make sure non-standard quoting is
   * working.
   */
  @Test
  void full_unload_csv_default_modification() throws Exception {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime select =
        EndToEndUtils.createQueryWithResultSetWithQuotes(
            EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(select));

    String[] args = {
      "unload",
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
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);
    verifyDelimiterCount(';', 168);
    verifyDelimiterCount('<', 96);
    EndToEndUtils.validateQueryCount(
        simulacron, 1, EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
    EndToEndUtils.validateOutputFiles(24, unloadDir);
  }

  @Test
  void full_unload_multi_thread() throws Exception {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    // 1000 rows required to fully exercise writing to 4 files
    RequestPrime prime =
        EndToEndUtils.createQueryWithResultSet(EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, 1000);
    simulacron.prime(new Prime(prime));

    String[] args = {
      "unload",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "4",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);
    EndToEndUtils.validateQueryCount(
        simulacron, 1, EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, ConsistencyLevel.LOCAL_ONE);
    EndToEndUtils.validateOutputFiles(1000, unloadDir);
  }

  @Test
  void unload_failure_during_read_single_thread() {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime prime =
        EndToEndUtils.createQueryWithError(
            EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY,
            new SyntaxErrorResult("Invalid table", 0L, false));
    simulacron.prime(new Prime(prime));

    String[] args = {
      "unload",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
    EndToEndUtils.validateQueryCount(
        simulacron, 0, EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
    EndToEndUtils.validatePrepare(simulacron, EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY);
  }

  @Test
  void unload_failure_during_read_multi_thread() {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime prime =
        EndToEndUtils.createQueryWithError(
            EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY,
            new SyntaxErrorResult("Invalid table", 0L, false));
    simulacron.prime(new Prime(prime));

    String[] args = {
      "unload",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "4",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
    EndToEndUtils.validateQueryCount(
        simulacron, 0, EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
    EndToEndUtils.validatePrepare(simulacron, EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY);
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
          public void configure(@NonNull Config settings, boolean read) {
            settings =
                ConfigFactory.parseString(
                        "url = "
                            + quoteJson(unloadDir)
                            + ", header = false, maxConcurrentFiles = 4")
                    .withFallback(
                        ConfigUtils.createReferenceConfig().getConfig("dsbulk.connector.csv"));
            super.configure(settings, read);
          }

          @NonNull
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

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime prime =
        EndToEndUtils.createQueryWithResultSet(EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, 10);
    simulacron.prime(new Prime(prime));

    String[] args = {
      "unload",
      "--connector.name",
      "mock",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
    assertThat(stdErr.getStreamAsString())
        .contains("failed")
        .containsPattern("output-00000[1-4].csv");
    assertThat(logs.getAllMessagesAsString())
        .contains("failed")
        .containsPattern("output-00000[1-4].csv");
  }

  @Test
  void validate_stdout() {

    EndToEndUtils.primeIpByCountryTable(simulacron);
    RequestPrime select =
        EndToEndUtils.createQueryWithResultSet(EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(select));

    String[] args = {
      "unload",
      "-header",
      "false",
      "--connector.csv.url",
      "-",
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);

    EndToEndUtils.validateQueryCount(
        simulacron, 1, EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
    assertThat(stdOut.getStreamLines()).hasSize(24);
  }

  private void verifyDelimiterCount(char delimiter, int expected) throws Exception {
    String contents =
        FileUtils.readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.joining("\n"));
    assertThat(StringUtils.countOccurrences(delimiter, contents)).isEqualTo(expected);
  }
}
