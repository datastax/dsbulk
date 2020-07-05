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

import static com.datastax.oss.driver.api.core.type.DataTypes.BLOB;
import static com.datastax.oss.driver.api.core.type.DataTypes.BOOLEAN;
import static com.datastax.oss.driver.api.core.type.DataTypes.INT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_ABORTED_FATAL_ERROR;
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_ABORTED_TOO_MANY_ERRORS;
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_COMPLETED_WITH_ERRORS;
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_OK;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.assertStatus;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.createParameterizedQuery;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.createQueryWithError;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.createQueryWithResultSet;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.createQueryWithResultSetWithQuotes;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.createSimpleParameterizedQuery;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.primeIpByCountryTable;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.validateExceptionsLog;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.validateNumberOfBadRecords;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.validateOutputFiles;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.validatePrepare;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.validateQueryCount;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDERR;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDOUT;
import static com.datastax.oss.dsbulk.tests.utils.FileUtils.createURLFile;
import static com.datastax.oss.dsbulk.tests.utils.FileUtils.readAllLinesInDirectoryAsStream;
import static com.datastax.oss.dsbulk.tests.utils.StringUtils.quoteJson;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_ONE;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.csv.CSVConnector;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.ExitStatus;
import com.datastax.oss.dsbulk.runner.tests.CsvUtils;
import com.datastax.oss.dsbulk.runner.tests.MockConnector;
import com.datastax.oss.dsbulk.runner.tests.RecordUtils;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Column;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Keyspace;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Table;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

class CSVEndToEndSimulacronIT extends EndToEndSimulacronITBase {

  private Path urlFileTwoFiles;
  private Path urlFileOneFileOneDir;
  private Path urlFileTwoDirs;

  CSVEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture(loggerName = "com.datastax.oss.dsbulk") LogInterceptor logs,
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

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
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
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Batches: total: 24, size: 1.00 mean, 1 min, 1 max")
        .contains("Writes: total: 24, successful: 24, failed: 0");
    validateQueryCount(simulacron, 24, "INSERT INTO ip_by_country", LOCAL_ONE);
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
        INSERT_INTO_IP_BY_COUNTRY,
        "--schema.mapping",
        IP_BY_COUNTRY_MAPPING_INDEXED
      };

      ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
      assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
      validateExceptionsLog(2, "AllNodesFailedException", "operation.log");
    } finally {
      simulacron.acceptConnections();
    }
  }

  @ParameterizedTest
  @MethodSource("multipleUrlsProvider")
  void full_load_multiple_urls(Path urlfile) {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
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
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Batches: total: 24, size: 1.00 mean, 1 min, 1 max")
        .contains("Writes: total: 24, successful: 24, failed: 0");
    assertStatus(status, STATUS_OK);
    validateQueryCount(simulacron, 24, "INSERT INTO ip_by_country", LOCAL_ONE);
  }

  @SuppressWarnings("unused")
  private List<Arguments> multipleUrlsProvider() {
    return Lists.newArrayList(
        arguments(urlFileTwoFiles), arguments(urlFileOneFileOneDir), arguments(urlFileTwoDirs));
  }

  @Test
  void full_load_dry_run() {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
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
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateQueryCount(simulacron, 0, "INSERT INTO ip_by_country", ONE);
  }

  @Test
  void full_load_crlf() {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
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
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateQueryCount(simulacron, 24, "INSERT INTO ip_by_country", LOCAL_ONE);
  }

  @Test
  void partial_load() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
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
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED,
      "--schema.allowMissingFields",
      "true"
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);
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
            INSERT_INTO_IP_BY_COUNTRY, params, new ReadTimeoutResult(LOCAL_ONE, 1, 0, false));
    simulacron.prime(new Prime(prime1));

    params.put("country_name", "Gregistan");
    prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new WriteTimeoutResult(LOCAL_ONE, 0, 0, WriteType.BATCH_LOG));
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
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_ERROR),
      "--driver.advanced.retry-policy.max-retries",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);

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
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED,
      "--schema.allowMissingFields",
      "true"
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);
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
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_LONG),
      "--connector.csv.maxCharsPerColumn",
      "10000",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateQueryCount(simulacron, 1, "INSERT INTO ip_by_country", LOCAL_ONE);
  }

  @Test
  void error_load_percentage() {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
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
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED,
      "--batch.mode",
      "DISABLED"
    };
    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 1%");
  }

  @Test
  void error_load_primary_key_cannot_be_null() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
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
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };
    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 9")
        .contains("Records: total: 24, successful: 14, failed: 10");
    // the number of writes may vary due to the abortion
    validateNumberOfBadRecords(10);
    validateExceptionsLog(
        10, "Primary key column country_code cannot be set to null", "mapping-errors.log");
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
    assertStatus(status, STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 2")
        .contains("Records: total: 3, successful: 0, failed: 3");
    validateNumberOfBadRecords(3);
    validateExceptionsLog(
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
    assertStatus(status, STATUS_ABORTED_TOO_MANY_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("aborted: Too many errors, the maximum allowed is 2")
        .contains("Records: total: 3, successful: 0, failed: 3");
    validateNumberOfBadRecords(3);
    validateExceptionsLog(3, "Extraneous field C was found in record", "mapping-errors.log");
  }

  /**
   * Test for DAT-593. Emulates 100 resources with 1000 records each, among which 100 bad records.
   * Verifies that LogManager is capable of handling the amount of bad records, without disrupting
   * the main load workflow.
   */
  @Test
  void massive_load_errors() throws Exception {

    SimulacronUtils.primeTables(
        simulacron,
        new Keyspace(
            "ks1",
            new Table(
                "table1", new Column("pk", TEXT), new Column("cc", TEXT), new Column("v", TEXT))));

    MockConnector.setDelegate(
        new CSVConnector() {

          @Override
          public void configure(@NonNull Config settings, boolean read) {}

          @Override
          public void init() {}

          @Override
          public int estimatedResourceCount() {
            return Integer.MAX_VALUE; // to force runner to use maximum parallelism
          }

          @NonNull
          @Override
          public Publisher<Publisher<Record>> readByResource() {
            List<Publisher<Record>> resources = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
              AtomicInteger counter = new AtomicInteger();
              resources.add(
                  Flux.generate(
                      (sink) -> {
                        int next = counter.getAndIncrement();
                        if (next == 1_000) {
                          sink.complete();
                        } else if (next % 10 == 0) {
                          sink.next(
                              RecordUtils.error(new IllegalArgumentException("boo: " + next)));
                        } else {
                          sink.next(
                              RecordUtils.indexedCSV(
                                  "pk",
                                  String.valueOf(next),
                                  "cc",
                                  String.valueOf(next),
                                  "v",
                                  String.valueOf(next)));
                        }
                      }));
            }
            return Flux.fromIterable(resources);
          }
        });

    String[] args = {
      "load",
      "-c",
      "mock",
      "--log.maxErrors",
      "10000",
      "--schema.keyspace",
      "ks1",
      "--schema.table",
      "table1"
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("completed with 10000 errors")
        .contains("Records: total: 100,000, successful: 90,000, failed: 10,000");
    validateExceptionsLog(10_000, "boo", "connector-errors.log");
  }

  @Test
  void full_unload() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime select = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
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
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Reads: total: 24, successful: 24, failed: 0");
    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
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
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    verifyDelimiterCount(';', 168);
    verifyDelimiterCount('<', 96);
    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
    validateOutputFiles(24, unloadDir);
  }

  @Test
  void full_unload_multi_thread() throws Exception {

    primeIpByCountryTable(simulacron);
    // 1000 rows required to fully exercise writing to 4 files
    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 1000);
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
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
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
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
    validateQueryCount(simulacron, 0, SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
    validatePrepare(simulacron, SELECT_FROM_IP_BY_COUNTRY);
  }

  @Test
  void unload_failure_during_read_multi_thread() {

    primeIpByCountryTable(simulacron);
    RequestPrime prime =
        createQueryWithError(
            SELECT_FROM_IP_BY_COUNTRY, new SyntaxErrorResult("Invalid table", 0L, false));
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
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
    validateQueryCount(simulacron, 0, SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
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

    primeIpByCountryTable(simulacron);
    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 10);
    simulacron.prime(new Prime(prime));

    String[] args = {
      "unload",
      "--connector.name",
      "mock",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
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
      "-header",
      "false",
      "--connector.csv.url",
      "-",
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
    assertThat(stdOut.getStreamLines()).hasSize(24);
  }

  @ParameterizedTest
  @CsvSource({"BASE64,1|yv66vg==", "HEX,1|0xcafebabe"})
  void unload_binary(String format, String expected) throws Exception {

    SimulacronUtils.primeTables(
        simulacron,
        new Keyspace(
            "ks1",
            new Table(
                "table1",
                Collections.singletonList(new Column("pk", INT)),
                Collections.emptyList(),
                Collections.singletonList(new Column("v", BLOB)),
                Lists.newArrayList(
                    ImmutableMap.of("pk", 1, "v", ByteUtils.fromHexString("0xcafebabe"))))));

    String[] args = {
      "unload",
      "--connector.csv.header",
      "false",
      "--connector.csv.delimiter",
      quoteJson("|"),
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--schema.keyspace",
      "ks1",
      "--schema.table",
      "table1",
      "--codec.binary",
      format,
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    assertThat(readAllLinesInDirectoryAsStream(unloadDir)).containsExactly(expected);
  }

  private void verifyDelimiterCount(char delimiter, int expected) throws Exception {
    String contents = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.joining("\n"));
    assertThat(StringUtils.countOccurrences(delimiter, contents)).isEqualTo(expected);
  }
}
