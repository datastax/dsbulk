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
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.getCheckpointManager;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.primeIpByCountryTable;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.validateCheckpointFile;
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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.base.CharMatcher;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.DefaultResource;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.Resource;
import com.datastax.oss.dsbulk.connectors.csv.CSVConnector;
import com.datastax.oss.dsbulk.partitioner.TokenRangeReadStatementGenerator;
import com.datastax.oss.dsbulk.partitioner.utils.TokenUtils;
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
import com.datastax.oss.dsbulk.workflow.api.log.OperationDirectory;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.Checkpoint;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.CheckpointManager;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.ReplayStrategy;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadStatement;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.FunctionFailureResult;
import com.datastax.oss.simulacron.common.result.ReadTimeoutResult;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.result.SyntaxErrorResult;
import com.datastax.oss.simulacron.common.result.UnavailableResult;
import com.datastax.oss.simulacron.common.result.WriteFailureResult;
import com.datastax.oss.simulacron.common.result.WriteTimeoutResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.RejectScope;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
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
  void full_load() throws IOException {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.verbosity",
      "high",
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
    validateCheckpointFile(24);
  }

  @Test
  void full_load_no_checkpoint() {

    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));

    String[] args = {
      "load",
      "--log.verbosity",
      "high",
      "-header",
      "false",
      "--connector.csv.url",
      StringUtils.quoteJson(CsvUtils.CSV_RECORDS_UNIQUE),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED,
      "--log.checkpoint.enabled",
      "false"
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Batches: total: 24, size: 1.00 mean, 1 min, 1 max")
        .contains("Writes: total: 24, successful: 24, failed: 0");
    validateQueryCount(simulacron, 24, "INSERT INTO ip_by_country", LOCAL_ONE);
    Path logPath =
        OperationDirectory.getCurrentOperationDirectory().orElseThrow(IllegalStateException::new);
    Path checkpointFile = logPath.resolve("checkpoint.csv");
    assertThat(checkpointFile).doesNotExist();
  }

  @Test
  void full_load_with_all_nodes_failed_exception() throws Exception {
    // simulate AllNodesFailedException
    simulacron.rejectConnections(0, RejectScope.STOP);
    try {
      String[] args = {
        "load",
        "--log.verbosity",
        "high",
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
      "high",
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
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new SuccessResult(emptyList(), new LinkedHashMap<>()));
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

    // Verify that the console reporter has the correct number of errors
    List<String> streamLines = stdErr.getStreamLinesPlain();
    assertThat(streamLines).anyMatch(line -> line.startsWith("total | failed"));
    assertThat(streamLines).anyMatch(line -> line.startsWith("   24 |      4"));

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
      "high",
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
      "high",
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
      "high",
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

  /** Test for DAT-593 originally, enhanced to verify checkpointing. */
  @ParameterizedTest
  @EnumSource(ReplayStrategy.class)
  void massive_load_with_errors(ReplayStrategy replayStrategy) throws Exception {

    Keyspace ks =
        new Keyspace(
            "ks1",
            new Table(
                "table1", new Column("pk", TEXT), new Column("cc", TEXT), new Column("v", TEXT)));

    SimulacronUtils.primeTables(simulacron, ks);

    // generated 10 failed writes every 10 writes = 10 * 100 resources = 1000 writes failed
    for (int i = 1; i < 1000; i += 100) {
      Query when =
          new Query(
              "INSERT INTO ks1.table1 (pk,cc,v) VALUES (:pk,:cc,:v)",
              (String[]) null,
              Maps.newLinkedHashMap(ImmutableMap.of("pk", i + "", "cc", i + "", "v", i + "")),
              Maps.newLinkedHashMap(
                  ImmutableMap.of("pk", "varchar", "cc", "varchar", "v", "varchar")));
      Result then = new WriteTimeoutResult(LOCAL_ONE, 0, 1, WriteType.SIMPLE, 0, true);
      simulacron.prime(new Prime(new RequestPrime(when, then)));
    }

    Query when =
        new Query(
            "INSERT INTO ks1.table1 (pk,cc,v) VALUES (:pk,:cc,:v)",
            (String[]) null,
            Maps.newLinkedHashMap(ImmutableMap.of("pk", "*", "cc", "*", "v", "*")),
            Maps.newLinkedHashMap(
                ImmutableMap.of("pk", "varchar", "cc", "varchar", "v", "varchar")));
    Result then = new SuccessResult(null, null);
    simulacron.prime(new Prime(new RequestPrime(when, then)));

    MockConnector.setDelegate(
        new CSVConnector() {

          @Override
          public void configure(
              @NonNull Config settings, boolean read, boolean retainRecordSources) {}

          @Override
          public void init() {}

          @Override
          public int readConcurrency() {
            return Integer.MAX_VALUE; // to force runner to use maximum parallelism
          }

          @NonNull
          @Override
          public Publisher<Resource> read() {
            List<Resource> resources = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
              boolean first = i == 0;
              AtomicInteger counter = new AtomicInteger();
              URI resource = URI.create("file://file" + (i + 1));
              Flux<Record> records =
                  Flux.generate(
                      (sink) -> {
                        int next = counter.getAndIncrement();
                        if (next == 1_000) {
                          sink.complete();
                        } else if (next % 10 == 0 && !first) {
                          sink.next(
                              RecordUtils.error(
                                  resource,
                                  next + 1,
                                  new IllegalArgumentException(
                                      "Record could not be read: " + next)));
                        } else {
                          sink.next(
                              RecordUtils.mappedCSV(
                                  resource,
                                  next + 1,
                                  "pk",
                                  String.valueOf(next),
                                  "cc",
                                  String.valueOf(next),
                                  "v",
                                  String.valueOf(next)));
                        }
                      });
              resources.add(new DefaultResource(resource, records));
            }
            return Flux.fromIterable(resources);
          }
        });

    String[] args = {
      "load",
      "-c",
      "mock",
      "--batch.mode",
      "DISABLED",
      "--log.maxErrors",
      "11000",
      "--schema.query",
      "INSERT INTO ks1.table1 (pk,cc,v) VALUES (:pk,:cc,:v)",
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        // successful (1000) + (1000-100)*99 = 90100
        // failed 100000 - 90100 = 9900
        .contains("completed with 10,900 errors")
        .contains("Records: total: 100,000, successful: 90,100, failed: 9,900")
        // successful (1000-10)+(1000-100-10)*99 = 89100
        // failed 10*100 = 1000
        .contains("Writes: total: 90,100, successful: 89,100, failed: 1,000, in-flight: 0");
    validateExceptionsLog(9_900, "Record could not be read:", "connector-errors.log");
    validateExceptionsLog(
        1_000,
        "Statement execution failed: INSERT INTO ks1.table1 (pk,cc,v) VALUES (:pk,:cc,:v)",
        "load-errors.log");
    validateCheckpointFile(100_000);

    CheckpointManager manager = getCheckpointManager();

    // first resource has only 10 failed writes
    checkCheckpointFile(manager, 0, 990, 10);
    for (int i = 1; i < 100; i++) {
      // other resources: 100 failed connector reads + 10 failed writes = 110 failed records
      checkCheckpointFile(manager, i, 890, 110);
    }

    // Resume failed operation

    simulacron.clearPrimes(true);
    simulacron.clearLogs();

    SimulacronUtils.primeTables(simulacron, ks);
    simulacron.prime(new Prime(new RequestPrime(when, then)));

    MockConnector.setDelegate(
        new CSVConnector() {

          @Override
          public void configure(
              @NonNull Config settings, boolean read, boolean retainRecordSources) {}

          @Override
          public void init() {}

          @Override
          public int readConcurrency() {
            return Integer.MAX_VALUE; // to force runner to use maximum parallelism
          }

          @NonNull
          @Override
          public Publisher<Resource> read() {
            List<Resource> resources = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
              AtomicInteger counter = new AtomicInteger();
              URI resource = URI.create("file://file" + (i + 1));
              Flux<Record> records =
                  Flux.generate(
                      (sink) -> {
                        int next = counter.getAndIncrement();
                        if (next == 1_000) {
                          sink.complete();
                        } else {
                          sink.next(
                              RecordUtils.mappedCSV(
                                  resource,
                                  next + 1,
                                  "pk",
                                  String.valueOf(next),
                                  "cc",
                                  String.valueOf(next),
                                  "v",
                                  String.valueOf(next)));
                        }
                      });
              resources.add(new DefaultResource(resource, records));
            }
            return Flux.fromIterable(resources);
          }
        });

    Path checkpointFile =
        OperationDirectory.getCurrentOperationDirectory()
            .orElseThrow(IllegalStateException::new)
            .resolve("checkpoint.csv");

    args =
        new String[] {
          "load",
          "-c",
          "mock",
          "--batch.mode",
          "DISABLED",
          "--log.maxErrors",
          "11000",
          "--schema.query",
          "INSERT INTO ks1.table1 (pk,cc,v) VALUES (:pk,:cc,:v)",
          "--log.checkpoint.file",
          quoteJson(checkpointFile.toAbsolutePath()),
          "--log.checkpoint.replayStrategy",
          replayStrategy.name()
        };

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();

    switch (replayStrategy) {
      case resume:
        assertStatus(status, STATUS_ABORTED_FATAL_ERROR);
        assertThat(logs.getAllMessagesAsString()).contains("Nothing to replay");
        break;
      case retry:
        assertStatus(status, STATUS_OK);
        assertThat(logs.getAllMessagesAsString()).contains("completed successfully");
        assertThat(logs.getAllMessagesAsString())
            .contains("Writes: total: 10,900, successful: 10,900, failed: 0")
            .contains("Records: total: 100,000, successful: 100,000, failed: 0");
        validateCheckpointFile(100_000, replayStrategy, true);
        manager = getCheckpointManager();
        for (int i = 0; i < 100; i++) {
          checkCheckpointFile(manager, i, 1000, 0);
        }
        break;
      case rewind:
        assertStatus(status, STATUS_OK);
        assertThat(logs.getAllMessagesAsString()).contains("completed successfully");
        assertThat(logs.getAllMessagesAsString())
            .contains("Writes: total: 100,000, successful: 100,000, failed: 0")
            .contains("Records: total: 100,000, successful: 100,000, failed: 0");
        validateCheckpointFile(100_000, replayStrategy, true);
        manager = getCheckpointManager();
        for (int i = 0; i < 100; i++) {
          checkCheckpointFile(manager, i, 1000, 0);
        }
        break;
    }
  }

  private void checkCheckpointFile(
      CheckpointManager manager, int i, int expectedSuccessful, int expectedFailed) {
    URI resource = URI.create("file://file" + (i + 1));
    Checkpoint checkpoint = manager.getCheckpoint(resource);
    assertThat(checkpoint.isComplete()).isTrue();
    assertThat(checkpoint.getConsumedSuccessful().sum()).isEqualTo(expectedSuccessful);
    assertThat(checkpoint.getConsumedFailed().sum()).isEqualTo(expectedFailed);
  }

  @ParameterizedTest
  @EnumSource(ReplayStrategy.class)
  void massive_unload_with_errors(ReplayStrategy replayStrategy) throws Exception {

    List<Column> partitionKeys = Collections.singletonList(new Column("pk", TEXT));
    List<Column> clusteringColumns = Collections.singletonList(new Column("cc", TEXT));
    List<Column> regularColumns = Collections.singletonList(new Column("v", TEXT));
    List<LinkedHashMap<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      LinkedHashMap<String, Object> row = new LinkedHashMap<>();
      row.put("pk", "pk" + i);
      row.put("cc", "cc" + i);
      row.put("v", "v" + i);
      rows.add(row);
    }
    Table table = new Table("table1", partitionKeys, clusteringColumns, regularColumns, rows);
    Keyspace ks = new Keyspace("ks1", table);
    SimulacronUtils.primeTables(simulacron, ks);

    List<TokenRange> goodRanges = new ArrayList<>();
    List<TokenRange> badRanges = new ArrayList<>();

    OptionsMap optionsMap = OptionsMap.driverDefaults();
    optionsMap.put(TypedDriverOption.PROTOCOL_VERSION, "V4");

    Map<TokenRange, SimpleStatement> stmts;
    try (CqlSession session =
        CqlSession.builder()
            .addContactPoint(InetSocketAddress.createUnresolved(hostname, port))
            .withLocalDatacenter(simulacron.dc(0).getName())
            .withConfigLoader(DriverConfigLoader.fromMap(optionsMap))
            .build()) {

      Metadata metadata = session.refreshSchema();

      TableMetadata tableMetadata =
          metadata
              .getKeyspace("ks1")
              .orElseThrow(IllegalStateException::new)
              .getTable("table1")
              .orElseThrow(IllegalStateException::new);

      TokenRangeReadStatementGenerator generator =
          new TokenRangeReadStatementGenerator(tableMetadata, metadata);
      stmts = generator.generate(100);
      int i = 0;
      for (Entry<TokenRange, SimpleStatement> entry : stmts.entrySet()) {
        Query when =
            new Query(
                "SELECT pk,cc,v FROM ks1.table1 WHERE token(pk) > :start AND token(pk) <= :end",
                (String[]) null,
                Maps.newLinkedHashMap(
                    ImmutableMap.of(
                        "start",
                        TokenUtils.getTokenValue(entry.getKey().getStart()),
                        "end",
                        TokenUtils.getTokenValue(entry.getKey().getEnd()))),
                Maps.newLinkedHashMap(ImmutableMap.of("start", "bigint", "end", "bigint")));
        Result then;
        if (++i % 10 == 0) {
          badRanges.add(entry.getKey());
          then = new UnavailableResult(LOCAL_ONE, 1, 0);
        } else {
          goodRanges.add(entry.getKey());
          then = new SuccessResult(rows, table.allColumnTypes());
        }
        simulacron.prime(new Prime(new RequestPrime(when, then)));
      }
    }

    MockConnector.setDelegate(
        new CSVConnector() {

          @Override
          public void configure(
              @NonNull Config settings, boolean read, boolean retainRecordSources) {}

          @Override
          public void init() {}

          @Override
          public int writeConcurrency() {
            return 100;
          }

          @NonNull
          @Override
          public Function<Publisher<Record>, Publisher<Record>> write() {
            return upstream ->
                Flux.from(upstream)
                    .map(
                        record -> {
                          if (record.getPosition() % 10 == 0) {
                            return RecordUtils.error(
                                record.getResource(),
                                record.getPosition(),
                                new IllegalArgumentException(
                                    "Record could not be written: " + record.getSource()));
                          } else {
                            return record;
                          }
                        });
          }
        });

    String[] args = {
      "unload",
      "-c",
      "mock",
      "--log.maxErrors",
      "10000",
      "-maxConcurrentQueries",
      "100",
      "--schema.splits",
      "100",
      "--schema.query",
      "SELECT pk,cc,v FROM ks1.table1 WHERE token(pk) > :start AND token(pk) <= :end"
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);
    assertThat(logs.getAllMessagesAsString())
        .contains("completed with 9,010 errors")
        .contains("Reads: total: 90,010, successful: 90,000, failed: 10")
        // successful: 90 successful ranges * 900 successful records  = 81,000
        // total: 90 successful ranges * 1000 records + 10 failed reads = 90,010
        // failed: 90 * 100 failed records + 10 failed reads = 9,010
        .contains("Records: total: 90,010, successful: 81,000, failed: 9,010");
    validateExceptionsLog(
        10,
        "BulkExecutionException: Statement execution failed: SELECT pk,cc,v FROM ks1.table1 WHERE token(pk) > :start AND token(pk) <= :end",
        "unload-errors.log");
    validateExceptionsLog(9_000, "Record could not be written:", "connector-errors.log");
    validateNumberOfBadRecords(9_000, "connector.bad");

    CheckpointManager manager = getCheckpointManager();

    for (TokenRange range : goodRanges) {
      checkRangeCheckpoint(manager, range, true, 900, 100);
    }
    for (TokenRange range : badRanges) {
      checkRangeCheckpoint(manager, range, false, 0, 0);
    }

    // Resume failed operation

    simulacron.clearPrimes(true);
    simulacron.clearLogs();
    SimulacronUtils.primeTables(simulacron, ks);

    for (Entry<TokenRange, SimpleStatement> entry : stmts.entrySet()) {
      Query when =
          new Query(
              "SELECT pk,cc,v FROM ks1.table1 WHERE token(pk) > :start AND token(pk) <= :end",
              (String[]) null,
              Maps.newLinkedHashMap(
                  ImmutableMap.of(
                      "start",
                      TokenUtils.getTokenValue(entry.getKey().getStart()),
                      "end",
                      TokenUtils.getTokenValue(entry.getKey().getEnd()))),
              Maps.newLinkedHashMap(ImmutableMap.of("start", "bigint", "end", "bigint")));
      Result then = new SuccessResult(rows, table.allColumnTypes());
      simulacron.prime(new Prime(new RequestPrime(when, then)));
    }

    MockConnector.setDelegate(
        new CSVConnector() {

          @Override
          public void configure(
              @NonNull Config settings, boolean read, boolean retainRecordSources) {}

          @Override
          public void init() {}

          @Override
          public int writeConcurrency() {
            return 100;
          }

          @NonNull
          @Override
          public Function<Publisher<Record>, Publisher<Record>> write() {
            return upstream -> upstream;
          }
        });

    Path checkpointFile =
        OperationDirectory.getCurrentOperationDirectory()
            .orElseThrow(IllegalStateException::new)
            .resolve("checkpoint.csv");

    args =
        new String[] {
          "unload",
          "-c",
          "mock",
          "--log.maxErrors",
          "0",
          "-maxConcurrentQueries",
          "100",
          "--schema.splits",
          "100",
          "--schema.query",
          "SELECT pk,cc,v FROM ks1.table1 WHERE token(pk) > :start AND token(pk) <= :end",
          "--log.checkpoint.file",
          quoteJson(checkpointFile.toAbsolutePath()),
          "--log.checkpoint.replayStrategy",
          replayStrategy.name()
        };

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();

    validateCheckpointFile(100_000, replayStrategy, true);

    manager = getCheckpointManager();

    switch (replayStrategy) {
      case resume:
        assertStatus(status, STATUS_COMPLETED_WITH_ERRORS);
        assertThat(logs.getAllMessagesAsString()).contains("completed with 9,000 errors");
        assertThat(logs.getAllMessagesAsString())
            // should have read only failed ranges: 10 * 1000 records = 10,000 records total
            .contains("Reads: total: 10,000, successful: 10,000, failed: 0")
            .contains("Records: total: 100,000, successful: 91,000, failed: 9,000");
        for (TokenRange range : goodRanges) {
          checkRangeCheckpoint(manager, range, true, 900, 100);
        }
        for (TokenRange range : badRanges) {
          checkRangeCheckpoint(manager, range, true, 1000, 0);
        }
        break;
      case retry:
      case rewind:
        assertStatus(status, STATUS_OK);
        assertThat(logs.getAllMessagesAsString()).contains("completed successfully");
        assertThat(logs.getAllMessagesAsString())
            .contains("Reads: total: 100,000, successful: 100,000, failed: 0")
            .contains("Records: total: 100,000, successful: 100,000, failed: 0");
        for (TokenRange range : goodRanges) {
          checkRangeCheckpoint(manager, range, true, 1000, 0);
        }
        for (TokenRange range : badRanges) {
          checkRangeCheckpoint(manager, range, true, 1000, 0);
        }
        break;
    }
  }

  private void checkRangeCheckpoint(
      CheckpointManager manager,
      TokenRange range,
      boolean expectedComplete,
      int expectedSuccessful,
      int expectedFailed) {
    CqlIdentifier ks1 = CqlIdentifier.fromInternal("ks1");
    CqlIdentifier table1 = CqlIdentifier.fromInternal("table1");
    URI resource = RangeReadStatement.rangeReadResource(ks1, table1, range);
    Checkpoint checkpoint = manager.getCheckpoint(resource);
    assertThat(checkpoint.isComplete()).isEqualTo(expectedComplete);
    assertThat(checkpoint.getConsumedSuccessful().sum()).isEqualTo(expectedSuccessful);
    assertThat(checkpoint.getConsumedFailed().sum()).isEqualTo(expectedFailed);
  }

  @Test
  void full_unload() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime select = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(select));

    String[] args = {
      "unload",
      "--log.verbosity",
      "high",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
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
    validateCheckpointFile(
        URI.create("cql://ks1/ip_by_country?start=-9223372036854775808&end=-9223372036854775808"),
        24);
    validateOutputFiles(24, unloadDir);
  }

  @Test
  void full_unload_no_checkpoint() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime select = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(select));

    String[] args = {
      "unload",
      "--log.verbosity",
      "high",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED,
      "--log.checkpoint.enabled",
      "false"
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    assertThat(logs.getAllMessagesAsString())
        .contains("Records: total: 24, successful: 24, failed: 0")
        .contains("Reads: total: 24, successful: 24, failed: 0");
    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, LOCAL_ONE);
    validateOutputFiles(24, unloadDir);
    Path logPath =
        OperationDirectory.getCurrentOperationDirectory().orElseThrow(IllegalStateException::new);
    Path checkpointFile = logPath.resolve("checkpoint.csv");
    assertThat(checkpointFile).doesNotExist();
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
    validateCheckpointFile(
        URI.create("cql://ks1/ip_by_country?start=-9223372036854775808&end=-9223372036854775808"),
        24);
  }

  @Test
  void full_unload_large_result_set() throws Exception {

    primeIpByCountryTable(simulacron);
    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 10_000);
    simulacron.prime(new Prime(prime));

    String[] args = {
      "unload",
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(unloadDir),
      "--connector.csv.maxConcurrentFiles",
      "1C",
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
    validateOutputFiles(10_000, unloadDir);
    validateCheckpointFile(
        URI.create("cql://ks1/ip_by_country?start=-9223372036854775808&end=-9223372036854775808"),
        10_000);
  }

  @Test
  void unload_failure_during_read_single_thread() {

    // Note: the error happens in SchemaSettings initialization, which is why there is no summary
    // file.

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

    // Note: the error happens in SchemaSettings initialization, which is why there is no summary
    // file.

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
      "1C",
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
  void unload_write_fatal_error() throws IOException {

    MockConnector.setDelegate(
        new CSVConnector() {

          @Override
          public void configure(
              @NonNull Config settings, boolean read, boolean retainRecordSources) {
            settings =
                ConfigFactory.parseString(
                        "url = "
                            + quoteJson(unloadDir)
                            + ", header = false, maxConcurrentFiles = 4")
                    .withFallback(
                        ConfigUtils.createReferenceConfig().getConfig("dsbulk.connector.csv"));
            super.configure(settings, read, retainRecordSources);
          }

          @Override
          public int writeConcurrency() {
            return 1;
          }

          @NonNull
          @Override
          public Function<Publisher<Record>, Publisher<Record>> write() {
            // emulate the last record triggering a fatal IO error
            AtomicInteger counter = new AtomicInteger();
            return upstream ->
                Flux.from(upstream)
                    .flatMap(
                        record -> {
                          if (counter.incrementAndGet() == 10) {
                            return Flux.error(new IOException("disk full"));
                          }
                          return Flux.just(record);
                        });
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
    assertThat(stdErr.getStreamAsString()).contains("failed").contains("disk full");
    assertThat(logs.getAllMessagesAsString()).contains("failed").contains("disk full");
    validateCheckpointFile(
        URI.create("cql://ks1/ip_by_country?start=-9223372036854775808&end=-9223372036854775808"),
        1,
        9,
        false,
        ReplayStrategy.resume);
  }

  @Test
  void validate_stdout() throws IOException {

    primeIpByCountryTable(simulacron);
    RequestPrime select = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(select));

    String[] args = {
      "unload",
      "-header",
      "false",
      "--connector.csv.url",
      "-",
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
    validateCheckpointFile(
        URI.create("cql://ks1/ip_by_country?start=-9223372036854775808&end=-9223372036854775808"),
        24);
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
                    new LinkedHashMap<>(
                        ImmutableMap.of("pk", 1, "v", ByteUtils.fromHexString("0xcafebabe")))))));

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
    validateCheckpointFile(
        URI.create("cql://ks1/table1?start=-9223372036854775808&end=-9223372036854775808"), 1);
  }

  private void verifyDelimiterCount(char delimiter, int expected) throws Exception {
    String contents = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.joining("\n"));
    assertThat(CharMatcher.is(delimiter).countIn(contents)).isEqualTo(expected);
  }
}
