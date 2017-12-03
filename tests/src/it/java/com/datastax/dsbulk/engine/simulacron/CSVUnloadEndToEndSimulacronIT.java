/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.createQueryWithError;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.createQueryWithResultSet;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.createQueryWithResultSetWithQuotes;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.fetchContactPoints;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateExceptionsLog;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateOutputFiles;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateQueryCount;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.internal.utils.FileUtils;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.result.SyntaxErrorResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CSVUnloadEndToEndSimulacronIT {

  private final BoundCluster simulacron;

  private Path unloadDir;
  private Path outputFile;

  CSVUnloadEndToEndSimulacronIT(BoundCluster simulacron) {
    this.simulacron = simulacron;
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

  @Test
  void full_unload() throws Exception {

    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      unloadDir.toString(),
      "--connector.csv.maxConcurrentFiles",
      "1 ",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(unloadArgs).run();
    assertThat(status).isZero();

    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ConsistencyLevel.ONE);
    validateOutputFiles(24, outputFile);
  }

  /**
   * This exercises logic which will replace the delimiter and make sure non-standard quoting is
   * working.
   */
  @Test
  void full_unload_csv_default_modification() throws Exception {

    RequestPrime prime = createQueryWithResultSetWithQuotes(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      unloadDir.toString(),
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--connector.csv.delimiter",
      ";",
      "--connector.csv.quote",
      "<",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(unloadArgs).run();
    assertThat(status).isZero();

    verifyDelimiterCount(';', 120);
    verifyDelimiterCount('<', 48);
    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ConsistencyLevel.ONE);
    validateOutputFiles(24, outputFile);
  }

  @Test
  void full_unload_multi_thread() throws Exception {

    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      unloadDir.toString(),
      "--connector.csv.maxConcurrentFiles",
      "4 ",
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.hosts",
      fetchContactPoints(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(unloadArgs).run();
    assertThat(status).isZero();

    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ConsistencyLevel.LOCAL_ONE);
    validateOutputFiles(
        24,
        unloadDir.resolve("output-000001.csv"),
        unloadDir.resolve("output-000002.csv"),
        unloadDir.resolve("output-000003.csv"),
        unloadDir.resolve("output-000004.csv"));
  }

  @Test
  void unload_failure_during_read_single_thread() throws Exception {

    RequestPrime prime =
        createQueryWithError(
            SELECT_FROM_IP_BY_COUNTRY, new SyntaxErrorResult("Invalid table", 0L, true));
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      unloadDir.toString(),
      "--connector.csv.maxConcurrentFiles",
      "1 ",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(unloadArgs).run();
    assertThat(status).isZero();

    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ConsistencyLevel.ONE);
    validateExceptionsLog(1, "Statement:", "unload-errors.log");
    validateOutputFiles(0, outputFile);
  }

  @Test
  void unload_failure_during_read_multi_thread() throws Exception {

    RequestPrime prime =
        createQueryWithError(
            SELECT_FROM_IP_BY_COUNTRY, new SyntaxErrorResult("Invalid table", 0L, true));
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      unloadDir.toString(),
      "--connector.csv.maxConcurrentFiles",
      "4 ",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(unloadArgs).run();
    assertThat(status).isZero();

    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ConsistencyLevel.ONE);
    validateExceptionsLog(1, "Statement:", "unload-errors.log");
    validateOutputFiles(0, outputFile);
  }

  private void verifyDelimiterCount(char delimiter, int expected) throws Exception {
    String contents = FileUtils.readFile(outputFile, UTF_8);
    assertThat(StringUtils.countOccurrences(delimiter, contents)).isEqualTo(expected);
  }
}
