/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.INSERT_INTO_IP_BY_COUNTRY;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.commons.tests.utils.EndToEndUtils.createParameterizedQuery;
import static com.datastax.dsbulk.commons.tests.utils.EndToEndUtils.createSimpleParametrizedQuery;
import static com.datastax.dsbulk.commons.tests.utils.EndToEndUtils.fetchContactPoints;
import static com.datastax.dsbulk.commons.tests.utils.EndToEndUtils.validateBadOps;
import static com.datastax.dsbulk.commons.tests.utils.EndToEndUtils.validateExceptionsLog;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_CRLF;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_ERROR;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_LONG;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_PARTIAL_BAD;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_PARTIAL_BAD_LONG;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_SKIP;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.event.Level.ERROR;

import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.utils.EndToEndUtils;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.result.FunctionFailureResult;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.result.UnavailableResult;
import com.datastax.oss.simulacron.common.result.WriteFailureResult;
import com.datastax.oss.simulacron.common.result.WriteTimeoutResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(LogInterceptingExtension.class)
class CSVLoadEndToEndSimulacronIT {

  private final BoundCluster boundCluster;

  CSVLoadEndToEndSimulacronIT(BoundCluster boundCluster) {
    this.boundCluster = boundCluster;
  }

  @BeforeEach
  void primeQueries() {
    RequestPrime prime = createSimpleParametrizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    boundCluster.prime(new Prime(prime));
  }

  @AfterEach
  void resetLogbackConfiguration() throws JoranException {
    EndToEndUtils.resetLogbackConfiguration();
  }

  @Test
  void full_load() throws Exception {
    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_UNIQUE.toExternalForm(),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(boundCluster),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(args).run();
    assertThat(status).isZero();
    validateQueryCount(24, ConsistencyLevel.ONE);
  }

  @Test
  void full_load_dry_run() throws Exception {
    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_UNIQUE.toExternalForm(),
      "-dryRun",
      "true",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(boundCluster),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(args).run();
    assertThat(status).isZero();

    validateQueryCount(0, ConsistencyLevel.ONE);
  }

  @Test
  void full_load_crlf() throws Exception {

    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_CRLF.toExternalForm(),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(boundCluster),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(args).run();
    assertThat(status).isZero();

    validateQueryCount(24, ConsistencyLevel.ONE);
  }

  @Test
  void partial_load() throws Exception {

    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_PARTIAL_BAD.toExternalForm(),
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.hosts",
      fetchContactPoints(boundCluster),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(args).run();
    assertThat(status).isZero();

    validateQueryCount(21, LOCAL_ONE);
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateBadOps(3, logPath);
    validateExceptionsLog(3, "Source  :", "mapping-errors.log", logPath);
  }

  @Test
  void load_errors() throws Exception {
    boundCluster.clearPrimes(true);

    HashMap<String, Object> params = new HashMap<>();
    params.put("country_name", "Sweden");
    RequestPrime prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new SuccessResult(new ArrayList<>(), new HashMap<>()));
    boundCluster.prime(new Prime(prime1));

    // recoverable errors only

    params.put("country_name", "France");
    prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY, params, new UnavailableResult(LOCAL_ONE, 1, 0));
    boundCluster.prime(new Prime(prime1));

    params.put("country_name", "Gregistan");
    prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new WriteTimeoutResult(ConsistencyLevel.ONE, 0, 0, WriteType.BATCH));
    boundCluster.prime(new Prime(prime1));

    params.put("country_name", "Andybaijan");
    prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new WriteFailureResult(ConsistencyLevel.ONE, 0, 0, new HashMap<>(), WriteType.BATCH));
    boundCluster.prime(new Prime(prime1));

    params = new HashMap<>();
    params.put("country_name", "United States");
    prime1 =
        createParameterizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new FunctionFailureResult(
                "keyspace", "function", new ArrayList<>(), "bad function call"));
    boundCluster.prime(new Prime(prime1));

    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_ERROR.toExternalForm(),
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.policy.maxRetries",
      "1",
      "--driver.hosts",
      fetchContactPoints(boundCluster),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    // There are 24 rows of data, but two extra queries due to the retry for the write timeout and
    // the unavailable.
    int status = new Main(args).run();
    assertThat(status).isZero();

    validateQueryCount(26, LOCAL_ONE);
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateBadOps(4, logPath);
    validateExceptionsLog(4, "Source  :", "load-errors.log", logPath);
  }

  @Test
  void skip_test_load() throws Exception {

    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_SKIP.toExternalForm(),
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.hosts",
      fetchContactPoints(boundCluster),
      "--driver.pooling.local.connections",
      "1",
      "--connector.csv.skipRecords",
      "3",
      "--connector.csv.maxRecords",
      "24",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(args).run();
    assertThat(status).isZero();

    validateQueryCount(21, LOCAL_ONE);
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateBadOps(3, logPath);
    validateExceptionsLog(3, "Source  :", "mapping-errors.log", logPath);
  }

  @Test
  void load_long_column() throws Exception {
    // This will attempt to load a CSV file with column longer then 4096 characters.
    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_LONG.toExternalForm(),
      "--connector.csv.maxCharsPerColumn",
      "10000",
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.hosts",
      fetchContactPoints(boundCluster),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(args).run();
    assertThat(status).isZero();

    validateQueryCount(1, LOCAL_ONE);
  }

  @Test
  void error_load_percentage(@LogCapture(value = Main.class, level = ERROR) LogInterceptor logs)
      throws Exception {
    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "--log.maxErrors",
      "1%",
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_PARTIAL_BAD_LONG.toExternalForm(),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(boundCluster),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING,
      "--batch.enabled",
      "false"
    };
    int status = new Main(args).run();
    assertThat(status).isZero();
    assertThat(logs.getAllMessagesAsString())
        .contains("failed: Too many errors, the maximum percentage allowed is 1.0%");
  }

  private void validateQueryCount(int numOfQueries, ConsistencyLevel level) {
    EndToEndUtils.validateQueryCount(
        boundCluster, numOfQueries, "INSERT INTO ip_by_country", level);
  }
}
