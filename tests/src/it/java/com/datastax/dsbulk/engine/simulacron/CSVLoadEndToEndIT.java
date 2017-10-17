/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.tests.utils.CsvUtils.INSERT_INTO_IP_BY_COUNTRY;

import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.SimulacronRule;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import com.datastax.dsbulk.tests.utils.EndToEndUtils;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.result.AlreadyExistsResult;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.result.SyntaxErrorResult;
import com.datastax.oss.simulacron.common.result.WriteTimeoutResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import java.util.ArrayList;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class CSVLoadEndToEndIT {

  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Before
  public void primeStatements() {
    RequestPrime prime = EndToEndUtils.createSimpleParametrizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.cluster().prime(new Prime(prime));
  }

  @Test
  public void full_load() throws Exception {
    String[] args = {
      "load",
      "--log.directory",
      "./target",
      "-header",
      "false",
      "--connector.csv.url",
      CsvUtils.CSV_RECORDS_UNIQUE.toExternalForm(),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--driver.protocol.compression",
      "NONE",
      "--schema.query",
      CsvUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(args).run();
    validateQueryCount(24, ConsistencyLevel.ONE);
  }

  @Test
  public void full_load_crlf() throws Exception {

    String[] args = {
      "load",
      "--log.directory",
      "\"./target\"",
      "-header",
      "false",
      "--connector.csv.url",
      CsvUtils.CSV_RECORDS_CRLF.toExternalForm(),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--driver.protocol.compression",
      "NONE",
      "--schema.query",
      CsvUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(args).run();
    validateQueryCount(24, ConsistencyLevel.ONE);
  }

  @Test
  public void partial_load() throws Exception {

    String[] args = {
      "load",
      "--log.directory",
      "./target",
      "-header",
      "false",
      "--connector.csv.url",
      CsvUtils.CSV_RECORDS_PARTIAL_BAD.toExternalForm(),
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.hosts",
      EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--driver.protocol.compression",
      "NONE",
      "--schema.query",
      CsvUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(args).run();
    validateQueryCount(21, ConsistencyLevel.LOCAL_ONE);
    EndToEndUtils.validateBadOps(3);
    EndToEndUtils.validateExceptionsLog(3, "Source  :", "record-mapping-errors.log");
  }

  @Test
  public void load_errors() throws Exception {
    simulacron.cluster().clearPrimes(true);

    HashMap<String, Object> params = new HashMap<>();
    params.put("country_name", "Sweden");
    RequestPrime prime1 =
        EndToEndUtils.createParametrizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new SuccessResult(new ArrayList<>(), new HashMap<>()));
    simulacron.cluster().prime(new Prime(prime1));

    params.put("country_name", "France");
    prime1 =
        EndToEndUtils.createParametrizedQuery(
            INSERT_INTO_IP_BY_COUNTRY, params, new SyntaxErrorResult("France is not a keyword"));
    simulacron.cluster().prime(new Prime(prime1));

    params.put("country_name", "Gregistan");
    prime1 =
        EndToEndUtils.createParametrizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new WriteTimeoutResult(ConsistencyLevel.ONE, 0, 0, WriteType.BATCH));
    simulacron.cluster().prime(new Prime(prime1));

    params.put("country_name", "Andybaijan");
    prime1 =
        EndToEndUtils.createParametrizedQuery(
            INSERT_INTO_IP_BY_COUNTRY,
            params,
            new AlreadyExistsResult("Not a real country", "keyspace", "table"));
    simulacron.cluster().prime(new Prime(prime1));

    params = new HashMap<>();
    params.put("country_name", "United States");
    prime1 =
        EndToEndUtils.createParametrizedQuery(
            INSERT_INTO_IP_BY_COUNTRY, params, new SyntaxErrorResult("USA is not keyword"));
    simulacron.cluster().prime(new Prime(prime1));

    String[] args = {
      "load",
      "--log.directory",
      "./target",
      "-header",
      "false",
      "--connector.csv.url",
      CsvUtils.CSV_RECORDS_ERROR.toExternalForm(),
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.policy.maxRetries",
      "1",
      "--driver.hosts",
      "" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--driver.protocol.compression",
      "NONE",
      "--schema.query",
      CsvUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    // There are 24 rows of data, but one extra query due to the retry for the write timeout.
    new Main(args).run();
    validateQueryCount(25, ConsistencyLevel.LOCAL_ONE);
    EndToEndUtils.validateBadOps(4);
    EndToEndUtils.validateExceptionsLog(4, "Source  :", "load-errors.log");
  }

  @Test
  public void skip_test_load() throws Exception {

    String[] args = {
      "load",
      "--log.directory",
      "./target",
      "-header",
      "false",
      "--connector.csv.url",
      CsvUtils.CSV_RECORDS_SKIP.toExternalForm(),
      "--driver.query.consistency",
      "LOCAL_ONE",
      "--driver.hosts",
      "" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--driver.protocol.compression",
      "NONE",
      "--connector.csv.skipLines=3",
      "--connector.csv.maxLines",
      "24",
      "--schema.query",
      CsvUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      "{0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };
    new Main(args).run();
    validateQueryCount(21, ConsistencyLevel.LOCAL_ONE);
    EndToEndUtils.validateBadOps(3);
    EndToEndUtils.validateExceptionsLog(3, "Source  :", "record-mapping-errors.log");
  }

  private void validateQueryCount(int numOfQueries, ConsistencyLevel level) {
    EndToEndUtils.validateQueryCount(simulacron, numOfQueries, "INSERT INTO ip_by_country", level);
  }
}
