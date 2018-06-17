/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.tests.utils;

import static com.datastax.driver.core.DataType.bigint;
import static com.datastax.driver.core.DataType.inet;
import static com.datastax.driver.core.DataType.varchar;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.ErrorResult;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;

public class EndToEndUtils {

  public static RequestPrime createSimpleParameterizedQuery(String query) {
    Map<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("country_code", "varchar");
    paramTypes.put("country_name", "varchar");
    paramTypes.put("beginning_ip_address", "inet");
    paramTypes.put("ending_ip_address", "inet");
    paramTypes.put("beginning_ip_number", "bigint");
    paramTypes.put("ending_ip_number", "bigint");
    Query when = new Query(query, Collections.emptyList(), new HashMap<>(), paramTypes);
    SuccessResult then = new SuccessResult(new ArrayList<>(), new HashMap<>());
    return new RequestPrime(when, then);
  }

  public static RequestPrime createQueryWithResultSet(String query, int numOfResults) {
    Query when = new Query(query);
    Map<String, String> columnTypes = new LinkedHashMap<>();
    columnTypes.put("country_code", "varchar");
    columnTypes.put("country_name", "varchar");
    columnTypes.put("beginning_ip_address", "inet");
    columnTypes.put("ending_ip_address", "inet");
    columnTypes.put("beginning_ip_number", "bigint");
    columnTypes.put("ending_ip_number", "bigint");
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < numOfResults; i++) {
      HashMap<String, Object> row = new HashMap<>();
      row.put("country_code", "country" + Integer.toString(i));
      row.put("country_name", "country" + Integer.toString(i));
      row.put("beginning_ip_address", "127.0.0.1");
      row.put("ending_ip_address", "127.2.0.1");
      row.put("beginning_ip_number", Integer.toString(i));
      row.put("ending_ip_number", Integer.toString(i));
      rows.add(row);
    }
    SuccessResult then = new SuccessResult(rows, columnTypes);
    return new RequestPrime(when, then);
  }

  public static RequestPrime createQueryWithResultSetWithQuotes(String query, int numOfResults) {
    Query when = new Query(query);
    Map<String, String> columnTypes = new LinkedHashMap<>();
    columnTypes.put("country_code", "varchar");
    columnTypes.put("country_name", "varchar");
    columnTypes.put("beginning_ip_address", "inet");
    columnTypes.put("ending_ip_address", "inet");
    columnTypes.put("beginning_ip_number", "bigint");
    columnTypes.put("ending_ip_number", "bigint");
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < numOfResults; i++) {
      HashMap<String, Object> row = new HashMap<>();
      row.put("country_code", "country" + ";" + Integer.toString(i));
      row.put("country_name", "country" + ";" + Integer.toString(i));
      row.put("beginning_ip_address", "127.0.0." + Integer.toString(i));
      row.put("ending_ip_address", "127.2.0." + Integer.toString(i));
      row.put("beginning_ip_number", Integer.toString(i));
      row.put("ending_ip_number", Integer.toString(i));
      rows.add(row);
    }
    SuccessResult then = new SuccessResult(rows, columnTypes);
    return new RequestPrime(when, then);
  }

  public static RequestPrime createQueryWithError(String query, ErrorResult result) {
    Query when = new Query(query);
    return new RequestPrime(when, result);
  }

  public static RequestPrime createParameterizedQuery(
      String query, Map<String, Object> params, Result then) {
    Map<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("country_code", "varchar");
    paramTypes.put("country_name", "varchar");
    paramTypes.put("beginning_ip_address", "inet");
    paramTypes.put("ending_ip_address", "inet");
    paramTypes.put("beginning_ip_number", "bigint");
    paramTypes.put("ending_ip_number", "bigint");

    Map<String, Object> defaultParams = new LinkedHashMap<>();
    defaultParams.put("country_code", "*");
    defaultParams.put("country_name", "*");
    defaultParams.put("beginning_ip_address", "*");
    defaultParams.put("ending_ip_address", "*");
    defaultParams.put("beginning_ip_number", "*");
    defaultParams.put("ending_ip_number", "*");

    for (String key : params.keySet()) {
      defaultParams.put(key, params.get(key));
    }

    Query when = new Query(query, Collections.emptyList(), defaultParams, paramTypes);

    return new RequestPrime(when, then);
  }

  public static void validateExceptionsLog(int size, String keyword, String fileName, Path logPath)
      throws Exception {
    Path exceptionFile = logPath.resolve(fileName);
    try (Stream<String> lines = Files.lines(exceptionFile)) {
      long numErrors = lines.filter(l -> l.contains(keyword)).count();
      assertThat(numErrors).isEqualTo(size);
    }
  }

  public static void validateBadOps(int size, Path logPath) throws Exception {
    PathMatcher badFileMatcher = FileSystems.getDefault().getPathMatcher("glob:**/*.bad");
    try (Stream<Path> paths = Files.list(logPath)) {
      long numBadOps =
          paths.filter(badFileMatcher::matches).flatMap(FileUtils::readAllLines).count();
      assertThat(numBadOps).isEqualTo(size);
    }
  }

  public static void validateOutputFiles(int numOfRecords, Path dir) throws IOException {
    try (Stream<String> lines = FileUtils.readAllLinesInDirectoryAsStream(dir)) {
      // Sum the number of lines in each file and assert that the total matches the expected value.
      long totalLines = lines.count();
      assertThat(totalLines).isEqualTo(numOfRecords);
    }
  }

  public static void validateQueryCount(
      BoundCluster simulacron, int numOfQueries, String query, ConsistencyLevel level) {
    List<QueryLog> logs = simulacron.getLogs().getQueryLogs();
    List<QueryLog> ipLogs =
        logs.stream()
            .filter(l -> !l.getType().equals("PREPARE") && l.getQuery().startsWith(query))
            .collect(Collectors.toList());
    assertThat(ipLogs.size()).isEqualTo(numOfQueries);
    for (QueryLog log : ipLogs) {
      Assertions.assertThat(log.getConsistency()).isEqualTo(level);
    }
  }

  public static void validatePrepare(BoundCluster simulacron, String query) {
    List<QueryLog> logs = simulacron.getLogs().getQueryLogs();
    List<QueryLog> ipLogs =
        logs.stream()
            .filter(l -> l.getType().equals("PREPARE") && l.getQuery().startsWith(query))
            .collect(Collectors.toList());
    assertThat(ipLogs.size()).isEqualTo(1);
  }

  public static void primeIpByCountryTable(BoundCluster simulacron) {
    SimulacronUtils.primeTables(
        simulacron,
        new SimulacronUtils.Keyspace(
            "ks1",
            new SimulacronUtils.Table(
                "ip_by_country",
                new SimulacronUtils.Column("country_code", varchar()),
                new SimulacronUtils.Column("beginning_ip_address", inet()),
                new SimulacronUtils.Column("country_name", varchar()),
                new SimulacronUtils.Column("ending_ip_address", inet()),
                new SimulacronUtils.Column("beginning_ip_number", bigint()),
                new SimulacronUtils.Column("ending_ip_number", bigint()))));
  }
}
