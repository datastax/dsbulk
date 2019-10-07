/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.tests.utils;

import static com.datastax.oss.driver.api.core.type.DataTypes.BIGINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.INET;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.ErrorResult;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;

public class EndToEndUtils {

  public static final String INSERT_INTO_IP_BY_COUNTRY =
      "INSERT INTO ip_by_country "
          + "(country_code, country_name, beginning_ip_address, ending_ip_address, beginning_ip_number, ending_ip_number) "
          + "VALUES (?,?,?,?,?,?)";

  public static final String SELECT_FROM_IP_BY_COUNTRY =
      "SELECT * FROM ip_by_country "
          + "WHERE token(country_code) > :start AND token(country_code) <= :end";

  public static final String SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES =
      "SELECT * FROM \"MYKS\".\"WITH_SPACES\" "
          + "WHERE token(key) > :start AND token(key) <= :end";

  public static final String IP_BY_COUNTRY_MAPPING_INDEXED =
      "0=beginning_ip_address,"
          + "1=ending_ip_address,"
          + "2=beginning_ip_number,"
          + "3=ending_ip_number,"
          + "4=country_code,"
          + "5=country_name";

  public static final String IP_BY_COUNTRY_MAPPING_NAMED =
      "beginning_ip_address=beginning_ip_address,"
          + "ending_ip_address=ending_ip_address,"
          + "beginning_ip_number=beginning_ip_number,"
          + "ending_ip_number=ending_ip_number,"
          + "country_code=country_code,"
          + "country_name=country_name";

  public static final String IP_BY_COUNTRY_MAPPING_CASE_SENSITIVE =
      "0=\"BEGINNING IP ADDRESS\",1=\"ENDING IP ADDRESS\",2=\"BEGINNING IP NUMBER\",3=\"ENDING IP NUMBER\","
          + "4=\"COUNTRY CODE\",5=\"COUNTRY NAME\"";

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

  public static Path getOperationDirectory() {
    return Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
  }

  public static void validateExceptionsLog(int size, String keyword, String fileName)
      throws Exception {
    Path logPath = getOperationDirectory();
    Path exceptionFile = logPath.resolve(fileName);
    try (Stream<String> lines = Files.lines(exceptionFile)) {
      long numErrors = lines.filter(l -> l.contains(keyword)).count();
      assertThat(numErrors).isEqualTo(size);
    }
  }

  public static void validateNumberOfBadRecords(int size) throws Exception {
    Path logPath = getOperationDirectory();
    PathMatcher badFileMatcher = FileSystems.getDefault().getPathMatcher("glob:**/*.bad");
    try (Stream<Path> paths = Files.list(logPath)) {
      long numBadOps =
          paths.filter(badFileMatcher::matches).flatMap(FileUtils::readAllLines).count();
      assertThat(numBadOps).isEqualTo(size);
    }
  }

  public static void validatePositionsFile(Path resource, long lastPosition)
      throws IOException, URISyntaxException {
    validatePositionsFile(resource.toUri().toURL(), lastPosition);
  }

  public static void validatePositionsFile(URL resource, long lastPosition)
      throws IOException, URISyntaxException {
    validatePositionsFile(resource.toURI(), lastPosition);
  }

  public static void validatePositionsFile(URI resource, long lastPosition) throws IOException {
    Path logPath = getOperationDirectory();
    Path positions = logPath.resolve("positions.txt");
    assertThat(positions).exists();
    List<String> lines = Files.readAllLines(positions, UTF_8);
    assertThat(lines).hasSize(1).containsExactly(resource + ":" + lastPosition);
  }

  public static void validatePositionsFile(Map<URI, Long> lastPositions) throws IOException {
    Path logPath = getOperationDirectory();
    Path positions = logPath.resolve("positions.txt");
    assertThat(positions).exists();
    List<String> lines = Files.readAllLines(positions, UTF_8);
    assertThat(lines).hasSize(lastPositions.size());
    for (Entry<URI, Long> entry : lastPositions.entrySet()) {
      assertThat(lines).contains(entry.getKey() + ":" + entry.getValue());
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
            .filter(
                l ->
                    !l.getType().equals("PREPARE")
                        && l.getQuery() != null
                        && l.getQuery().startsWith(query))
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
                new SimulacronUtils.Column("country_code", TEXT),
                new SimulacronUtils.Column("beginning_ip_address", INET),
                new SimulacronUtils.Column("country_name", TEXT),
                new SimulacronUtils.Column("ending_ip_address", INET),
                new SimulacronUtils.Column("beginning_ip_number", BIGINT),
                new SimulacronUtils.Column("ending_ip_number", BIGINT))));
  }

  public static void createIpByCountryTable(CqlSession session) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS ip_by_country ("
            + "country_code varchar,"
            + "country_name varchar static,"
            + "beginning_ip_address inet,"
            + "ending_ip_address inet,"
            + "beginning_ip_number bigint,"
            + "ending_ip_number bigint,"
            + "PRIMARY KEY(country_code, beginning_ip_address))");
  }

  public static void createIpByCountryTable(CqlSession session, String keyspace) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS "
            + keyspace
            + ".ip_by_country ("
            + "country_code varchar,"
            + "country_name varchar static,"
            + "beginning_ip_address inet,"
            + "ending_ip_address inet,"
            + "beginning_ip_number bigint,"
            + "ending_ip_number bigint,"
            + "PRIMARY KEY(country_code, beginning_ip_address))");
  }

  public static void createWithSpacesTable(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"MYKS\" "
            + "WITH replication = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 }");
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"MYKS\".\"WITH_SPACES\" ("
            + "key int PRIMARY KEY, \"my destination\" text)");
  }

  public static void createIpByCountryCaseSensitiveTable(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"MYKS\" "
            + "WITH replication = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 }");
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"MYKS\".\"IPBYCOUNTRY\" ("
            + "\"COUNTRY CODE\" varchar,"
            + "\"COUNTRY NAME\" varchar static,"
            + "\"BEGINNING IP ADDRESS\" inet,"
            + "\"ENDING IP ADDRESS\" inet,"
            + "\"BEGINNING IP NUMBER\" bigint,"
            + "\"ENDING IP NUMBER\" bigint,"
            + "PRIMARY KEY(\"COUNTRY CODE\", \"BEGINNING IP ADDRESS\"))");
  }
}
