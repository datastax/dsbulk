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
package com.datastax.oss.dsbulk.runner.tests;

import static com.datastax.oss.driver.api.core.type.DataTypes.BIGINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.INET;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.runner.ExitStatus;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.workflow.api.log.OperationDirectory;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.Checkpoint;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.CheckpointManager;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.Range;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.RangeSet;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.ReplayStrategy;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.ErrorResult;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.server.BoundCluster;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("country_code", "varchar");
    paramTypes.put("country_name", "varchar");
    paramTypes.put("beginning_ip_address", "inet");
    paramTypes.put("ending_ip_address", "inet");
    paramTypes.put("beginning_ip_number", "bigint");
    paramTypes.put("ending_ip_number", "bigint");
    Query when = new Query(query, Collections.emptyList(), new LinkedHashMap<>(), paramTypes);
    SuccessResult then = new SuccessResult(new ArrayList<>(), new LinkedHashMap<>());
    return new RequestPrime(when, then);
  }

  public static RequestPrime createQueryWithResultSet(String query, int numOfResults) {
    Query when = new Query(query);
    LinkedHashMap<String, String> columnTypes = new LinkedHashMap<>();
    columnTypes.put("country_code", "varchar");
    columnTypes.put("country_name", "varchar");
    columnTypes.put("beginning_ip_address", "inet");
    columnTypes.put("ending_ip_address", "inet");
    columnTypes.put("beginning_ip_number", "bigint");
    columnTypes.put("ending_ip_number", "bigint");
    List<LinkedHashMap<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < numOfResults; i++) {
      LinkedHashMap<String, Object> row = new LinkedHashMap<>();
      row.put("country_code", "country" + i);
      row.put("country_name", "country" + i);
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
    LinkedHashMap<String, String> columnTypes = new LinkedHashMap<>();
    columnTypes.put("country_code", "varchar");
    columnTypes.put("country_name", "varchar");
    columnTypes.put("beginning_ip_address", "inet");
    columnTypes.put("ending_ip_address", "inet");
    columnTypes.put("beginning_ip_number", "bigint");
    columnTypes.put("ending_ip_number", "bigint");
    List<LinkedHashMap<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < numOfResults; i++) {
      LinkedHashMap<String, Object> row = new LinkedHashMap<>();
      row.put("country_code", "country" + ";" + i);
      row.put("country_name", "country" + ";" + i);
      row.put("beginning_ip_address", "127.0.0." + i);
      row.put("ending_ip_address", "127.2.0." + i);
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
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("country_code", "varchar");
    paramTypes.put("country_name", "varchar");
    paramTypes.put("beginning_ip_address", "inet");
    paramTypes.put("ending_ip_address", "inet");
    paramTypes.put("beginning_ip_number", "bigint");
    paramTypes.put("ending_ip_number", "bigint");

    LinkedHashMap<String, Object> defaultParams = new LinkedHashMap<>();
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

  public static void validateExceptionsLog(int size, String keyword, String fileName)
      throws Exception {
    Path logPath =
        OperationDirectory.getCurrentOperationDirectory().orElseThrow(IllegalStateException::new);
    Path exceptionFile = logPath.resolve(fileName);
    try (Stream<String> lines = Files.lines(exceptionFile)) {
      long numErrors = lines.filter(l -> l.contains(keyword)).count();
      assertThat(numErrors).isEqualTo(size);
    }
  }

  public static void assertStatus(ExitStatus actual, ExitStatus expected) {
    StringWriter sw = new StringWriter();
    if (actual != expected) {
      PrintWriter pw = new PrintWriter(sw);
      pw.printf("Expected exit status %s, but got: %s%n", expected.name(), actual.name());
      try {
        Map<String, String> filesContent = new LinkedHashMap<>();
        filesContent.put("operation.log", getFileContent("operation.log"));
        addErrorFilesContent(filesContent);
        String delimiter = "------------------------------------------";
        filesContent.forEach(
            (fileName, content) -> {
              pw.println(delimiter);
              pw.println(fileName + ":");
              pw.println(content);
            });
        pw.println(delimiter);
      } catch (IOException e) {
        pw.println("Failed to retrieve error logs: ");
        e.printStackTrace(pw);
      }
    }
    assertThat(actual).withFailMessage(sw.toString()).isEqualTo(expected);
  }

  private static String getFileContent(String fileName) {
    Path logPath =
        OperationDirectory.getCurrentOperationDirectory().orElseThrow(IllegalStateException::new);
    Path exceptionFile = logPath.resolve(fileName);
    try {
      return FileUtils.readFile(exceptionFile);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void addErrorFilesContent(Map<String, String> result) throws IOException {
    Path logPath =
        OperationDirectory.getCurrentOperationDirectory().orElseThrow(IllegalStateException::new);
    // find all available -errors.log files
    try (Stream<Path> stream = Files.walk(logPath, 1)) {
      stream
          .filter(file -> !Files.isDirectory(file))
          .map(Path::getFileName)
          .map(Path::toString)
          .filter(s -> s.endsWith("-errors.log"))
          .forEach(fileName -> result.put(fileName, getFileContent(fileName)));
    }
  }

  public static void validateNumberOfBadRecords(int size) throws Exception {
    Path logPath =
        OperationDirectory.getCurrentOperationDirectory().orElseThrow(IllegalStateException::new);
    PathMatcher badFileMatcher = FileSystems.getDefault().getPathMatcher("glob:**/*.bad");
    try (Stream<Path> paths = Files.list(logPath)) {
      long numBadOps =
          paths.filter(badFileMatcher::matches).flatMap(FileUtils::readAllLines).count();
      assertThat(numBadOps).isEqualTo(size);
    }
  }

  public static void validateNumberOfBadRecords(int size, String fileName) {
    Path logPath =
        OperationDirectory.getCurrentOperationDirectory().orElseThrow(IllegalStateException::new);
    Path badPath = logPath.resolve(fileName);
    long numBadOps = FileUtils.readAllLines(badPath).count();
    assertThat(numBadOps).isEqualTo(size);
  }

  public static void validateCheckpointFile(long expectedTotal) throws IOException {
    validateCheckpointFile(expectedTotal, ReplayStrategy.resume, true);
  }

  public static void validateCheckpointFile(
      long expectedTotal, ReplayStrategy replayStrategy, boolean onlyCompleted) throws IOException {
    CheckpointManager manager = getCheckpointManager();
    if (onlyCompleted) {
      assertThat(manager.isComplete(replayStrategy)).isTrue();
    }
    long actualTotal = 0;
    @SuppressWarnings("unchecked")
    Map<URI, Checkpoint> checkpoints =
        (Map<URI, Checkpoint>) ReflectionUtils.getInternalState(manager, "checkpoints");
    for (Checkpoint checkpoint : checkpoints.values()) {
      actualTotal +=
          checkpoint.getConsumedSuccessful().sum() + checkpoint.getConsumedFailed().sum();
    }
    assertThat(actualTotal).isEqualTo(expectedTotal);
  }

  public static void validateCheckpointFile(Path resource, long lastPosition)
      throws IOException, URISyntaxException {
    validateCheckpointFile(resource.toUri().toURL(), lastPosition);
  }

  public static void validateCheckpointFile(URL resource, long lastPosition)
      throws IOException, URISyntaxException {
    validateCheckpointFile(resource.toURI(), lastPosition);
  }

  public static void validateCheckpointFile(URI resource, long lastPosition) throws IOException {
    validateCheckpointFile(resource, 1, lastPosition);
  }

  public static void validateCheckpointFile(URI resource, long firstPosition, long lastPosition)
      throws IOException {
    validateCheckpointFile(resource, firstPosition, lastPosition, true, ReplayStrategy.resume);
  }

  public static void validateCheckpointFile(
      URI resource,
      long firstPosition,
      long lastPosition,
      boolean complete,
      ReplayStrategy replayStrategy)
      throws IOException {
    CheckpointManager manager = getCheckpointManager();
    Checkpoint checkpoint = manager.getCheckpoint(resource);
    assertThat(replayStrategy.isComplete(checkpoint)).isEqualTo(complete);
    RangeSet consumedSuccessful = checkpoint.getConsumedSuccessful();
    RangeSet consumedFailed = checkpoint.getConsumedFailed();
    RangeSet merged = RangeSet.of();
    merged.merge(consumedSuccessful);
    merged.merge(consumedFailed);
    if (firstPosition == 0 && lastPosition == 0) {
      assertThat(merged.isEmpty()).isTrue();
    } else {
      Range range = merged.iterator().next();
      assertThat(range.getLower()).isEqualTo(firstPosition);
      assertThat(range.getUpper()).isEqualTo(lastPosition);
    }
  }

  @NonNull
  public static CheckpointManager getCheckpointManager() throws IOException {
    Path logPath =
        OperationDirectory.getCurrentOperationDirectory().orElseThrow(IllegalStateException::new);
    Path checkpointFile = logPath.resolve("checkpoint.csv");
    assertThat(checkpointFile).exists();
    return CheckpointManager.parse(Files.newBufferedReader(checkpointFile, UTF_8));
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
            + "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"MYKS\".\"WITH_SPACES\" ("
            + "key int PRIMARY KEY, \"my destination\" text)");
  }

  public static void createIpByCountryCaseSensitiveTable(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"MYKS\" "
            + "WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
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
