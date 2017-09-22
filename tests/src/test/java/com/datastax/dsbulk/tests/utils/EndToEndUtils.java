/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.utils;

import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.tests.SimulacronRule;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.ErrorResult;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;

@SuppressWarnings("SameParameterValue")
public class EndToEndUtils {

  public static RequestPrime createSimpleParametrizedQuery(String query) {
    Map<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("country_code", "ascii");
    paramTypes.put("country_name", "ascii");
    paramTypes.put("beginning_ip_address", "inet");
    paramTypes.put("ending_ip_address", "inet");
    paramTypes.put("beginning_ip_number", "bigint");
    paramTypes.put("ending_ip_number", "bigint");
    Query when = new Query(query, Collections.emptyList(), new HashMap<>(), paramTypes);
    SuccessResult then = new SuccessResult(new ArrayList<>(), new HashMap<>());
    return new RequestPrime(when, then);
  }

  public static RequestPrime createQueryWithResultSet(String query, int numOfResults) {
    Query when = new Query(query, Collections.emptyList(), new HashMap<>(), new HashMap<>());

    Map<String, String> columnTypes = new LinkedHashMap<>();

    columnTypes.put("country_name", "ascii");
    columnTypes.put("beginning_ip_address", "inet");
    columnTypes.put("ending_ip_address", "inet");
    columnTypes.put("beginning_ip_number", "bigint");
    columnTypes.put("ending_ip_number", "bigint");
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < numOfResults; i++) {
      HashMap<String, Object> row = new HashMap<>();
      row.put("country_name", "country" + Integer.toString(i));
      row.put("beginning_ip_address", "127.0.0." + Integer.toString(i));
      row.put("ending_ip_address", "127.2.0." + Integer.toString(i));
      row.put("beginning_ip_number", Integer.toString(i));
      row.put("ending_ip_number", Integer.toString(i));
      rows.add(row);
    }

    SuccessResult then = new SuccessResult(rows, columnTypes);
    return new RequestPrime(when, then);
  }

  public static RequestPrime createQueryWithResultSetWithQuotes(String query, int numOfResults) {
    Query when = new Query(query, Collections.emptyList(), new HashMap<>(), new HashMap<>());

    Map<String, String> columnTypes = new LinkedHashMap<>();

    columnTypes.put("country_name", "ascii");
    columnTypes.put("beginning_ip_address", "inet");
    columnTypes.put("ending_ip_address", "inet");
    columnTypes.put("beginning_ip_number", "bigint");
    columnTypes.put("ending_ip_number", "bigint");
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < numOfResults; i++) {
      HashMap<String, Object> row = new HashMap<>();
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
    Query when = new Query(query, Collections.emptyList(), new HashMap<>(), new HashMap<>());

    return new RequestPrime(when, result);
  }

  public static RequestPrime createParametrizedQuery(
      String query, Map<String, Object> params, Result then) {
    Map<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("country_code", "ascii");
    paramTypes.put("country_name", "ascii");
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

  public static void validateExceptionsLog(int size, String keyword, String file_name)
      throws Exception {
    Path logPath = getLogDirectory();
    Path exceptionFile = logPath.resolve(file_name);
    List<String> exceptionLines =
        Files.lines(exceptionFile, Charset.defaultCharset()).collect(Collectors.toList());
    long numErrors = exceptionLines.stream().filter(l -> l.startsWith(keyword)).count();
    Assertions.assertThat(numErrors).isEqualTo(size);
  }

  private static Path getLogDirectory() {
    String logPath = System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY);
    return Paths.get(logPath);
  }

  public static void validateBadOps(int size) throws Exception {
    Path logPath = getLogDirectory();
    Path badOps = logPath.resolve("operation.bad");
    long numBadOps = Files.lines(badOps, Charset.defaultCharset()).count();
    Assertions.assertThat(numBadOps).isEqualTo(size);
  }

  public static void validateOutputFiles(int numOfRecords, Path... outputFilePaths) {
    // Sum the number of lines in each file and assert that the total matches the expected value.
    long totalLines =
        Arrays.stream(outputFilePaths)
            .mapToLong(
                path -> {
                  try {
                    return Files.lines(path, Charset.defaultCharset()).count();
                  } catch (IOException e) {
                    // This should never happen. Emit the stack trace, but in case
                    // it's not visible in the test output, make the test fail
                    // in an extremely blatant way -- make the total line count negative.
                    e.printStackTrace();
                    return -100000;
                  }
                })
            .sum();
    Assertions.assertThat(totalLines).isEqualTo(numOfRecords);
  }

  public static void validateStringOutput(String output, int numOfRecords) {
    String lines[] = output.split(System.lineSeparator());
    Assertions.assertThat(lines.length).isEqualTo(numOfRecords);
  }

  public static String fetchSimulacronContactPointsForArg(SimulacronRule simulacron) {
    return simulacron.getContactPoints().iterator().next().toString().substring(1);
  }

  public static void validateQueryCount(
      SimulacronRule simulacron, int numOfQueries, String query, ConsistencyLevel level) {
    List<QueryLog> logs = simulacron.cluster().getLogs().getQueryLogs();
    List<QueryLog> ipLogs =
        logs.stream().filter(l -> l.getQuery().startsWith(query)).collect(Collectors.toList());
    Assertions.assertThat(ipLogs.size()).isEqualTo(numOfQueries);
    for (QueryLog log : ipLogs) {
      Assertions.assertThat(log.getConsistency()).isEqualTo(level);
    }
  }

  public static void setURLFactoryIfNeeded() {
    try {
      URL.setURLStreamHandlerFactory(new LoaderURLStreamHandlerFactory());
    } catch (Exception e) {
      //URL.setURLStreamHandlerFactory throws an exception if it's been set more then once
      //Ignore that and keep going.
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static void deleteIfExists(Path filepath) throws IOException {
    if (Files.exists(filepath)) {
      Files.walkFileTree(
          filepath,
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Files.delete(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              Files.delete(dir);
              return FileVisitResult.CONTINUE;
            }
          });
    }
  }
}
