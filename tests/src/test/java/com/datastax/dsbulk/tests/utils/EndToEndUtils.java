/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.utils;

import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.tests.SimulacronRule;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.ErrorResult;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;

public class EndToEndUtils {
  public static RequestPrime createSimpleParameterizedQuery(String query) {
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

    Map<String, String> ColumnTypes = new LinkedHashMap<>();

    ColumnTypes.put("country_name", "ascii");
    ColumnTypes.put("beginning_ip_address", "inet");
    ColumnTypes.put("ending_ip_address", "inet");
    ColumnTypes.put("beginning_ip_number", "bigint");
    ColumnTypes.put("ending_ip_number", "bigint");
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

    SuccessResult then = new SuccessResult(rows, ColumnTypes);
    return new RequestPrime(when, then);
  }

  public static RequestPrime createQueryWithResultSetWithQuotes(String query, int numOfResults) {
    Query when = new Query(query, Collections.emptyList(), new HashMap<>(), new HashMap<>());

    Map<String, String> ColumnTypes = new LinkedHashMap<>();

    ColumnTypes.put("country_name", "ascii");
    ColumnTypes.put("beginning_ip_address", "inet");
    ColumnTypes.put("ending_ip_address", "inet");
    ColumnTypes.put("beginning_ip_number", "bigint");
    ColumnTypes.put("ending_ip_number", "bigint");
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

    SuccessResult then = new SuccessResult(rows, ColumnTypes);
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
    List<String> sourceErrors =
        exceptionLines.stream().filter(l -> l.startsWith(keyword)).collect(Collectors.toList());
    Assertions.assertThat(sourceErrors.size()).isEqualTo(size);
  }

  private static Path getLogDirectory() {
    String logPath = System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY);
    return Paths.get(logPath);
  }

  public static void validateBadOps(int size) throws Exception {
    Path logPath = getLogDirectory();
    Path badOps = logPath.resolve("operation.bad");
    List<String> lines = Files.lines(badOps, Charset.defaultCharset()).collect(Collectors.toList());
    Assertions.assertThat(lines.size()).isEqualTo(size);
  }

  public static void validateOutputFile(Path outputFilePath, int numOfRecords) throws Exception {

    List<String> lines =
        Files.lines(outputFilePath, Charset.defaultCharset()).collect(Collectors.toList());
    Assertions.assertThat(lines.size()).isEqualTo(numOfRecords);
  }

  public static String fetchSimulacronContactPointsForArg(SimulacronRule simulacron) {
    return "[\"" + simulacron.getContactPoints().iterator().next().toString().substring(1) + "\"]";
  }
}
