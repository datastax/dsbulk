/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.tests.utils;

import static com.datastax.dsbulk.engine.internal.settings.LogSettings.PRODUCTION_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
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
    Query when = new Query(query);
    Map<String, String> columnTypes = new LinkedHashMap<>();
    columnTypes.put("country_code", "ascii");
    columnTypes.put("country_name", "ascii");
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
    columnTypes.put("country_code", "ascii");
    columnTypes.put("country_name", "ascii");
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

  public static void validateExceptionsLog(int size, String keyword, String fileName, Path logPath)
      throws Exception {
    Path exceptionFile = logPath.resolve(fileName);
    @SuppressWarnings("StreamResourceLeak")
    long numErrors = Files.lines(exceptionFile).filter(l -> l.contains(keyword)).count();
    assertThat(numErrors).isEqualTo(size);
  }

  public static void validateBadOps(int size, Path logPath) throws Exception {
    Path badOps = logPath.resolve("operation.bad");
    @SuppressWarnings("StreamResourceLeak")
    long numBadOps = Files.lines(badOps, Charset.defaultCharset()).count();
    assertThat(numBadOps).isEqualTo(size);
  }

  public static void validateOutputFiles(int numOfRecords, Path dir) throws IOException {
    // Sum the number of lines in each file and assert that the total matches the expected value.
    long totalLines =
        FileUtils.readAllLinesInDirectoryAsStream(dir, StandardCharsets.UTF_8).count();
    assertThat(totalLines).isEqualTo(numOfRecords);
  }

  public static String fetchContactPoints(BoundCluster simulacron) {
    return simulacron.dc(0).node(0).inet().getHostAddress();
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

  public static void resetLogbackConfiguration() throws JoranException {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    context.reset();
    configurator.doConfigure(ClassLoader.getSystemResource("logback-test.xml"));
  }

  public static void setProductionKey() {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    lc.putProperty(PRODUCTION_KEY, "true");
  }
}
