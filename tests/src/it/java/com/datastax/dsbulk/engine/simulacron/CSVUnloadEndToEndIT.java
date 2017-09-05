/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.SimulacronRule;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import com.datastax.dsbulk.tests.utils.EndToEndUtils;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.result.SyntaxErrorResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class CSVUnloadEndToEndIT {
  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Test
  public void full_unload() throws Exception {

    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    deleteIfExists(full_load_dir);
    RequestPrime prime = EndToEndUtils.createQueryWithResultSet("SELECT * FROM ip_by_country", 24);
    simulacron.cluster().prime(new Prime(prime));
    String[] unloadArgs = {
      "unload",
      "--log.outputDirectory=./target",
      "--connector.name=csv",
      "--connector.csv.url=" + full_load_dir.toString(),
      "--connector.csv.maxThreads=1 ",
      "--driver.query.consistency=ONE",
      "--driver.hosts=" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.protocol.compression=NONE",
      "--schema.statement=" + CsvUtils.SELECT_FROM_IP_BY_COUNTRY + "",
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(unloadArgs);

    validateQueryCount(1, ConsistencyLevel.ONE);
    EndToEndUtils.validateOutputFile(full_load_output_file, 24);
  }

  @Test
  public void full_unload_csv_default_modification() throws Exception {

    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    deleteIfExists(full_load_dir);
    RequestPrime prime =
        EndToEndUtils.createQueryWithResultSetWithQuotes("SELECT * FROM ip_by_country", 24);
    simulacron.cluster().prime(new Prime(prime));
    // This exercises logic which will replace the delimiter and make sure non-standard quoting is working.
    String[] unloadArgs = {
      "unload",
      "--log.outputDirectory=./target",
      "--connector.name=csv",
      "--connector.csv.url=" + full_load_dir.toString(),
      "--connector.csv.maxThreads=1 ",
      "--connector.csv.delimiter=;",
      "--connector.csv.quote=<",
      "--connector.csv.skipLines=2",
      "--driver.query.consistency=ONE",
      "--driver.hosts=" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.protocol.compression=NONE",
      "--schema.statement=" + CsvUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(unloadArgs);

    verifyDelimiterCount(";", full_load_output_file, 120);
    verifyDelimiterCount("<", full_load_output_file, 48);
    validateQueryCount(1, ConsistencyLevel.ONE);
    EndToEndUtils.validateOutputFile(full_load_output_file, 24);
  }

  @Test
  public void full_unload_multi_thread() throws Exception {
    Path full_load_dir = Paths.get("./full_load_dir");
    List<Path> outputFiles =
        Arrays.asList(
            Paths.get("./full_load_dir/output-000001.csv"),
            Paths.get("./full_load_dir/output-000002.csv"),
            Paths.get("./full_load_dir/output-000003.csv"),
            Paths.get("./full_load_dir/output-000004.csv"));
    deleteIfExists(full_load_dir);
    RequestPrime prime = EndToEndUtils.createQueryWithResultSet("SELECT * FROM ip_by_country", 24);
    simulacron.cluster().prime(new Prime(prime));
    String[] unloadArgs = {
      "unload",
      "--log.outputDirectory=./target",
      "--connector.name=csv",
      "--connector.csv.url=" + full_load_dir.toString(),
      "--connector.csv.maxThreads=4 ",
      "--driver.query.consistency=LOCAL_ONE",
      "--driver.hosts=" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.protocol.compression=NONE",
      "--schema.statement=" + CsvUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(unloadArgs);

    validateQueryCount(1, ConsistencyLevel.LOCAL_ONE);
    EndToEndUtils.validateOutputFilesTotal(outputFiles, 24);
  }

  @Test
  public void unload_failure_during_read_single_thread() throws Exception {

    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    deleteIfExists(full_load_dir);
    RequestPrime prime =
        EndToEndUtils.createQueryWithError(
            "SELECT * FROM ip_by_country", new SyntaxErrorResult("Invalid table", 0L, true));
    simulacron.cluster().prime(new Prime(prime));
    String[] unloadArgs = {
      "unload",
      "--log.outputDirectory=./target",
      "--connector.name=csv",
      "--connector.csv.url=" + full_load_dir.toString(),
      "--connector.csv.maxThreads=1 ",
      "--driver.query.consistency=ONE",
      "--driver.hosts=" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.protocol.compression=NONE",
      "--schema.statement=" + CsvUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(unloadArgs);

    validateQueryCount(1, ConsistencyLevel.ONE);
    EndToEndUtils.validateExceptionsLog(1, "Statement:", "unload-errors.log");
    EndToEndUtils.validateOutputFile(full_load_output_file, 0);
  }

  @Test
  public void unload_failure_during_read_multi_thread() throws Exception {

    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    deleteIfExists(full_load_dir);
    RequestPrime prime =
        EndToEndUtils.createQueryWithError(
            "SELECT * FROM ip_by_country", new SyntaxErrorResult("Invalid table", 0L, true));
    simulacron.cluster().prime(new Prime(prime));
    String[] unloadArgs = {
      "unload",
      "--log.outputDirectory=./target",
      "--connector.name=csv",
      "--connector.csv.url=" + full_load_dir.toString(),
      "--connector.csv.maxThreads=4 ",
      "--driver.query.consistency=ONE",
      "--driver.hosts=" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.protocol.compression=NONE",
      "--schema.statement=" + CsvUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(unloadArgs);

    validateQueryCount(1, ConsistencyLevel.ONE);
    EndToEndUtils.validateExceptionsLog(1, "Statement:", "unload-errors.log");
    EndToEndUtils.validateOutputFile(full_load_output_file, 0);
  }

  @Test
  @Ignore
  public void unload_failure_during_prepare() throws Exception {

    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    deleteIfExists(full_load_dir);
    RequestPrime prime =
        EndToEndUtils.createQueryWithError(
            "SELECT * FROM ip_by_country", new SyntaxErrorResult("Invalid table", 0L, false));
    simulacron.cluster().prime(new Prime(prime));
    String[] unloadArgs = {
      "unload",
      "--log.outputDirectory=./target",
      "--connector.name=csv",
      "--connector.csv.url=" + full_load_dir.toString(),
      "--connector.csv.maxThreads=1 ",
      "--driver.query.consistency=ONE",
      "--driver.hosts=" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.protocol.compression=NONE",
      "--schema.statement=" + CsvUtils.SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(unloadArgs);

    validateQueryCount(1, ConsistencyLevel.ONE);
    EndToEndUtils.validateExceptionsLog(1, "Statement:", "unload-errors.log");
    EndToEndUtils.validateOutputFile(full_load_output_file, 0);
  }

  @SuppressWarnings("SameParameterValue")
  private void validateQueryCount(int numOfQueries, ConsistencyLevel level) {
    EndToEndUtils.validateQueryCount(
        simulacron, numOfQueries, "SELECT * FROM ip_by_country", level);
  }

  private void verifyDelimiterCount(String delimiter, Path output_path, int expected)
      throws Exception {

    Scanner content = new Scanner(output_path.toFile()).useDelimiter(delimiter);
    int i = 0;
    while (content.hasNext()) {
      content.next();
      i++;
    }
    Assertions.assertThat(i - 1).isEqualTo(expected);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void deleteIfExists(Path filepath) {

    if (filepath != null) {
      File dir = filepath.toFile();
      if (dir.isDirectory()) {
        String[] children = dir.list();
        assert children != null;
        for (String child : children) {
          new File(dir, child).delete();
        }
      }
      dir.delete();
    }
  }
}
