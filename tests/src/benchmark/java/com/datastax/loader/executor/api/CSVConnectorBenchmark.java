/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api;

import static com.datastax.loader.tests.utils.CsvUtils.createIpByCountryTable;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.loader.engine.ReadWorkflow;
import com.datastax.loader.engine.WriteWorkflow;
import com.datastax.loader.tests.utils.ZipUtils;
import io.reactivex.plugins.RxJavaPlugins;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

public class CSVConnectorBenchmark {

  private static final int TOTAL_RECORDS = 70865;
  private static final int WARMUP_ITERATIONS = 10;
  private static final int MEASUREMENT_ITERATIONS = 1;
  private static final int MEASUREMENT_TIME_IN_MINUTES = 5;

  @Benchmark
  @OperationsPerInvocation(TOTAL_RECORDS)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = WARMUP_ITERATIONS)
  @Measurement(
    time = MEASUREMENT_TIME_IN_MINUTES,
    timeUnit = MINUTES,
    iterations = MEASUREMENT_ITERATIONS
  )
  @Fork(1)
  public void benchmarkCsvConnectorWrite(CSVConnectorBenchmarkState state) throws Exception {
    new WriteWorkflow(state.args).execute();
  }

  @Benchmark
  @OperationsPerInvocation(TOTAL_RECORDS)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = WARMUP_ITERATIONS)
  @Measurement(
    time = MEASUREMENT_TIME_IN_MINUTES,
    timeUnit = MINUTES,
    iterations = MEASUREMENT_ITERATIONS
  )
  @Fork(1)
  public void benchmarkCsvConnectorRead(CSVConnectorBenchmarkState state) throws Exception {
    new ReadWorkflow(state.args).execute();
  }

  @State(Scope.Benchmark)
  public static class CSVConnectorBenchmarkState {

    private URL csvFile;
    private String[] args;

    @Setup(Level.Trial)
    public void init() throws Exception {
      RxJavaPlugins.setErrorHandler((t) -> {});
      Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
      Session session = cluster.connect();
      // fixtures for write benchmarks
      session.execute("DROP KEYSPACE IF EXISTS csv_connector_benchmark");
      session.execute(
          "CREATE KEYSPACE csv_connector_benchmark WITH replication = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 }");
      session.execute("USE csv_connector_benchmark");
      Path dest = Files.createTempDirectory("benchmark");
      ZipUtils.unzip("ip-by-country-all.csv.zip", dest);
      createIpByCountryTable(session);
      csvFile = dest.resolve("ip-by-country.csv").toUri().toURL();
      cluster.close();
      args =
          new String[] {
            "log.outputDirectory=\"file:./target\"",
            "connector.name=csv",
            "connector.url=\"" + csvFile.toExternalForm() + "\"",
            "schema.keyspace=csv_connector_benchmark",
            "schema.table=ip_by_country"
            //        "schema.mapping={0=year,1=make,2=model,3=description}
          };
      new WriteWorkflow(args).execute();
    }
  }
}
