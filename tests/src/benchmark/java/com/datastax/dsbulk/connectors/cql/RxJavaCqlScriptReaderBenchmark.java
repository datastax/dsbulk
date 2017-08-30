/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.cql;

import com.datastax.dsbulk.tests.utils.ZipUtils;
import io.reactivex.plugins.RxJavaPlugins;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class RxJavaCqlScriptReaderBenchmark {

  private static final int TOTAL_RECORDS = 70865;

  @Benchmark
  @OperationsPerInvocation(TOTAL_RECORDS)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Fork(1)
  public void benchmarkSingleLine(RxJavaBulkExecutionState state, Blackhole bh) throws Exception {
    RxJavaCqlScriptReader reader =
        new RxJavaCqlScriptReader(
            new BufferedReader(new InputStreamReader(state.cqlFile.openStream())));
    reader.readReactive().doOnNext(bh::consume).blockingSubscribe();
  }

  @Benchmark
  @OperationsPerInvocation(TOTAL_RECORDS)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Fork(1)
  public void benchmarkMultiLine(RxJavaBulkExecutionState state, Blackhole bh) throws Exception {
    RxJavaCqlScriptReader reader =
        new RxJavaCqlScriptReader(
            new BufferedReader(new InputStreamReader(state.cqlFile.openStream())), true);
    reader.readReactive().doOnNext(bh::consume).blockingSubscribe();
  }

  @State(Scope.Benchmark)
  public static class RxJavaBulkExecutionState {

    private URL cqlFile;

    @Setup(Level.Trial)
    public void init() throws IOException {
      RxJavaPlugins.setErrorHandler((t) -> {});
      Path dest = Files.createTempDirectory("benchmark");
      ZipUtils.unzip("ip-by-country-all.cql.zip", dest);
      cqlFile = dest.resolve("ip-by-country.cql").toUri().toURL();
    }
  }
}
