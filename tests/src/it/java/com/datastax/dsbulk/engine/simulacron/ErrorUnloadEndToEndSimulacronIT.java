/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.commons.internal.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.internal.logging.StreamType.STDERR;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.createQueryWithResultSet;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.fetchContactPoints;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;
import static org.slf4j.event.Level.ERROR;

import com.datastax.dsbulk.commons.internal.logging.LogCapture;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptor;
import com.datastax.dsbulk.commons.internal.logging.StreamCapture;
import com.datastax.dsbulk.commons.internal.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.StreamInterceptor;
import com.datastax.dsbulk.connectors.csv.CSVConnector;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ErrorUnloadEndToEndSimulacronIT {

  private final BoundCluster simulacron;
  private final LogInterceptor logs;
  private final StreamInterceptor stdErr;

  private Path unloadDir;

  ErrorUnloadEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture(value = CSVConnector.class, level = ERROR) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {
    this.simulacron = simulacron;
    this.logs = logs;
    this.stdErr = stdErr;
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    unloadDir = createTempDirectory("test");
  }

  @AfterEach
  void deleteDirs() throws IOException {
    deleteRecursively(unloadDir, ALLOW_INSECURE);
  }

  @Test
  void unload_existing_file() throws Exception {
    // Prior to DAT-151 this case would hang
    Files.createFile(unloadDir.resolve("output-000001.csv"));

    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(prime));

    String[] unloadArgs = {
      "unload",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      unloadDir.toString(),
      "--connector.csv.maxConcurrentFiles",
      "1",
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      SELECT_FROM_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(unloadArgs).run();
    assertThat(status).isZero();

    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("Could not create CSV writer for file:")
        .contains("output-000001.csv")
        .contains("Error writing to file:")
        .contains("output-000001.csv");
  }
}
