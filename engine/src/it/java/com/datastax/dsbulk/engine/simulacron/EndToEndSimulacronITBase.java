/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static java.nio.file.Files.createTempDirectory;

import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronExtension;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils;
import com.datastax.dsbulk.engine.tests.utils.LogUtils;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndToEndSimulacronITBase {

  final BoundCluster simulacron;
  final LogInterceptor logs;
  final StreamInterceptor stdOut;
  final StreamInterceptor stdErr;
  final String hostname;
  final String port;

  Path unloadDir;
  Path logDir;

  EndToEndSimulacronITBase(
      BoundCluster simulacron,
      LogInterceptor logs,
      StreamInterceptor stdOut,
      StreamInterceptor stdErr) {
    this.simulacron = simulacron;
    this.logs = logs;
    this.stdOut = stdOut;
    this.stdErr = stdErr;
    InetSocketAddress node = simulacron.dc(0).node(0).inetSocketAddress();
    hostname = node.getAddress().getHostAddress();
    port = Integer.toString(node.getPort());
  }

  @BeforeEach
  void resetPrimes() {
    simulacron.clearPrimes(true);
    SimulacronUtils.primeSystemLocal(simulacron, Collections.emptyMap());
    SimulacronUtils.primeSystemPeers(simulacron);
    SimulacronUtils.primeSystemPeersV2(simulacron);
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
    deleteDirectory(unloadDir);
  }

  @AfterEach
  void resetLogbackConfiguration() throws JoranException {
    LogUtils.resetLogbackConfiguration();
  }

  @AfterEach
  void clearLogs() {
    simulacron.clearPrimes(true);
    simulacron.clearLogs();
  }

  String[] addCommonSettings(String[] args) {
    String[] commonArgs =
        new String[] {
          "--log.directory",
          quoteJson(logDir),
          "-cp",
          quoteJson(hostname + ':' + port),
          "-dc",
          "dc1",
          "-cl",
          "LOCAL_ONE",
          "--driver.advanced.connection.pool.local.size",
          "1",
        };
    return Stream.of(args, commonArgs).flatMap(Stream::of).toArray(String[]::new);
  }
}
