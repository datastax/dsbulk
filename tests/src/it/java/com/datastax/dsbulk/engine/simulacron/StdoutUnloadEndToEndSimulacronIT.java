/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.commons.internal.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.internal.logging.StreamType.STDOUT;
import static com.datastax.dsbulk.engine.internal.settings.LogSettings.PRODUCTION_KEY;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.createQueryWithResultSet;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.fetchContactPoints;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateQueryCount;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.LoggerContext;
import com.datastax.dsbulk.commons.internal.logging.LogCapture;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptor;
import com.datastax.dsbulk.commons.internal.logging.StreamCapture;
import com.datastax.dsbulk.commons.internal.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.StreamInterceptor;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.nio.file.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StdoutUnloadEndToEndSimulacronIT {

  private final BoundCluster simulacron;
  private final StreamInterceptor stdOut;
  private final StreamInterceptor stdErr;
  private final LogInterceptor logs;

  StdoutUnloadEndToEndSimulacronIT(
      BoundCluster simulacron,
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr,
      @LogCapture(LogSettings.class) LogInterceptor logs) {
    this.simulacron = simulacron;
    this.stdOut = stdOut;
    this.logs = logs;
    this.stdErr = stdErr;
  }

  @BeforeEach
  void setProductionKey() {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    lc.putProperty(PRODUCTION_KEY, "true");
  }

  @AfterEach
  void removeProductionKey() {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    lc.putProperty(PRODUCTION_KEY, "false");
  }

  @BeforeEach
  void primeQuery() {
    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(prime));
  }

  @Test
  void validate_stdout() throws Exception {

    String[] args = {
      "unload",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      "stdout:/",
      "--connector.csv.maxConcurrentFiles",
      "1 ",
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

    int status = new Main(args).run();
    assertThat(status).isZero();

    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ConsistencyLevel.ONE);
    assertThat(stdOut.getStreamLines().size()).isEqualTo(24);
    assertThat(stdErr.getStreamAsString())
        .contains("Standard output is reserved, log messages are redirected to standard error.");
    assertThat(logs.getAllMessagesAsString())
        .contains("Standard output is reserved, log messages are redirected to standard error.");
  }
}
