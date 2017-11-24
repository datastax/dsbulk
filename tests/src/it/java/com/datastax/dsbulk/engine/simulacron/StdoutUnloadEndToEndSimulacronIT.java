/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.createQueryWithResultSet;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.fetchContactPoints;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.setURLFactoryIfNeeded;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateQueryCount;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateStringOutput;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@ExtendWith(SimulacronExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StdoutUnloadEndToEndSimulacronIT {

  private final BoundCluster simulacron;

  private PrintStream originalStdout;
  private ByteArrayOutputStream baos;
  private Logger root;
  private Appender<ILoggingEvent> stdoutAppender;

  StdoutUnloadEndToEndSimulacronIT(BoundCluster simulacron) {
    this.simulacron = simulacron;
  }

  @BeforeEach
  void hijackStandardOut() {
    setURLFactoryIfNeeded();
    originalStdout = System.out;
    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    stdoutAppender = root.getAppender("STDOUT");
    root.detachAppender(stdoutAppender);
  }

  @AfterEach
  void releaseStandardOut() {
    System.setOut(originalStdout);
    root.addAppender(stdoutAppender);
  }

  @Test
  void validate_stdoutput() throws Exception {

    RequestPrime prime = createQueryWithResultSet(SELECT_FROM_IP_BY_COUNTRY, 24);
    simulacron.prime(new Prime(prime));

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

    String out = baos.toString();
    validateQueryCount(simulacron, 1, SELECT_FROM_IP_BY_COUNTRY, ConsistencyLevel.ONE);
    validateStringOutput(out, 24);
  }
}
