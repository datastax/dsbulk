/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.tests.utils.CsvUtils.INSERT_INTO_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.fetchContactPoints;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.getErrorEventMessages;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.dsbulk.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.dsbulk.tests.utils.TestAppender;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@ExtendWith(SimulacronExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
@SimulacronConfig(dse = false)
class CassLoadEndToEndSimulacronIT {

  private final BoundCluster simulacron;

  private Logger root;
  private TestAppender appender;
  private Level oldLevel;
  private Appender<ILoggingEvent> stdout;

  CassLoadEndToEndSimulacronIT(BoundCluster simulacron) {
    this.simulacron = simulacron;
  }

  @SuppressWarnings("Duplicates")
  @BeforeEach
  void setUp() throws Exception {
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    appender = new TestAppender();
    root.addAppender(appender);
    oldLevel = root.getLevel();
    root.setLevel(Level.INFO);
    stdout = root.getAppender("STDOUT");
    root.detachAppender(stdout);
  }

  @AfterEach
  void tearDown() throws Exception {
    root.detachAppender(appender);
    root.setLevel(oldLevel);
    root.addAppender(stdout);
  }

  @Test
  void full_load() throws Exception {
    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_UNIQUE.toExternalForm(),
      "--driver.hosts",
      fetchContactPoints(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(args).run();
    assertThat(status).isZero();

    List<String> errorMessages = getErrorEventMessages(appender);
    assertThat(errorMessages).isNotEmpty();
    assertThat(errorMessages.get(0))
        .contains(
            "Incompatible cluster detected. Load functionality is only compatible with a DSE cluster");
    assertThat(errorMessages.get(1))
        .contains("The following nodes do not appear to be running DSE");
  }
}
