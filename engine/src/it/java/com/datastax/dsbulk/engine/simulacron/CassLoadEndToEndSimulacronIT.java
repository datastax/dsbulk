/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.INSERT_INTO_IP_BY_COUNTRY;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static java.nio.file.Files.createTempDirectory;
import static org.slf4j.event.Level.ERROR;

import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronExtension;
import com.datastax.dsbulk.commons.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.dsbulk.engine.tests.utils.EndToEndUtils;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
@SimulacronConfig(dse = false)
class CassLoadEndToEndSimulacronIT {

  private final BoundCluster simulacron;
  private final LogInterceptor interceptor;

  private Path logDir;

  CassLoadEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture(value = WorkflowUtils.class, level = ERROR) LogInterceptor interceptor) {
    this.simulacron = simulacron;
    this.interceptor = interceptor;
  }

  @AfterEach
  void resetLogbackConfiguration() throws JoranException {
    EndToEndUtils.resetLogbackConfiguration();
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
  }

  @Test
  void full_load() {
    String[] args = {
      "load",
      "--log.directory",
      escapeUserInput(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      escapeUserInput(CSV_RECORDS_UNIQUE),
      "--driver.hosts",
      simulacron.dc(0).node(0).inetSocketAddress().getHostName(),
      "--driver.port",
      Integer.toString(simulacron.dc(0).node(0).inetSocketAddress().getPort()),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING
    };

    int status = new Main(args).run();
    assertThat(status).isEqualTo(Main.STATUS_ABORTED_FATAL_ERROR);

    assertThat(interceptor)
        .hasMessageContaining(
            "Incompatible cluster detected. Load functionality is only compatible with a DSE cluster")
        .hasMessageContaining("The following nodes do not appear to be running DSE");
  }
}
