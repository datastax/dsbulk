/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.commons.internal.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.tests.utils.CsvUtils.INSERT_INTO_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.fetchContactPoints;
import static org.slf4j.event.Level.ERROR;

import com.datastax.dsbulk.commons.internal.logging.LogCapture;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptor;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.dsbulk.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.nio.file.Files;
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

  CassLoadEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture(value = WorkflowUtils.class, level = ERROR) LogInterceptor interceptor) {
    this.simulacron = simulacron;
    this.interceptor = interceptor;
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

    assertThat(interceptor)
        .hasMessageContaining(
            "Incompatible cluster detected. Load functionality is only compatible with a DSE cluster")
        .hasMessageContaining("The following nodes do not appear to be running DSE");
  }
}
