/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static com.datastax.dsbulk.commons.internal.logging.StreamType.STDERR;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_PARTIAL_BAD_LONG;
import static com.datastax.dsbulk.tests.utils.CsvUtils.INSERT_INTO_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.createSimpleParametrizedQuery;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.fetchContactPoints;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.event.Level.ERROR;

import com.datastax.dsbulk.commons.internal.logging.LogCapture;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptor;
import com.datastax.dsbulk.commons.internal.logging.StreamCapture;
import com.datastax.dsbulk.commons.internal.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.StreamInterceptor;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.nio.file.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ErrorLoadEndToEndSimulacronIT {

  private final BoundCluster simulacron;
  private final StreamInterceptor stdErr;

  ErrorLoadEndToEndSimulacronIT(
      BoundCluster simulacron,
      @StreamCapture(STDERR) StreamInterceptor stdErr,
      @LogCapture(value = Main.class, level = ERROR) LogInterceptor logs) {
    this.simulacron = simulacron;
    this.stdErr = stdErr;
  }

  @BeforeEach
  void primeQueries() {
    RequestPrime prime = createSimpleParametrizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(prime));
  }

  @Test
  void full_load() throws Exception {
    String[] args = {
      "load",
      "--log.directory",
      Files.createTempDirectory("test").toString(),
      "--log.maxErrors",
      "1%",
      "-header",
      "false",
      "--connector.csv.url",
      CSV_RECORDS_PARTIAL_BAD_LONG.toExternalForm(),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      fetchContactPoints(simulacron),
      "--driver.pooling.local.connections",
      "1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING,
      "--batch.enabled",
      "false"
    };
    int status = new Main(args).run();
    assertThat(status).isZero();
    assertThat(stdErr.getStreamAsString())
        .contains("failed: Too many errors, the maximum percentage allowed is 1.0%");
  }
}
