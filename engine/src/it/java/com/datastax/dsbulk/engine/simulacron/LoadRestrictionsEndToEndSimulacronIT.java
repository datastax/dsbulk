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
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.INSERT_INTO_IP_BY_COUNTRY;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createSimpleParameterizedQuery;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.primeIpByCountryTable;
import static org.slf4j.event.Level.ERROR;

import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronExtension;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
class LoadRestrictionsEndToEndSimulacronIT extends EndToEndSimulacronITBase {

  LoadRestrictionsEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture(value = WorkflowUtils.class, level = ERROR) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {
    super(simulacron, logs, stdOut, stdErr);
  }

  @Override
  @BeforeEach
  void resetPrimes() {
    simulacron.clearPrimes(true);
    SimulacronUtils.primeSystemPeers(simulacron);
    SimulacronUtils.primeSystemPeersV2(simulacron);
    primeIpByCountryTable(simulacron);
    RequestPrime insert = createSimpleParameterizedQuery(INSERT_INTO_IP_BY_COUNTRY);
    simulacron.prime(new Prime(insert));
  }

  @Test
  void should_deny_load_to_oss_cassandra() {
    // DAT-322: absence of dse_version + absence of a DSE patch in release_version => OSS C*
    SimulacronUtils.primeSystemLocal(
        simulacron, NullAllowingImmutableMap.of("release_version", "4.0.0", "dse_version", null));
    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_UNIQUE),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);

    assertThat(logs)
        .hasMessageContaining(
            "Incompatible cluster detected. Load functionality is only compatible with a DSE cluster")
        .hasMessageContaining("The following nodes do not appear to be running DSE");

    assertThat(stdErr.getStreamAsString())
        .contains(
            "Incompatible cluster detected. Load functionality is only compatible with a DSE cluster")
        .contains("The following nodes do not appear to be running DSE")
        .contains(hostname)
        .contains(port);
  }

  @Test
  void should_allow_load_to_ddac() {
    // DAT-322: absence of dse_version + presence of a DSE patch in release_version => DDAC
    // (DataStax Distribution of Apache Cassandra).
    SimulacronUtils.primeSystemLocal(
        simulacron,
        NullAllowingImmutableMap.of("release_version", "4.0.0.2284", "dse_version", null));
    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_UNIQUE),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };
    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isZero();
  }

  @Test
  void should_allow_load_to_dse() {
    // DAT-322: presence of dse_version + presence of a DSE patch in release_version => DSE
    SimulacronUtils.primeSystemLocal(
        simulacron, ImmutableMap.of("release_version", "4.0.0.2284", "dse_version", "5.0.11"));
    String[] args = {
      "load",
      "--log.directory",
      quoteJson(logDir),
      "-header",
      "false",
      "--connector.csv.url",
      quoteJson(CSV_RECORDS_UNIQUE),
      "--schema.keyspace",
      "ks1",
      "--schema.query",
      INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping",
      IP_BY_COUNTRY_MAPPING_INDEXED
    };
    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isZero();
  }
}
