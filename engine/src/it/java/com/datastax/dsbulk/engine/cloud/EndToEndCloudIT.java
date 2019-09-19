/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.cloud;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createIpByCountryTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validatePositionsFile;
import static java.nio.file.Files.createTempDirectory;
import static org.slf4j.event.Level.INFO;

import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.dsbulk.commons.tests.cloud.SNIProxyServer;
import com.datastax.dsbulk.commons.tests.cloud.SNIProxyServerExtension;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.internal.settings.DriverSettings;
import com.datastax.dsbulk.engine.tests.utils.LogUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(SNIProxyServerExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("long")
class EndToEndCloudIT {

  private final SNIProxyServer proxy;
  private final CqlSession session;
  private final LogInterceptor logs;

  private Path logDir;
  private Path unloadDir;

  EndToEndCloudIT(
      SNIProxyServer proxy,
      CqlSession session,
      @LogCapture(level = INFO, value = DriverSettings.class) LogInterceptor logs) {
    this.proxy = proxy;
    this.session = session;
    this.logs = logs;
  }

  @BeforeAll
  void createTables() {
    createIpByCountryTable(session);
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @BeforeEach
  void clearLogs() {
    logs.clear();
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

  @Test
  void full_load_unload_default_CL() throws Exception {
    performLoad(new ArrayList<>());
    assertThat(logs)
        .hasMessageContaining(
            "Changing default consistency level to LOCAL_QUORUM for Cloud deployments");
    performUnload();
  }

  @Test
  void full_load_unload_forced_CL() throws Exception {
    performLoad(Lists.newArrayList("--driver.query.consistency", "LOCAL_QUORUM"));
    assertThat(logs.getLoggedEvents()).isEmpty();
    performUnload();
  }

  @Test
  void full_load_unload_forced_wrong_CL() throws Exception {
    performLoad(Lists.newArrayList("--driver.query.consistency", "LOCAL_ONE"));
    assertThat(logs)
        .hasMessageContaining(
            "Consistency level LOCAL_ONE was specified, but Cloud deployments only accept "
                + "LOCAL_QUORUM when writing; forcing LOCAL_QUORUM");
    performUnload();
  }

  private void performLoad(List<String> initialArgs) throws IOException, URISyntaxException {
    List<String> loadArgs =
        Lists.newArrayList(
            "load",
            "--connector.csv.url",
            quoteJson(CSV_RECORDS_UNIQUE),
            "--connector.csv.header",
            "false",
            "--schema.keyspace",
            session.getKeyspace().get().asInternal(),
            "--schema.table",
            "ip_by_country",
            "--schema.mapping",
            IP_BY_COUNTRY_MAPPING_INDEXED);
    loadArgs.addAll(initialArgs);
    int status = new DataStaxBulkLoader(addCommonSettings(loadArgs)).run();
    assertThat(status).isZero();
    ResultSet set = session.execute("SELECT * FROM ip_by_country");
    List<Row> results = set.all();
    assertThat(results.size()).isEqualTo(24);
    validatePositionsFile(CSV_RECORDS_UNIQUE, 24);
    deleteDirectory(logDir);
  }

  private void performUnload() throws IOException {
    List<String> unloadArgs =
        Lists.newArrayList(
            "unload",
            "--connector.csv.url",
            quoteJson(unloadDir),
            "--connector.csv.header",
            "false",
            "--connector.csv.maxConcurrentFiles",
            "1",
            "--schema.keyspace",
            session.getKeyspace().get().asInternal(),
            "--schema.table",
            "ip_by_country",
            "--schema.mapping",
            IP_BY_COUNTRY_MAPPING_INDEXED);
    int status = new DataStaxBulkLoader(addCommonSettings(unloadArgs)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  private String[] addCommonSettings(List<String> args) {
    Collections.addAll(
        args,
        "--log.directory",
        quoteJson(logDir),
        "--driver.pooling.local.connections",
        "1",
        "--driver.cloud.secureConnectBundle",
        proxy.getSecureBundlePath().toString());
    return args.toArray(new String[0]);
  }
}
