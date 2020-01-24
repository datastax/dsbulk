/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createIpByCountryTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.oss.driver.api.core.CqlSession;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@CCMConfig(
    config = "authenticator:PasswordAuthenticator",
    jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0")
@Tag("medium")
@ExtendWith(LogInterceptingExtension.class)
class PlainTextAuthEndToEndCCMIT extends EndToEndCCMITBase {

  private final LogInterceptor logs;

  PlainTextAuthEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(credentials = {"cassandra", "cassandra"}) CqlSession session,
      @LogCapture LogInterceptor logs) {
    super(ccm, session);
    this.logs = logs;
  }

  @BeforeAll
  void createTables() {
    createIpByCountryTable(session);
  }

  @BeforeEach
  void clearLogs() {
    logs.clear();
  }

  @AfterEach
  void truncateTable() {
    session.execute("TRUNCATE ip_by_country");
  }

  @ParameterizedTest(name = "[{index}] inferAuthProvider = {0}")
  @ValueSource(strings = {"true", "false"})
  void full_load_unload(boolean inferAuthProvider) throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    if (!inferAuthProvider) {
      args.add("--datastax-java-driver.advanced.auth-provider.class");
      args.add("PlainTextAuthProvider");
    }

    args.add("--datastax-java-driver.advanced.auth-provider.username");
    args.add("cassandra");
    args.add("--datastax-java-driver.advanced.auth-provider.password");
    args.add("cassandra");

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
    deleteDirectory(logDir);
    logs.clear();

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    if (!inferAuthProvider) {
      args.add("--datastax-java-driver.advanced.auth-provider.class");
      args.add("PlainTextAuthProvider");
    }
    args.add("--datastax-java-driver.advanced.auth-provider.username");
    args.add("cassandra");
    args.add("--datastax-java-driver.advanced.auth-provider.password");
    args.add("cassandra");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
  }

  @ParameterizedTest(name = "[{index}] inferAuthProvider = {0}")
  @ValueSource(strings = {"true", "false"})
  void full_load_unload_legacy_settings(boolean inferAuthProvider) throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    if (!inferAuthProvider) {
      args.add("--driver.auth.provider");
      args.add("PlainTextAuthProvider");
    }
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    assertThat(logs)
        .hasMessageContaining(
            "Setting dsbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--datastax-java-driver.advanced.auth-provider.* instead");
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
    deleteDirectory(logDir);
    logs.clear();

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    if (!inferAuthProvider) {
      args.add("--driver.auth.provider");
      args.add("PlainTextAuthProvider");
    }
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
    assertThat(logs)
        .hasMessageContaining(
            "Setting dsbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--datastax-java-driver.advanced.auth-provider.* instead");
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
  }
}
