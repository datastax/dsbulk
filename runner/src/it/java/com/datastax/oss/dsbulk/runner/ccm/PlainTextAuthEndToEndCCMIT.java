/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.runner.ccm;

import static com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus.STATUS_OK;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDERR;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader.ExitStatus;
import com.datastax.oss.dsbulk.runner.tests.CsvUtils;
import com.datastax.oss.dsbulk.runner.tests.EndToEndUtils;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@CCMConfig(
    config = "authenticator:PasswordAuthenticator",
    jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0")
@Tag("medium")
class PlainTextAuthEndToEndCCMIT extends EndToEndCCMITBase {

  private final LogInterceptor logs;
  private final StreamInterceptor stderr;

  PlainTextAuthEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(credentials = {"cassandra", "cassandra"}) CqlSession session,
      @LogCapture LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    super(ccm, session);
    this.logs = logs;
    this.stderr = stderr;
  }

  @BeforeAll
  void createTables() {
    EndToEndUtils.createIpByCountryTable(session);
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
    args.add(StringUtils.quoteJson(CsvUtils.CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED);
    if (!inferAuthProvider) {
      args.add("--datastax-java-driver.advanced.auth-provider.class");
      args.add("PlainTextAuthProvider");
    }

    args.add("--datastax-java-driver.advanced.auth-provider.username");
    args.add("cassandra");
    args.add("--datastax-java-driver.advanced.auth-provider.password");
    args.add("cassandra");

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
      assertThat(stderr.getStreamAsString())
          .contains(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
    FileUtils.deleteDirectory(logDir);
    logs.clear();
    stderr.clear();

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(StringUtils.quoteJson(unloadDir));
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
    EndToEndUtils.assertStatus(status, STATUS_OK);
    EndToEndUtils.validateOutputFiles(24, unloadDir);
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
      assertThat(stderr.getStreamAsString())
          .contains(
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
    args.add(StringUtils.quoteJson(CsvUtils.CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getKeyspace().get().asInternal());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED);
    if (!inferAuthProvider) {
      args.add("--driver.auth.provider");
      args.add("PlainTextAuthProvider");
    }
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, STATUS_OK);
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    assertThat(logs)
        .hasMessageContaining(
            "Setting dsbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--datastax-java-driver.advanced.auth-provider.* instead");
    assertThat(stderr.getStreamAsString())
        .contains(
            "Setting dsbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--datastax-java-driver.advanced.auth-provider.* instead");
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
      assertThat(stderr.getStreamAsString())
          .contains(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
    FileUtils.deleteDirectory(logDir);
    logs.clear();

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(StringUtils.quoteJson(unloadDir));
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
    EndToEndUtils.assertStatus(status, STATUS_OK);
    EndToEndUtils.validateOutputFiles(24, unloadDir);
    assertThat(logs)
        .hasMessageContaining(
            "Setting dsbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--datastax-java-driver.advanced.auth-provider.* instead");
    assertThat(stderr.getStreamAsString())
        .contains(
            "Setting dsbulk.driver.auth.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--datastax-java-driver.advanced.auth-provider.* instead");
    if (inferAuthProvider) {
      assertThat(logs)
          .hasMessageContaining(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
      assertThat(stderr.getStreamAsString())
          .contains(
              "Username and password provided but auth provider not specified, "
                  + "inferring PlainTextAuthProvider");
    }
  }
}
