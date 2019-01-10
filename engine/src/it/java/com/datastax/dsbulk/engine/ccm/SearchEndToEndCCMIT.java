/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.nio.file.Files.createTempDirectory;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_MINUTE;
import static org.slf4j.event.Level.WARN;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Workload;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMWorkload;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.tests.MockConnector;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

// tests for DAT-309
@ExtendWith(LogInterceptingExtension.class)
@Tag("medium")
@CCMConfig(workloads = @CCMWorkload(Workload.solr))
@CCMRequirements(compatibleTypes = DSE)
class SearchEndToEndCCMIT extends EndToEndCCMITBase {

  private final LogInterceptor logs;

  private Path logDir;
  private List<Record> records;

  SearchEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig Session session,
      @LogCapture(level = WARN) LogInterceptor logs) {
    super(ccm, session);
    this.logs = logs;
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
  }

  @BeforeEach
  void clearLogs() {
    logs.clear();
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
  }

  @BeforeEach
  void setUpConnector() {
    records = MockConnector.mockWrites();
  }

  @Test
  void full_unload_search() {

    session.execute(
        "CREATE TABLE IF NOT EXISTS test_search (pk int, cc int, v varchar, PRIMARY KEY (pk, cc))");
    session.execute(
        "CREATE SEARCH INDEX IF NOT EXISTS ON test_search WITH COLUMNS v { indexed:true };");

    session.execute("INSERT INTO test_search (pk, cc, v) VALUES (0, 0, 'foo')");
    session.execute("INSERT INTO test_search (pk, cc, v) VALUES (0, 1, 'bar')");
    session.execute("INSERT INTO test_search (pk, cc, v) VALUES (0, 2, 'qix')");

    String query = "SELECT v FROM test_search WHERE solr_query = '{\"q\": \"v:foo\"}'";

    // Wait until index is built
    await().atMost(ONE_MINUTE).until(() -> !session.execute(query).all().isEmpty());

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add(quoteJson(query));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertThat(logs)
        .hasMessageContaining(
            "Continuous paging is enabled but is not compatible with search queries; disabling");

    assertThat(records)
        .hasSize(1)
        .hasOnlyOneElementSatisfying(
            record -> {
              assertThat(record.fields()).hasSize(1);
              assertThat(record.getFieldValue(new DefaultMappedField("v"))).isEqualTo("foo");
            });
  }
}
