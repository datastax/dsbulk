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
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.assertStatus;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_MINUTE;
import static org.slf4j.event.Level.WARN;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Workload;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMWorkload;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.tests.MockConnector;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.data.Index;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
@Tag("medium")
@CCMConfig(workloads = @CCMWorkload(Workload.solr))
@CCMRequirements(
    compatibleTypes = DSE,
    versionRequirements = @CCMVersionRequirement(type = DSE, min = "5.1"))
class SearchEndToEndCCMIT extends EndToEndCCMITBase {

  private final LogInterceptor logs;

  private List<Record> records;

  SearchEndToEndCCMIT(
      CCMCluster ccm, CqlSession session, @LogCapture(level = WARN) LogInterceptor logs) {
    super(ccm, session);
    this.logs = logs;
  }

  @BeforeEach
  void clearLogs() {
    logs.clear();
  }

  @BeforeEach
  void setUpConnector() {
    records = MockConnector.mockWrites();
  }

  /** Test for DAT-309: unload of a Solr query */
  @Test
  void full_unload_search_solr_query() {

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
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.query");
    args.add(quoteJson(query));

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, 0);

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

  /**
   * Test for DAT-365: regular unload of a search-enabled table should not contain the solr_query
   * column.
   */
  @Test
  void normal_unload_of_search_enabled_table() {

    session.execute(
        "CREATE TABLE IF NOT EXISTS test_search2 (pk int, cc int, v varchar, PRIMARY KEY (pk, cc))");
    session.execute(
        "CREATE SEARCH INDEX IF NOT EXISTS ON test_search2 WITH COLUMNS v { indexed:true };");

    session.execute("INSERT INTO test_search2 (pk, cc, v) VALUES (0, 0, 'foo')");
    session.execute("INSERT INTO test_search2 (pk, cc, v) VALUES (0, 1, 'bar')");
    session.execute("INSERT INTO test_search2 (pk, cc, v) VALUES (0, 2, 'qix')");

    // Wait until index is built
    await()
        .atMost(ONE_MINUTE)
        .until(
            () ->
                !session
                    .execute("SELECT v FROM test_search2 WHERE solr_query = '{\"q\": \"v:foo\"}'")
                    .all()
                    .isEmpty());

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("mock");
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.table");
    args.add("test_search2");

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, 0);

    assertThat(records)
        .hasSize(3)
        .satisfies(
            record -> {
              assertThat(record.fields()).hasSize(3);
              assertThat(record.getFieldValue(new DefaultMappedField("pk"))).isEqualTo("0");
              assertThat(record.getFieldValue(new DefaultMappedField("cc"))).isEqualTo("0");
              assertThat(record.getFieldValue(new DefaultMappedField("v"))).isEqualTo("foo");
            },
            Index.atIndex(0))
        .satisfies(
            record -> {
              assertThat(record.fields()).hasSize(3);
              assertThat(record.getFieldValue(new DefaultMappedField("pk"))).isEqualTo("0");
              assertThat(record.getFieldValue(new DefaultMappedField("cc"))).isEqualTo("1");
              assertThat(record.getFieldValue(new DefaultMappedField("v"))).isEqualTo("bar");
            },
            Index.atIndex(1))
        .satisfies(
            record -> {
              assertThat(record.fields()).hasSize(3);
              assertThat(record.getFieldValue(new DefaultMappedField("pk"))).isEqualTo("0");
              assertThat(record.getFieldValue(new DefaultMappedField("cc"))).isEqualTo("2");
              assertThat(record.getFieldValue(new DefaultMappedField("v"))).isEqualTo("qix");
            },
            Index.atIndex(2));
  }
}
