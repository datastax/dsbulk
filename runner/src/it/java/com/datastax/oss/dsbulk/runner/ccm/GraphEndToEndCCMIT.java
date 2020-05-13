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

import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_OK;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.assertStatus;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.validateOutputFiles;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDERR;
import static com.datastax.oss.dsbulk.tests.utils.StringUtils.quoteJson;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.ExitStatus;
import com.datastax.oss.dsbulk.tests.assertions.TestAssertions;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Workload;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMRequirements;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMWorkload;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import com.datastax.oss.dsbulk.tests.utils.CQLUtils;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import java.net.URL;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

// tests for DAT-478
@CCMConfig(numberOfNodes = 1, workloads = @CCMWorkload(Workload.graph))
@CCMRequirements(
    compatibleTypes = Type.DSE,
    versionRequirements = @CCMVersionRequirement(type = Type.DSE, min = "6.8.0"))
@Tag("medium")
class GraphEndToEndCCMIT extends EndToEndCCMITBase {

  private static final String REPLICATION_CONFIG_MAP =
      "{'class': 'SimpleStrategy', 'replication_factor' :1}";

  private static final String CREATE_GRAPH_GREMLIN_QUERY =
      "system.graph(name).ifNotExists().withReplication(replicationConfig).coreEngine().create()";

  private static final String FRAUD_GRAPH = "Fraud";

  private static final String CUSTOMER_VERTEX_LABEL = "Customer";

  private static final String ORDER_VERTEX_LABEL = "Order";

  private static final String PLACES_EDGE_LABEL = "Places";

  private static final String CUSTOMER_TABLE = "Customers";

  private static final String CUSTOMER_PLACES_ORDER_TABLE =
      CUSTOMER_VERTEX_LABEL + "__" + PLACES_EDGE_LABEL + "__" + ORDER_VERTEX_LABEL;

  private static final String CUSTOMER_ORDER_MAPPINGS =
      "Customerid = Customer_Customerid, Orderid = Order_Orderid";

  private static final String SELECT_ALL_CUSTOMERS =
      "SELECT * FROM \"" + FRAUD_GRAPH + "\".\"" + CUSTOMER_TABLE + "\"";

  private static final String SELECT_ALL_CUSTOMER_ORDERS =
      "SELECT * FROM \"" + FRAUD_GRAPH + "\".\"" + CUSTOMER_PLACES_ORDER_TABLE + "\"";

  private static final URL CSV_CUSTOMER_RECORDS =
      ClassLoader.getSystemResource("graph/customers.csv");

  private static final URL CSV_CUSTOMER_ORDER_RECORDS =
      ClassLoader.getSystemResource("graph/customer-orders.csv");

  private static final URL JSON_CUSTOMER_RECORDS =
      ClassLoader.getSystemResource("graph/customers.json");

  private static final URL JSON_CUSTOMER_ORDER_RECORDS =
      ClassLoader.getSystemResource("graph/customer-orders.json");

  private final LogInterceptor logs;
  private final StreamInterceptor stderr;

  GraphEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(settings = "basic.graph.name = " + FRAUD_GRAPH) CqlSession session,
      @LogCapture(loggerName = "com.datastax.oss.dsbulk") LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    super(ccm, session);
    this.logs = logs;
    this.stderr = stderr;
  }

  @BeforeAll
  void createTables() {
    createFraudGraph();
    createCustomerVertex();
    createOrderVertex();
    createCustomerPlacesOrderEdge();
  }

  @BeforeEach
  void truncateTables() {
    session.execute(
        CQLUtils.truncateTable(FRAUD_GRAPH, CUSTOMER_TABLE)
            .setExecutionProfile(SessionUtils.slowProfile(session)));
    session.execute(
        CQLUtils.truncateTable(FRAUD_GRAPH, CUSTOMER_PLACES_ORDER_TABLE)
            .setExecutionProfile(SessionUtils.slowProfile(session)));
  }

  @Test
  void csv_full_load_unload_and_load_again_vertices() throws Exception {

    List<String> args =
        Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-v",
            CUSTOMER_VERTEX_LABEL,
            "-url",
            StringUtils.quoteJson(CSV_CUSTOMER_RECORDS),
            "--connector.csv.delimiter",
            "|");

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(34, SELECT_ALL_CUSTOMERS);
    GraphResultSet results =
        session.execute(
            ScriptGraphStatement.newInstance("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')"));
    assertThat(results).hasSize(34);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    logs.clear();
    stderr.clear();
    FileUtils.deleteDirectory(logDir);

    args =
        Lists.newArrayList(
            "unload",
            "-g",
            FRAUD_GRAPH,
            "-v",
            CUSTOMER_VERTEX_LABEL,
            "-url",
            quoteJson(unloadDir),
            "--connector.csv.delimiter",
            "|",
            "--connector.csv.maxConcurrentFiles",
            "1");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(35, unloadDir);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    logs.clear();
    stderr.clear();
    // Remove data for reload validation
    truncateTables();

    args =
        Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-v",
            CUSTOMER_VERTEX_LABEL,
            "-url",
            quoteJson(unloadDir),
            "--connector.csv.delimiter",
            "|");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(34, SELECT_ALL_CUSTOMERS);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    results =
        session.execute(
            ScriptGraphStatement.newInstance("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')"));
    assertThat(results).hasSize(34);
  }

  @Test
  void csv_full_load_unload_and_load_again_edges() throws Exception {

    List<String> args =
        Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-e",
            PLACES_EDGE_LABEL,
            "-from",
            CUSTOMER_VERTEX_LABEL,
            "-to",
            ORDER_VERTEX_LABEL,
            "-url",
            StringUtils.quoteJson(CSV_CUSTOMER_ORDER_RECORDS),
            "-m",
            CUSTOMER_ORDER_MAPPINGS,
            "--connector.csv.delimiter",
            "|");

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(14, SELECT_ALL_CUSTOMER_ORDERS);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    logs.clear();
    stderr.clear();
    GraphResultSet results =
        session.execute(
            ScriptGraphStatement.newInstance("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')"));
    assertThat(results).hasSize(14);
    FileUtils.deleteDirectory(logDir);

    args =
        Lists.newArrayList(
            "unload",
            "-g",
            FRAUD_GRAPH,
            "-e",
            PLACES_EDGE_LABEL,
            "-from",
            CUSTOMER_VERTEX_LABEL,
            "-to",
            ORDER_VERTEX_LABEL,
            "-url",
            quoteJson(unloadDir),
            "-m",
            CUSTOMER_ORDER_MAPPINGS,
            "--connector.csv.delimiter",
            "|",
            "--connector.csv.maxConcurrentFiles",
            "1");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(15, unloadDir);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    logs.clear();
    stderr.clear();
    // Remove data for reload validation
    truncateTables();

    args =
        Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-e",
            PLACES_EDGE_LABEL,
            "-from",
            CUSTOMER_VERTEX_LABEL,
            "-to",
            ORDER_VERTEX_LABEL,
            "-url",
            quoteJson(unloadDir),
            "-m",
            CUSTOMER_ORDER_MAPPINGS,
            "--connector.csv.delimiter",
            "|");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(14, SELECT_ALL_CUSTOMER_ORDERS);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    results =
        session.execute(
            ScriptGraphStatement.newInstance("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')"));
    assertThat(results).hasSize(14);
  }

  @Test
  void json_full_load_unload_and_load_again_vertices() throws Exception {

    // Load customer JSON file.
    List<String> args =
        com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-v",
            CUSTOMER_VERTEX_LABEL,
            "-url",
            StringUtils.quoteJson(JSON_CUSTOMER_RECORDS),
            "--connector.name",
            "json");

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(34, SELECT_ALL_CUSTOMERS);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    logs.clear();
    stderr.clear();
    GraphResultSet results =
        session.execute(
            ScriptGraphStatement.newInstance("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')"));
    assertThat(results).hasSize(34);
    FileUtils.deleteDirectory(logDir);

    // Unload customer JSON file
    args =
        com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList(
            "unload",
            "-g",
            FRAUD_GRAPH,
            "-v",
            CUSTOMER_VERTEX_LABEL,
            "-url",
            quoteJson(unloadDir),
            "--connector.name",
            "json",
            "--connector.json.maxConcurrentFiles",
            "1");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(34, unloadDir);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    logs.clear();
    stderr.clear();
    // Remove data for reload validation
    truncateTables();

    // Reload customer data
    args =
        com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-v",
            CUSTOMER_VERTEX_LABEL,
            "-url",
            StringUtils.quoteJson(JSON_CUSTOMER_RECORDS),
            "--connector.name",
            "json");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(34, SELECT_ALL_CUSTOMERS);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    results =
        session.execute(
            ScriptGraphStatement.newInstance("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')"));
    assertThat(results).hasSize(34);
  }

  @Test
  void json_full_load_unload_and_load_again_edges() throws Exception {

    // Load Customer Order data
    List<String> args =
        com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-e",
            PLACES_EDGE_LABEL,
            "-from",
            CUSTOMER_VERTEX_LABEL,
            "-to",
            ORDER_VERTEX_LABEL,
            "-url",
            StringUtils.quoteJson(JSON_CUSTOMER_ORDER_RECORDS),
            "-m",
            CUSTOMER_ORDER_MAPPINGS,
            "--connector.name",
            "json");

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(14, SELECT_ALL_CUSTOMER_ORDERS);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    logs.clear();
    stderr.clear();
    GraphResultSet results =
        session.execute(
            ScriptGraphStatement.newInstance("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')"));
    assertThat(results).hasSize(14);
    FileUtils.deleteDirectory(logDir);

    // Unload customer order data
    args =
        com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList(
            "unload",
            "-g",
            FRAUD_GRAPH,
            "-e",
            PLACES_EDGE_LABEL,
            "-from",
            CUSTOMER_VERTEX_LABEL,
            "-to",
            ORDER_VERTEX_LABEL,
            "--connector.name",
            "json",
            "-url",
            quoteJson(unloadDir),
            "-m",
            CUSTOMER_ORDER_MAPPINGS,
            "--connector.json.maxConcurrentFiles",
            "1");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(14, unloadDir);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    logs.clear();
    stderr.clear();
    // Remove data for reload validation
    truncateTables();

    // Reload Customer Order data
    args =
        com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-e",
            PLACES_EDGE_LABEL,
            "-from",
            CUSTOMER_VERTEX_LABEL,
            "-to",
            ORDER_VERTEX_LABEL,
            "-url",
            quoteJson(unloadDir),
            "--connector.name",
            "json",
            "-m",
            CUSTOMER_ORDER_MAPPINGS);

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(14, SELECT_ALL_CUSTOMER_ORDERS);
    TestAssertions.assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    results =
        session.execute(
            ScriptGraphStatement.newInstance("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')"));
    assertThat(results).hasSize(14);
  }

  private void createCustomerVertex() {
    // Exercise the creation of a vertex table with plain CQL with a table name different from the
    // label name.
    session.execute(
        SimpleStatement.newInstance(
                "CREATE TABLE \""
                    + FRAUD_GRAPH
                    + "\".\"Customers\" (" // table name is different from label name
                    + "\"Customerid\" uuid PRIMARY KEY, "
                    + "\"Createdtime\" timestamp, "
                    + "\"Email\" text, "
                    + "\"Firstname\" text, "
                    + "\"Lastname\" text, "
                    + "\"Phone\" text"
                    + ") WITH VERTEX LABEL \"Customer\"")
            .setExecutionProfile(SessionUtils.slowProfile(session)));
  }

  private void createOrderVertex() {
    session.execute(
        ScriptGraphStatement.newInstance(
                "schema.vertexLabel(\""
                    + ORDER_VERTEX_LABEL
                    + "\").ifNotExists()"
                    + ".partitionBy(\"Orderid\", Uuid)"
                    + ".property(\"Createdtime\", Timestamp)"
                    + ".property(\"Outcome\", Text)"
                    + ".property(\"Creditcardhashed\", Text)"
                    + ".property(\"Ipaddress\", Text)"
                    + ".property(\"Amount\", Decimal)"
                    + ".property(\"Deviceid\", Uuid)"
                    + ".create()")
            .setExecutionProfile(SessionUtils.slowProfile(session)));
  }

  private void createCustomerPlacesOrderEdge() {
    session.execute(
        ScriptGraphStatement.newInstance(
                "schema.edgeLabel(\""
                    + PLACES_EDGE_LABEL
                    + "\").from(\""
                    + CUSTOMER_VERTEX_LABEL
                    + "\").to(\""
                    + ORDER_VERTEX_LABEL
                    + "\").create()")
            .setExecutionProfile(SessionUtils.slowProfile(session)));
  }

  private void createFraudGraph() {
    session.execute(
        ScriptGraphStatement.builder(CREATE_GRAPH_GREMLIN_QUERY)
            .setQueryParam("name", FRAUD_GRAPH)
            .setQueryParam("replicationConfig", REPLICATION_CONFIG_MAP)
            .setSystemQuery(true)
            .setExecutionProfile(SessionUtils.slowProfile(session))
            .build());
  }
}
