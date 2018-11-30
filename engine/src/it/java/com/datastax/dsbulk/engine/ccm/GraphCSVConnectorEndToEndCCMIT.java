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
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static java.nio.file.Files.createTempDirectory;

import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMWorkload;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.commons.tests.utils.CQLUtils;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

// tests for DAT-355
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@CCMConfig(
    numberOfNodes = 1,
    workloads = {@CCMWorkload({CCMCluster.Workload.graph})})
@Tag("medium")
@CCMRequirements(
    compatibleTypes = DSE,
    versionRequirements = {@CCMVersionRequirement(type = DSE, min = "6.8.0")})
class GraphCSVConnectorEndToEndCCMIT extends GraphEndToEndCCMITBase {

  private static final URL CUSTOMER_RECORDS = ClassLoader.getSystemResource("graph/customers.csv");

  private static final URL CUSTOMER_ORDER_RECORDS =
      ClassLoader.getSystemResource("graph/customer-orders.csv");

  private final LogInterceptor logs;

  private final StreamInterceptor stderr;

  private Path logDir;

  private Path unloadDir;

  GraphCSVConnectorEndToEndCCMIT(
      CCMCluster ccm,
      DseSession session,
      @LogCapture LogInterceptor logs,
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
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @BeforeEach
  void truncateTables() {
    session.execute(CQLUtils.truncateTable(FRAUD_GRAPH, CUSTOMER_TABLE));
    session.execute(CQLUtils.truncateTable(FRAUD_GRAPH, CUSTOMER_PLACES_ORDER_TABLE));
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
    deleteDirectory(unloadDir);
  }

  @BeforeEach
  void clearLogs() {
    logs.clear();
    stderr.clear();
  }

  @Test
  void full_load_unload_and_load_again_vertices() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("-g");
    args.add(FRAUD_GRAPH);
    args.add("-v");
    args.add(CUSTOMER_VERTEX_LABEL);
    args.add("-url");
    args.add(escapeUserInput(CUSTOMER_RECORDS));
    args.add("--connector.csv.delimiter");
    args.add("|");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(34, SELECT_ALL_CUSTOMERS);
    GraphResultSet results =
        ((DseSession) session).executeGraph("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')");
    assertThat(results).hasSize(34);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("-g");
    args.add(FRAUD_GRAPH);
    args.add("-v");
    args.add(CUSTOMER_VERTEX_LABEL);
    args.add("-url");
    args.add(escapeUserInput(unloadDir));
    args.add("--connector.csv.delimiter");
    args.add("|");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(35, unloadDir);
    // Remove data for reload validation
    truncateTables();

    args = new ArrayList<>();
    args.add("load");
    args.add("-g");
    args.add(FRAUD_GRAPH);
    args.add("-v");
    args.add(CUSTOMER_VERTEX_LABEL);
    args.add("-url");
    args.add(escapeUserInput(unloadDir));
    args.add("--connector.csv.delimiter");
    args.add("|");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(34, SELECT_ALL_CUSTOMERS);
    results =
        ((DseSession) session).executeGraph("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')");
    assertThat(results).hasSize(34);
  }

  @Test
  void full_load_unload_and_load_again_edges() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("-g");
    args.add(FRAUD_GRAPH);
    args.add("-e");
    args.add(PLACES_EDGE_LABEL);
    args.add("-from");
    args.add(CUSTOMER_VERTEX_LABEL);
    args.add("-to");
    args.add(ORDER_VERTEX_LABEL);
    args.add("-url");
    args.add(escapeUserInput(CUSTOMER_ORDER_RECORDS));
    args.add("-m");
    args.add(CUSTOMER_ORDER_MAPPINGS);
    args.add("--connector.csv.delimiter");
    args.add("|");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(14, SELECT_ALL_CUSTOMER_ORDERS);
    GraphResultSet results =
        ((DseSession) session).executeGraph("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')");
    assertThat(results).hasSize(14);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("-g");
    args.add(FRAUD_GRAPH);
    args.add("-e");
    args.add(PLACES_EDGE_LABEL);
    args.add("-from");
    args.add(CUSTOMER_VERTEX_LABEL);
    args.add("-to");
    args.add(ORDER_VERTEX_LABEL);
    args.add("-url");
    args.add(escapeUserInput(unloadDir));
    args.add("-m");
    args.add(CUSTOMER_ORDER_MAPPINGS);
    args.add("--connector.csv.delimiter");
    args.add("|");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(15, unloadDir);
    // Remove data for reload validation
    truncateTables();

    args = new ArrayList<>();
    args.add("load");
    args.add("-g");
    args.add(FRAUD_GRAPH);
    args.add("-e");
    args.add(PLACES_EDGE_LABEL);
    args.add("-from");
    args.add(CUSTOMER_VERTEX_LABEL);
    args.add("-to");
    args.add(ORDER_VERTEX_LABEL);
    args.add("-url");
    args.add(escapeUserInput(unloadDir));
    args.add("-m");
    args.add(CUSTOMER_ORDER_MAPPINGS);
    args.add("--connector.csv.delimiter");
    args.add("|");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(14, SELECT_ALL_CUSTOMER_ORDERS);
    results = ((DseSession) session).executeGraph("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')");
    assertThat(results).hasSize(14);
  }
}
