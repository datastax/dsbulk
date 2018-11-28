/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import com.datastax.driver.core.Session;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.datastax.driver.dse.graph.GraphStatement;
import com.datastax.driver.dse.graph.SimpleGraphStatement;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.CUSTOMER_MAPPINGS;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.CUSTOMER_ORDER_EDGE_LABEL;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.CUSTOMER_ORDER_MAPPINGS;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.CUSTOMER_ORDER_RECORDS;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.CUSTOMER_ORDER_TABLE;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.CUSTOMER_RECORDS;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.CUSTOMER_TABLE;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.FRAUD_KEYSPACE;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.ORDER_TABLE;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.SELECT_ALL_CUSTOMER_ORDERS;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.SELECT_ALL_FROM_CUSTOMERS;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.createCustomerOrderTable;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.createCustomersTable;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.createGraphKeyspace;
import static com.datastax.dsbulk.engine.tests.graph.utils.DataCreatorUtils.createOrderTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static java.nio.file.Files.createTempDirectory;

// tests for DAT-355
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@CCMConfig(numberOfNodes = 1, workloads = {@CCMWorkload({CCMCluster.Workload.graph})})
@Tag("medium")
@CCMRequirements(compatibleTypes = DSE, versionRequirements = {@CCMVersionRequirement(type = DSE, min = "6.8.0")})
class GraphConnectorEndToEndCCMIT extends EndToEndCCMITBase {

  private final LogInterceptor logs;
  private final StreamInterceptor stderr;
  private Path logDir;
  private Path unloadDir;

  GraphConnectorEndToEndCCMIT(
      CCMCluster ccm,
      Session session,
      @LogCapture LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    super(ccm, session);
    this.logs = logs;
    this.stderr = stderr;
  }

  @BeforeAll
  void createTables() {
    createGraphKeyspace(session);
    createCustomersTable(session);
    createOrderTable(session);
    createCustomerOrderTable(session);
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @BeforeEach
  void truncateTable() {
    session.execute(CQLUtils.truncateKeyspaceTable(FRAUD_KEYSPACE, CUSTOMER_TABLE));
    session.execute(CQLUtils.truncateKeyspaceTable(FRAUD_KEYSPACE, CUSTOMER_ORDER_TABLE));
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
  void full_load_unload_and_load_again() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("-g");
    args.add(FRAUD_KEYSPACE);
    args.add("-v");
    args.add(CUSTOMER_TABLE);
    args.add("-url");
    args.add(escapeUserInput(CUSTOMER_RECORDS));
    args.add("-m");
    args.add(CUSTOMER_MAPPINGS);
    args.add("--connector.csv.delimiter");
    args.add("|");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(34, SELECT_ALL_FROM_CUSTOMERS);
    GraphStatement statement =
        new SimpleGraphStatement("g.V().hasLabel('" + CUSTOMER_TABLE + "')");
    GraphResultSet results = ((DseSession) session).executeGraph(statement);
    assertThat(results.all().size()).isEqualTo(34);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("-g");
    args.add(FRAUD_KEYSPACE);
    args.add("-v");
    args.add(CUSTOMER_TABLE);
    args.add("-url");
    args.add(escapeUserInput(unloadDir));
    args.add("-m");
    args.add(CUSTOMER_MAPPINGS);
    args.add("--connector.csv.delimiter");
    args.add("|");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(35, unloadDir);

    args = new ArrayList<>();
    args.add("load");
    args.add("-g");
    args.add(FRAUD_KEYSPACE);
    args.add("-v");
    args.add(CUSTOMER_TABLE);
    args.add("-url");
    args.add(escapeUserInput(unloadDir));
    args.add("-m");
    args.add(CUSTOMER_MAPPINGS);
    args.add("--connector.csv.delimiter");
    args.add("|");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(34, SELECT_ALL_FROM_CUSTOMERS);
    statement =
        new SimpleGraphStatement("g.V().hasLabel('" + CUSTOMER_TABLE + "')");
    results = ((DseSession) session).executeGraph(statement);
    assertThat(results.all().size()).isEqualTo(34);
    deleteDirectory(logDir);
  }

  @Test
  void full_load_unload_edges() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("-g");
    args.add(FRAUD_KEYSPACE);
    args.add("-e");
    args.add(CUSTOMER_ORDER_EDGE_LABEL);
    args.add("-from");
    args.add(CUSTOMER_TABLE);
    args.add("-to");
    args.add(ORDER_TABLE);
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
    GraphStatement statement =
        new SimpleGraphStatement("g.E().hasLabel('" + CUSTOMER_ORDER_EDGE_LABEL + "')");
    GraphResultSet results = ((DseSession) session).executeGraph(statement);
    assertThat(results.all().size()).isEqualTo(14);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("-g");
    args.add(FRAUD_KEYSPACE);
    args.add("-e");
    args.add(CUSTOMER_ORDER_EDGE_LABEL);
    args.add("-from");
    args.add(CUSTOMER_TABLE);
    args.add("-to");
    args.add(ORDER_TABLE);
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

    args = new ArrayList<>();
    args.add("load");
    args.add("-g");
    args.add(FRAUD_KEYSPACE);
    args.add("-e");
    args.add(CUSTOMER_ORDER_EDGE_LABEL);
    args.add("-from");
    args.add(CUSTOMER_TABLE);
    args.add("-to");
    args.add(ORDER_TABLE);
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
    statement =
        new SimpleGraphStatement("g.E().hasLabel('" + CUSTOMER_ORDER_EDGE_LABEL + "')");
    results = ((DseSession) session).executeGraph(statement);
    assertThat(results.all().size()).isEqualTo(14);
  }
}
