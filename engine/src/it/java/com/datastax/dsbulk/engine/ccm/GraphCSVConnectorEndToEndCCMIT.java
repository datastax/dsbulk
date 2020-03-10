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
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Workload.graph;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.DataStaxBulkLoader.STATUS_OK;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.assertStatus;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMWorkload;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import java.net.URL;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

// tests for DAT-478
@CCMConfig(numberOfNodes = 1, workloads = @CCMWorkload(graph))
@CCMRequirements(
    compatibleTypes = DSE,
    versionRequirements = @CCMVersionRequirement(type = DSE, min = "6.8.0"))
@Tag("medium")
class GraphCSVConnectorEndToEndCCMIT extends GraphEndToEndCCMITBase {

  private static final URL CUSTOMER_RECORDS = ClassLoader.getSystemResource("graph/customers.csv");

  private static final URL CUSTOMER_ORDER_RECORDS =
      ClassLoader.getSystemResource("graph/customer-orders.csv");

  GraphCSVConnectorEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(settings = "basic.graph.name = " + FRAUD_GRAPH) CqlSession session) {
    super(ccm, session);
  }

  @Test
  void full_load_unload_and_load_again_vertices() throws Exception {

    List<String> args =
        Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-v",
            CUSTOMER_VERTEX_LABEL,
            "-url",
            quoteJson(CUSTOMER_RECORDS),
            "--connector.csv.delimiter",
            "|");

    DataStaxBulkLoader dataStaxBulkLoader = new DataStaxBulkLoader(addCommonSettings(args));
    int status = dataStaxBulkLoader.run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(34, SELECT_ALL_CUSTOMERS);
    GraphResultSet results =
        session.execute(
            ScriptGraphStatement.newInstance("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')"));
    assertThat(results).hasSize(34);
    deleteDirectory(logDir);

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
    results =
        session.execute(
            ScriptGraphStatement.newInstance("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')"));
    assertThat(results).hasSize(34);
  }

  @Test
  void full_load_unload_and_load_again_edges() throws Exception {

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
            quoteJson(CUSTOMER_ORDER_RECORDS),
            "-m",
            CUSTOMER_ORDER_MAPPINGS,
            "--connector.csv.delimiter",
            "|");

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(14, SELECT_ALL_CUSTOMER_ORDERS);
    GraphResultSet results =
        session.execute(
            ScriptGraphStatement.newInstance("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')"));
    assertThat(results).hasSize(14);
    deleteDirectory(logDir);

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
    results =
        session.execute(
            ScriptGraphStatement.newInstance("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')"));
    assertThat(results).hasSize(14);
  }
}
