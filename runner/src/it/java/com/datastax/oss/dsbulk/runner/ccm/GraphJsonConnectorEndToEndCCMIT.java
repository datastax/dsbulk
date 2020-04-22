/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */

package com.datastax.oss.dsbulk.runner.ccm;

import static com.datastax.oss.dsbulk.tests.utils.StringUtils.quoteJson;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.tests.EndToEndUtils;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Workload;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMRequirements;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMWorkload;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import java.net.URL;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(numberOfNodes = 1, workloads = @CCMWorkload(Workload.graph))
@CCMRequirements(
    compatibleTypes = Type.DSE,
    versionRequirements = @CCMVersionRequirement(type = Type.DSE, min = "6.8.0"))
@Tag("medium")
class GraphJsonConnectorEndToEndCCMIT extends GraphEndToEndCCMITBase {

  private static final URL CUSTOMER_RECORDS = ClassLoader.getSystemResource("graph/customers.json");

  private static final URL CUSTOMER_ORDER_RECORDS =
      ClassLoader.getSystemResource("graph/customer-orders.json");

  GraphJsonConnectorEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(settings = "basic.graph.name = " + FRAUD_GRAPH) CqlSession session) {
    super(ccm, session);
  }

  @Test
  void full_load_unload_and_load_again_vertices() throws Exception {

    // Load customer JSON file.
    List<String> args =
        Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-v",
            CUSTOMER_VERTEX_LABEL,
            "-url",
            StringUtils.quoteJson(CUSTOMER_RECORDS),
            "--connector.name",
            "json");

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, DataStaxBulkLoader.STATUS_OK);
    validateResultSetSize(34, SELECT_ALL_CUSTOMERS);
    GraphResultSet results =
        session.execute(
            ScriptGraphStatement.newInstance("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')"));
    assertThat(results).hasSize(34);
    FileUtils.deleteDirectory(logDir);

    // Unload customer JSON file
    args =
        Lists.newArrayList(
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
    EndToEndUtils.assertStatus(status, DataStaxBulkLoader.STATUS_OK);
    EndToEndUtils.validateOutputFiles(34, unloadDir);
    // Remove data for reload validation
    truncateTables();

    // Reload customer data
    args =
        Lists.newArrayList(
            "load",
            "-g",
            FRAUD_GRAPH,
            "-v",
            CUSTOMER_VERTEX_LABEL,
            "-url",
            StringUtils.quoteJson(CUSTOMER_RECORDS),
            "--connector.name",
            "json");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, DataStaxBulkLoader.STATUS_OK);
    validateResultSetSize(34, SELECT_ALL_CUSTOMERS);
    results =
        session.execute(
            ScriptGraphStatement.newInstance("g.V().hasLabel('" + CUSTOMER_VERTEX_LABEL + "')"));
    assertThat(results).hasSize(34);
  }

  @Test
  void full_load_unload_and_load_again_edges() throws Exception {

    // Load Customer Order data
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
            StringUtils.quoteJson(CUSTOMER_ORDER_RECORDS),
            "-m",
            CUSTOMER_ORDER_MAPPINGS,
            "--connector.name",
            "json");

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, DataStaxBulkLoader.STATUS_OK);
    validateResultSetSize(14, SELECT_ALL_CUSTOMER_ORDERS);
    GraphResultSet results =
        session.execute(
            ScriptGraphStatement.newInstance("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')"));
    assertThat(results).hasSize(14);
    FileUtils.deleteDirectory(logDir);

    // Unload customer order data
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
            "--connector.name",
            "json",
            "-url",
            quoteJson(unloadDir),
            "-m",
            CUSTOMER_ORDER_MAPPINGS,
            "--connector.json.maxConcurrentFiles",
            "1");

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, DataStaxBulkLoader.STATUS_OK);
    EndToEndUtils.validateOutputFiles(14, unloadDir);
    // Remove data for reload validation
    truncateTables();

    // Reload Customer Order data
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
            "--connector.name",
            "json",
            "-m",
            CUSTOMER_ORDER_MAPPINGS);

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    EndToEndUtils.assertStatus(status, DataStaxBulkLoader.STATUS_OK);
    validateResultSetSize(14, SELECT_ALL_CUSTOMER_ORDERS);
    results =
        session.execute(
            ScriptGraphStatement.newInstance("g.E().hasLabel('" + PLACES_EDGE_LABEL + "')"));
    assertThat(results).hasSize(14);
  }
}
