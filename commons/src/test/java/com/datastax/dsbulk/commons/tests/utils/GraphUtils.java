/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import com.datastax.driver.dse.DseSession;
import com.google.common.collect.ImmutableMap;

public class GraphUtils {

  private static final String REPLICATION_CONFIG_MAP =
      "{'class': 'SimpleStrategy', 'replication_factor' :1}";

  private static final String CREATE_GRAPH_GREMLIN_QUERY =
      "system.graph(name).ifNotExists().withReplication(replicationConfig).using(Native).create()";

  public static void createGraphKeyspace(DseSession session, String name) {
    session.executeGraph(
        CREATE_GRAPH_GREMLIN_QUERY,
        ImmutableMap.of("name", name, "replicationConfig", REPLICATION_CONFIG_MAP));
    session.getCluster().getConfiguration().getGraphOptions().setGraphName(name);
  }
}
