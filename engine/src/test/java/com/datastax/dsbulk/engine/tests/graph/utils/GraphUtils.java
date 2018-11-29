package com.datastax.dsbulk.engine.tests.graph.utils;

import com.datastax.driver.core.Session;
import com.datastax.driver.dse.DseSession;
import com.google.common.collect.ImmutableMap;

public class GraphUtils {
  public static void createGraphKeyspace(Session session, String name) {

    String replicationConfig = "{'class': 'SimpleStrategy', 'replication_factor' : " + 1 + "}";
    String schema =
        "system.graph(name).ifNotExists().withReplication(replicationConfig).using(Native).create()";

    ((DseSession) session)
        .executeGraph(
            schema, ImmutableMap.of("name", name, "replicationConfig", replicationConfig));

    ((DseSession) session).getCluster().getConfiguration().getGraphOptions().setGraphName(name);
  }
}
