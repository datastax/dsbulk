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
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.google.common.collect.ImmutableMap;

abstract class GraphEndToEndCCMITBase extends EndToEndCCMITBase {

  protected GraphEndToEndCCMITBase(CCMCluster ccm, Session session) {
    super(ccm, session);
  }

  public static final String REPLICATION_CONFIG_MAP =
      "{'class': 'SimpleStrategy', 'replication_factor' :1}";

  public static final String CREATE_GRAPH_GREMLIN_QUERY =
      "system.graph(name).ifNotExists().withReplication(replicationConfig).using(Native).create()";

  public static final String FRAUD_GRAPH = "Fraud";

  public static final String CUSTOMER_VERTEX_LABEL = "Customer";

  public static final String ORDER_VERTEX_LABEL = "Order";

  public static final String PLACES_EDGE_LABEL = "Places";

  public static final String CUSTOMER_TABLE = "Customers";

  public static final String CUSTOMER_PLACES_ORDER_TABLE =
      CUSTOMER_VERTEX_LABEL + "__" + PLACES_EDGE_LABEL + "__" + ORDER_VERTEX_LABEL;

  public static final String CUSTOMER_ORDER_MAPPINGS =
      "Customerid = out_Customerid, Orderid = in_Orderid";

  public static final String SELECT_ALL_CUSTOMERS =
      "SELECT * FROM \"" + FRAUD_GRAPH + "\".\"" + CUSTOMER_TABLE + "\"";

  public static final String SELECT_ALL_CUSTOMER_ORDERS =
      "SELECT * FROM \"" + FRAUD_GRAPH + "\".\"" + CUSTOMER_PLACES_ORDER_TABLE + "\"";

  public void createCustomerVertex() {
    // Exercise the creation of a vertex table with plain CQL with a table name different from the
    // label name.
    session.execute(
        "CREATE TABLE \""
            + FRAUD_GRAPH
            + "\".\"Customers\" (" // table name is different from label name
            + "\"Customerid\" uuid PRIMARY KEY, "
            + "\"Createdtime\" timestamp, "
            + "\"Email\" text, "
            + "\"Firstname\" text, "
            + "\"Lastname\" text, "
            + "\"Phone\" text"
            + ") WITH VERTEX LABEL \"Customer\"");
  }

  void createOrderVertex() {
    ((DseSession) session)
        .executeGraph(
            "g.api().schema().vertexLabel(\""
                + ORDER_VERTEX_LABEL
                + "\").ifNotExists()"
                + ".partitionBy(\"Orderid\", Uuid)"
                + ".property(\"Createdtime\", Timestamp)"
                + ".property(\"Outcome\", Text)"
                + ".property(\"Creditcardhashed\", Text)"
                + ".property(\"Ipaddress\", Text)"
                + ".property(\"Amount\", Decimal)"
                + ".property(\"Deviceid\", Uuid)"
                + ".create()");
  }

  void createCustomerPlacesOrderEdge() {
    ((DseSession) session)
        .executeGraph(
            "g.api().schema().edgeLabel(\""
                + PLACES_EDGE_LABEL
                + "\").from(\""
                + CUSTOMER_VERTEX_LABEL
                + "\").to(\""
                + ORDER_VERTEX_LABEL
                + "\").create()");
  }

  void createFraudGraph() {
    createGraphKeyspace(FRAUD_GRAPH);
  }

  void createGraphKeyspace(String name) {
    ((DseSession) session)
        .executeGraph(
            CREATE_GRAPH_GREMLIN_QUERY,
            ImmutableMap.of("name", name, "replicationConfig", REPLICATION_CONFIG_MAP));
    ((DseSession) session).getCluster().getConfiguration().getGraphOptions().setGraphName(name);
  }
}
