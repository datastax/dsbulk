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

import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.utils.CQLUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

abstract class GraphEndToEndCCMITBase extends EndToEndCCMITBase {

  private static final String REPLICATION_CONFIG_MAP =
      "{'class': 'SimpleStrategy', 'replication_factor' :1}";

  private static final String CREATE_GRAPH_GREMLIN_QUERY =
      "system.graph(name).ifNotExists().withReplication(replicationConfig).coreEngine().create()";

  static final String FRAUD_GRAPH = "Fraud";

  static final String CUSTOMER_VERTEX_LABEL = "Customer";

  static final String ORDER_VERTEX_LABEL = "Order";

  static final String PLACES_EDGE_LABEL = "Places";

  static final String CUSTOMER_TABLE = "Customers";

  static final String CUSTOMER_PLACES_ORDER_TABLE =
      CUSTOMER_VERTEX_LABEL + "__" + PLACES_EDGE_LABEL + "__" + ORDER_VERTEX_LABEL;

  static final String CUSTOMER_ORDER_MAPPINGS =
      "Customerid = Customer_Customerid, Orderid = Order_Orderid";

  static final String SELECT_ALL_CUSTOMERS =
      "SELECT * FROM \"" + FRAUD_GRAPH + "\".\"" + CUSTOMER_TABLE + "\"";

  static final String SELECT_ALL_CUSTOMER_ORDERS =
      "SELECT * FROM \"" + FRAUD_GRAPH + "\".\"" + CUSTOMER_PLACES_ORDER_TABLE + "\"";

  GraphEndToEndCCMITBase(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
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

  void createCustomerVertex() {
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

  void createOrderVertex() {
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

  void createCustomerPlacesOrderEdge() {
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

  void createFraudGraph() {
    session.execute(
        ScriptGraphStatement.builder(CREATE_GRAPH_GREMLIN_QUERY)
            .setQueryParam("name", FRAUD_GRAPH)
            .setQueryParam("replicationConfig", REPLICATION_CONFIG_MAP)
            .setSystemQuery(true)
            .setExecutionProfile(SessionUtils.slowProfile(session))
            .build());
  }
}
