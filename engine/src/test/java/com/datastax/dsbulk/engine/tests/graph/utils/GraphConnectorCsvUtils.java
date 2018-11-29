package com.datastax.dsbulk.engine.tests.graph.utils;

import com.datastax.driver.core.Session;
import com.datastax.driver.dse.DseSession;
import com.google.common.collect.ImmutableMap;
import java.net.URL;

public class GraphConnectorCsvUtils {
  public static final String FRAUD_KEYSPACE = "fraud";
  public static final String CUSTOMER_TABLE = "customer";
  public static final String CUSTOMER_ORDER_EDGE_LABEL = "places";
  public static final String CUSTOMER_ORDER_TABLE =
      "customer__" + CUSTOMER_ORDER_EDGE_LABEL + "__order";
  public static final String ORDER_TABLE = "order";
  public static final String CUSTOMER_MAPPINGS =
      "customerid = customerid, "
          + "firstname = firstname, "
          + "lastname = lastname, "
          + "createdtime = createdtime, "
          + "email = email, "
          + "phone = phone";
  public static final String CUSTOMER_ORDER_MAPPINGS =
      "customerid = out_customerid, orderid = in_orderid";

  public static final URL CUSTOMER_RECORDS = ClassLoader.getSystemResource("graph/customers.csv");

  public static final URL CUSTOMER_ORDER_RECORDS =
      ClassLoader.getSystemResource("graph/customer-orders.csv");

  public static final String SELECT_ALL_FROM_CUSTOMERS =
      "SELECT * from " + FRAUD_KEYSPACE + "." + CUSTOMER_TABLE;

  public static final String SELECT_ALL_CUSTOMER_ORDERS =
      "SELECT * from " + FRAUD_KEYSPACE + "." + CUSTOMER_ORDER_TABLE;

  public static void createCustomersTable(Session session) {
    ((DseSession)session).executeGraph(
        "g.api().schema().vertexLabel(\"customer\")" +
            ".ifNotExists().partitionBy(\"customerid\", Uuid)" +
            ".property(\"firstname\", Text)" +
            ".property(\"lastname\", Text)" +
            ".property(\"email\", Text)" +
            ".property(\"phone\", Text)" +
            ".property(\"createdtime\", Timestamp)" +
            ".create();"
    );
  }

  public static void createCustomerOrderTable(Session session) {
    ((DseSession)session).executeGraph("g.api().schema().edgeLabel(\"places\").from(\"customer\").to(\"order\").create()");
  }

  public static void createOrderTable(Session session) {
    ((DseSession)session).executeGraph(
        "g.api().schema().vertexLabel(\"order\").ifNotExists().partitionBy(\"orderid\", Uuid).property(\"createdtime\", Timestamp).property(\"outcome\", Text).property(\"creditcardhashed\", Text).property(\"ipaddress\", Text).property(\"amount\", Decimal).property(\"deviceid\", Uuid).create()");
  }

  public static void createGraphKeyspace(Session session) {

    String replicationConfig = "{'class': 'SimpleStrategy', 'replication_factor' : " + 1 + "}";
    String schema =
        "system.graph(name).ifNotExists().withReplication(replicationConfig).using(Native).create()";

    ((DseSession) session)
        .executeGraph(
            schema,
            ImmutableMap.of(
                "name", FRAUD_KEYSPACE, "replicationConfig", replicationConfig));

    ((DseSession) session)
        .getCluster()
        .getConfiguration()
        .getGraphOptions()
        .setGraphName(FRAUD_KEYSPACE);
  }
}
