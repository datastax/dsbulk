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
    session.execute(
        "CREATE TABLE IF NOT EXISTS \""
            + FRAUD_KEYSPACE
            + "\".\""
            + CUSTOMER_TABLE
            + "\" ("
            + "\"customerid\" uuid,"
            + "\"createdtime\" timestamp,"
            + "\"email\" text,"
            + "\"firstname\" text,"
            + "\"lastname\" text,"
            + "\"phone\" text,"
            + "PRIMARY KEY(\"customerid\")) WITH VERTEX LABEL \""
            + CUSTOMER_TABLE
            + "\"");
  }

  public static void createCustomerOrderTable(Session session) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS \""
            + FRAUD_KEYSPACE
            + "\".\""
            + CUSTOMER_ORDER_TABLE
            + "\" ("
            + "\"out_customerid\" uuid,"
            + "\"in_orderid\" uuid,"
            + "PRIMARY KEY(\"out_customerid\", \"in_orderid\"))"
            + "WITH CLUSTERING ORDER BY (\"in_orderid\" ASC) "
            + "AND EDGE LABEL \""
            + CUSTOMER_ORDER_EDGE_LABEL
            + "\" FROM \""
            + CUSTOMER_TABLE
            + "\"((out_customerid)) "
            + "TO \""
            + ORDER_TABLE
            + "\"((in_orderid))");
  }

  public static void createOrderTable(Session session) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS \""
            + FRAUD_KEYSPACE
            + "\".\""
            + ORDER_TABLE
            + "\" ("
            + "    \"orderid\" uuid PRIMARY KEY,"
            + "    \"amount\" decimal,"
            + "    \"createdtime\" timestamp,"
            + "    \"creditcardhashed\" text,"
            + "    \"deviceid\" uuid,"
            + "    \"ipaddress\" text,"
            + "    \"outcome\" text"
            + ") WITH VERTEX LABEL \""
            + ORDER_TABLE
            + "\"");
  }

  public static void createGraphKeyspace(Session session) {

    String replicationConfig = "{'class': 'SimpleStrategy', 'replication_factor' : " + 1 + "}";
    String schema =
        "system.graph(name).ifNotExists().withReplication(replicationConfig).using(Native).create()";

    ((DseSession) session)
        .executeGraph(
            schema,
            ImmutableMap.<String, Object>of(
                "name", FRAUD_KEYSPACE, "replicationConfig", replicationConfig));

    ((DseSession) session)
        .getCluster()
        .getConfiguration()
        .getGraphOptions()
        .setGraphName(FRAUD_KEYSPACE);
  }
}
