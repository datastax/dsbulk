package com.datastax.dsbulk.engine.tests.graph.utils;

import com.datastax.driver.core.Session;

import java.net.URL;

public class DataCreatorUtils {
  public static final String FRAUD_KEYSPACE = "fraud";
  public static final String CUSTOMER_TABLE = "customer";
  public static final String CUSTOMER_ORDER_TABLE = "customer__places__order";
  public static final String CUSTOMER_MAPPINGS =
      "customerid = customerid, " +
          "firstname = firstname, " +
          "lastname = lastname, " +
          "createdtime = createdtime, " +
          "email = email, " +
          "phone = phone";
  public static final String CUSTOMER_ORDER_MAPPINGS =
      "customerid = out_customerid, orderid = in_orderid";

  public static final URL CUSTOMER_RECORDS = ClassLoader.getSystemResource(
      "graph/customers.csv"
  );

  public static final URL CUSTOMER_ORDER_RECORDS = ClassLoader.getSystemResource(
      "graph/customerOrders.csv"
  );

  public static final String SELECT_ALL_FROM_CUSTOMERS =
      "SELECT * from " + FRAUD_KEYSPACE + "." + CUSTOMER_TABLE;

  public static final String SELECT_ALL_CUSTOMER_ORDERS =
      "SELECT * from " + FRAUD_KEYSPACE + "." + CUSTOMER_ORDER_TABLE;

  public static void createCustomersTable(Session session) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"" + FRAUD_KEYSPACE + "\".\"" + CUSTOMER_TABLE + "\" ("
            + "\"customerid\" uuid,"
            + "\"createdtime\" timestamp,"
            + "\"email\" text,"
            + "\"firstname\" text,"
            + "\"lastname\" text,"
            + "\"phone\" text,"
            + "PRIMARY KEY(\"customerid\"))");
  }

  public static void createCustomerOrderTable(Session session) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"" + FRAUD_KEYSPACE + "\".\"" + CUSTOMER_ORDER_TABLE + "\" ("
            + "\"out_customerid\" uuid,"
            + "\"in_orderid\" timestamp,"
            + "PRIMARY KEY(\"out_customerid\", \"in_orderid\"))"
            + "WITH CLUSTERING ORDER BY (\"in_orderid\" ASC)");
  }

  public static void truncateCustomersTable(Session session) {
    session.execute("TRUNCATE " + FRAUD_KEYSPACE + "." + CUSTOMER_TABLE);
  }


  public static void truncateCustomerOrderTable(Session session) {
    session.execute("TRUNCATE " + FRAUD_KEYSPACE + "." + CUSTOMER_ORDER_TABLE);
  }

  public static void createGraphKeyspace(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"" + FRAUD_KEYSPACE + "\" "
            + "WITH replication = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 1 }");

  }
}
