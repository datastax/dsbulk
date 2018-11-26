package com.datastax.dsbulk.engine.tests.graph.utils;

import com.datastax.driver.core.Session;

import java.net.URL;

public class DataCreatorUtils {
  public static final String FRAUD_KEYSPACE = "fraud";
  public static final String CUSTOMER_TABLE = "customer";
  public static final String CUSTOMER_MAPPINGS =
      "customerid = customerid, " +
          "firstname = firstname, " +
          "lastname = lastname, " +
          "createdtime = createdtime, " +
          "email = email, " +
          "phone = phone";

  public static final URL CUSTOMER_RECORDS = ClassLoader.getSystemResource(
      "graph/customers.csv"
  );

  public static final String SELECT_ALL_FROM_CUSTOMERS =
      "SELECT * from " + FRAUD_KEYSPACE + "." + CUSTOMER_TABLE;

  public static void createCustomersTable(Session session) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"" + FRAUD_KEYSPACE + "\".\"" + CUSTOMER_TABLE + "\" ("
            + "\"" + CUSTOMER_TABLE + "id\" uuid,"
            + "\"createdtime\" timestamp,"
            + "\"email\" text,"
            + "\"firstname\" text,"
            + "\"lastname\" text,"
            + "\"phone\" text,"
            + "PRIMARY KEY(\"" + CUSTOMER_TABLE + "id\"))");
  }

  public static void truncateCustomersTable(Session session) {
    session.execute("TRUNCATE " + FRAUD_KEYSPACE + "." + CUSTOMER_TABLE);
  }


  public static void createGraphKeyspace(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"" + FRAUD_KEYSPACE + "\" "
            + "WITH replication = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 1 }");

  }
}
