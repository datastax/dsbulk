/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.tests.utils;

import com.datastax.driver.core.Session;
import java.net.URL;

public class JsonUtils {

  public static final URL JSON_RECORDS = ClassLoader.getSystemResource("ip-by-country-sample.json");
  public static final URL JSON_RECORDS_UNIQUE =
      ClassLoader.getSystemResource("ip-by-country-unique.json");
  public static final URL JSON_RECORDS_COMPLEX = ClassLoader.getSystemResource("complex.json");
  public static final URL JSON_RECORDS_CRLF =
      ClassLoader.getSystemResource("ip-by-country-crlf.json");
  public static final URL JSON_RECORDS_PARTIAL_BAD =
      ClassLoader.getSystemResource("ip-by-country-partial-bad.json");
  public static final URL JSON_RECORDS_SKIP =
      ClassLoader.getSystemResource("ip-by-country-skip-bad.json");
  public static final URL JSON_RECORDS_ERROR =
      ClassLoader.getSystemResource("ip-by-country-error.json");
  public static final URL JSON_RECORDS_WITH_SPACES =
      ClassLoader.getSystemResource("with-spaces.json");
  public static final URL JSON_RECORDS_WITH_COMMENTS =
      ClassLoader.getSystemResource("comments.json");

  public static final String INSERT_INTO_IP_BY_COUNTRY =
      "INSERT INTO ip_by_country "
          + "(country_code, country_name, beginning_ip_address, ending_ip_address, beginning_ip_number, ending_ip_number) "
          + "VALUES (?,?,?,?,?,?)";

  public static final String SELECT_FROM_IP_BY_COUNTRY = "SELECT * FROM ip_by_country";

  public static final String SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES =
      "SELECT * FROM \"MYKS\".\"WITH_SPACES\"";

  public static final String IP_BY_COUNTRY_MAPPING =
      "{"
          + "beginning_ip_address=beginning_ip_address,"
          + "ending_ip_address=ending_ip_address,"
          + "beginning_ip_number=beginning_ip_number,"
          + "ending_ip_number=ending_ip_number,"
          + "country_code=country_code,"
          + "country_name=country_name"
          + "}";

  public static void createIpByCountryTable(Session session) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS ip_by_country ("
            + "country_code varchar,"
            + "country_name varchar static,"
            + "beginning_ip_address inet,"
            + "ending_ip_address inet,"
            + "beginning_ip_number bigint,"
            + "ending_ip_number bigint,"
            + "PRIMARY KEY(country_code, beginning_ip_address))");
  }

  public static void createIpByCountryTable(Session session, String keyspace) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS "
            + keyspace
            + ".ip_by_country ("
            + "country_code varchar,"
            + "country_name varchar static,"
            + "beginning_ip_address inet,"
            + "ending_ip_address inet,"
            + "beginning_ip_number bigint,"
            + "ending_ip_number bigint,"
            + "PRIMARY KEY(country_code, beginning_ip_address))");
  }

  public static void createWithSpacesTable(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"MYKS\" "
            + "WITH replication = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 }");
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"MYKS\".\"WITH_SPACES\" ("
            + "key int PRIMARY KEY, \"my destination\" text)");
  }
}
