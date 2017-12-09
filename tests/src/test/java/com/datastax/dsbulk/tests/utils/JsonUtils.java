/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.utils;

import com.datastax.driver.core.Session;
import java.net.URL;

public class JsonUtils {

  public static final URL JSON_RECORDS = ClassLoader.getSystemResource("ip-by-country-sample.json");
  public static final URL JSON_RECORDS_UNIQUE =
      ClassLoader.getSystemResource("ip-by-country-unique.json");
  public static final URL JSON_RECORDS_COMPLEX =
      ClassLoader.getSystemResource("ip-by-country-sample-complex.json");
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

  public static final String INSERT_INTO_IP_BY_COUNTRY =
      "INSERT INTO ip_by_country "
          + "(country_code, country_name, beginning_ip_address, ending_ip_address, beginning_ip_number, ending_ip_number) "
          + "VALUES (?,?,?,?,?,?)";

  public static final String SELECT_FROM_IP_BY_COUNTRY = "SELECT * FROM ip_by_country";

  public static final String SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES =
      "SELECT * FROM \"MYKS\".\"WITH_SPACES\"";

  public static final String SELECT_FROM_IP_BY_COUNTRY_COMPLEX = "SELECT * FROM country_complex";

  public static final String IP_BY_COUNTRY_MAPPING =
      "{"
          + "beginning_ip_address=beginning_ip_address,"
          + "ending_ip_address=ending_ip_address,"
          + "beginning_ip_number=beginning_ip_number,"
          + "ending_ip_number=ending_ip_number,"
          + "country_code=country_code,"
          + "country_name=country_name"
          + "}";

  public static final String IP_BY_COUNTRY_COMPLEX_MAPPING =
      "country_name=country_name, country_tuple=country_tuple, country_map=country_map, country_list=country_list, country_set=country_set, country_contacts=country_contacts";

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

  public static void createComplexTable(Session session) {
    session.execute("CREATE TYPE contacts (alias text, numbers  frozen<list<text>>)");
    session.execute(
        "CREATE TABLE country_complex (country_name text PRIMARY KEY, "
            + "country_tuple frozen<tuple<int, text, float>>, "
            + "country_map map<text, text>,"
            + "country_list list<int>,"
            + "country_set set<float>,"
            + "country_contacts frozen<contacts>)");
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
