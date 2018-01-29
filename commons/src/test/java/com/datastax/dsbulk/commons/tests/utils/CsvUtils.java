/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.collect.ImmutableMap;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.conversions.Conversion;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Map;

public class CsvUtils {

  public static final Conversion<String, InetAddress> INET_CONVERTER =
      new Conversion<String, InetAddress>() {
        @Override
        public InetAddress execute(String input) {
          try {
            return InetAddress.getByName(input);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public String revert(InetAddress input) {
          return input.getHostAddress();
        }
      };

  public static final URL CSV_RECORDS = ClassLoader.getSystemResource("ip-by-country-sample.csv");

  private static final ImmutableMap<String, Record> RECORD_MAP =
      ImmutableMap.copyOf(
          csvRecords(CSV_RECORDS)
              .toMap(
                  record ->
                      record.getString("ISO 3166 Country Code")
                          + record.getString("beginning IP Address"))
              .blockingGet());

  public static final String INSERT_INTO_IP_BY_COUNTRY =
      "INSERT INTO ip_by_country "
          + "(country_code, country_name, beginning_ip_address, ending_ip_address, beginning_ip_number, ending_ip_number) "
          + "VALUES (?,?,?,?,?,?)";

  public static final String SELECT_FROM_IP_BY_COUNTRY =
      "SELECT country_code, country_name, beginning_ip_address, ending_ip_address, beginning_ip_number, ending_ip_number FROM ip_by_country";

  public static final String SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES =
      "SELECT * FROM \"MYKS\".\"WITH_SPACES\"";

  public static final String SELECT_FROM_IP_BY_COUNTRY_COMPLEX = "SELECT * FROM country_complex";

  public static final String IP_BY_COUNTRY_MAPPING =
      "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name";

  public static final String IP_BY_COUNTRY_COMPLEX_MAPPING =
      "0=country_name, 1=country_tuple, 2=country_map, 3=country_list, 4=country_set, 5=country_contacts";

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

  public static void createWithSpacesTable(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"MYKS\" "
            + "WITH replication = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 }");
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"MYKS\".\"WITH_SPACES\" ("
            + "key int PRIMARY KEY, \"my destination\" text)");
  }

  public static PreparedStatement prepareInsertStatement(Session session) {
    return session.prepare(INSERT_INTO_IP_BY_COUNTRY);
  }

  public static Map<String, Record> getRecordMap() {
    return RECORD_MAP;
  }

  public static void truncateIpByCountryTable(Session session) {
    session.execute("TRUNCATE ip_by_country");
  }

  public static String firstQuery() {
    return csvRecords(CSV_RECORDS).map(CsvUtils::toQuery).firstOrError().blockingGet();
  }

  public static Record recordForRow(Row row) {
    return RECORD_MAP.get(
        row.getString("country_code") + row.getInet("beginning_ip_address").getHostAddress());
  }

  public static Flowable<SimpleStatement> simpleStatements() {
    return csvRecords(CSV_RECORDS).map(CsvUtils::toSimpleStatement);
  }

  public static Flowable<BoundStatement> boundStatements(PreparedStatement ps) {
    return csvRecords(CSV_RECORDS).map(record -> toBoundStatement(ps, record));
  }

  public static Flowable<String> queries() {
    return csvRecords(CSV_RECORDS).map(CsvUtils::toQuery);
  }

  private static BoundStatement toBoundStatement(PreparedStatement ps, Record record) {
    BoundStatement statement =
        ps.bind(
            record.getString("ISO 3166 Country Code"),
            record.getString("Country Name"),
            record.getValue("beginning IP Address", InetAddress.class, INET_CONVERTER),
            record.getValue("ending IP Address", InetAddress.class, INET_CONVERTER),
            record.getLong("beginning IP Number"),
            record.getLong("ending IP Number"));
    statement.setIdempotent(true);
    return statement;
  }

  private static SimpleStatement toSimpleStatement(Record record) {
    SimpleStatement statement =
        new SimpleStatement(
            INSERT_INTO_IP_BY_COUNTRY,
            record.getString("ISO 3166 Country Code"),
            record.getString("Country Name"),
            record.getValue("beginning IP Address", InetAddress.class, INET_CONVERTER),
            record.getValue("ending IP Address", InetAddress.class, INET_CONVERTER),
            record.getLong("beginning IP Number"),
            record.getLong("ending IP Number"));
    statement.setIdempotent(true);
    return statement;
  }

  private static String toQuery(Record record) {
    return String.format(
        "INSERT INTO ip_by_country "
            + "(country_code, country_name, beginning_ip_address, ending_ip_address, beginning_ip_number, ending_ip_number) "
            + "VALUES ('%s','%s','%s','%s',%s,%s)",
        record.getString("ISO 3166 Country Code"),
        record.getString("Country Name"),
        record.getString("beginning IP Address"),
        record.getString("ending IP Address"),
        record.getString("beginning IP Number"),
        record.getString("ending IP Number"));
  }

  public static void assertRowEqualsRecord(Row row, Record record) {
    assertThat(row.getString("country_code")).isEqualTo(record.getString("ISO 3166 Country Code"));
    assertThat(row.getString("country_name")).isEqualTo(record.getString("Country Name"));
    assertThat(row.getInet("beginning_ip_address"))
        .isEqualTo(record.getValue("beginning IP Address", InetAddress.class, INET_CONVERTER));
    assertThat(row.getInet("ending_ip_address"))
        .isEqualTo(record.getValue("ending IP Address", InetAddress.class, INET_CONVERTER));
    assertThat(row.getLong("beginning_ip_number")).isEqualTo(record.getLong("beginning IP Number"));
    assertThat(row.getLong("ending_ip_number")).isEqualTo(record.getLong("ending IP Number"));
  }

  private static Flowable<Record> csvRecords(URL url) {
    return Flowable.create(
        e -> {
          CsvParserSettings settings = new CsvParserSettings();
          settings.setHeaderExtractionEnabled(true);
          CsvParser parser = new CsvParser(settings);
          try (InputStream is = new BufferedInputStream(url.openStream())) {
            parser.beginParsing(is, "UTF-8");
            Record row;
            while ((row = parser.parseNextRecord()) != null) {
              if (e.isCancelled()) {
                break;
              }
              e.onNext(row);
            }
            e.onComplete();
            parser.stopParsing();
          }
        },
        BackpressureStrategy.BUFFER);
  }
}
