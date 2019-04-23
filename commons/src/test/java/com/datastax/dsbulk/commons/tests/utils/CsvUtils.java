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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
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
import java.util.Objects;

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

  public static final String IP_BY_COUNTRY_MAPPING =
      "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name";

  public static final String IP_BY_COUNTRY_MAPPING_CASE_SENSITIVE =
      "0=\"BEGINNING IP ADDRESS\",1=\"ENDING IP ADDRESS\",2=\"BEGINNING IP NUMBER\",3=\"ENDING IP NUMBER\","
          + "4=\"COUNTRY CODE\",5=\"COUNTRY NAME\"";

  public static void createIpByCountryTable(CqlSession session) {
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

  public static void createWithSpacesTable(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"MYKS\" "
            + "WITH replication = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 }");
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"MYKS\".\"WITH_SPACES\" ("
            + "key int PRIMARY KEY, \"my destination\" text)");
  }

  public static void createIpByCountryCaseSensitiveTable(CqlSession session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"MYKS\" "
            + "WITH replication = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 }");
    session.execute(
        "CREATE TABLE IF NOT EXISTS \"MYKS\".\"IPBYCOUNTRY\" ("
            + "\"COUNTRY CODE\" varchar,"
            + "\"COUNTRY NAME\" varchar static,"
            + "\"BEGINNING IP ADDRESS\" inet,"
            + "\"ENDING IP ADDRESS\" inet,"
            + "\"BEGINNING IP NUMBER\" bigint,"
            + "\"ENDING IP NUMBER\" bigint,"
            + "PRIMARY KEY(\"COUNTRY CODE\", \"BEGINNING IP ADDRESS\"))");
  }

  public static PreparedStatement prepareInsertStatement(CqlSession session) {
    return session.prepare(INSERT_INTO_IP_BY_COUNTRY);
  }

  public static Map<String, Record> getRecordMap() {
    return RECORD_MAP;
  }

  public static void truncateIpByCountryTable(CqlSession session) {
    session.execute("TRUNCATE ip_by_country");
  }

  public static String firstQuery() {
    return csvRecords(CSV_RECORDS).map(CsvUtils::toQuery).firstOrError().blockingGet();
  }

  public static Record recordForRow(Row row) {
    return RECORD_MAP.get(
        row.getString("country_code")
            + Objects.requireNonNull(row.getInetAddress("beginning_ip_address")).getHostAddress());
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
    return statement.setIdempotent(true);
  }

  private static SimpleStatement toSimpleStatement(Record record) {
    SimpleStatement statement =
        SimpleStatement.newInstance(
            INSERT_INTO_IP_BY_COUNTRY,
            record.getString("ISO 3166 Country Code"),
            record.getString("Country Name"),
            record.getValue("beginning IP Address", InetAddress.class, INET_CONVERTER),
            record.getValue("ending IP Address", InetAddress.class, INET_CONVERTER),
            record.getLong("beginning IP Number"),
            record.getLong("ending IP Number"));
    return statement.setIdempotent(true);
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
    assertThat(row.getInetAddress("beginning_ip_address"))
        .isEqualTo(record.getValue("beginning IP Address", InetAddress.class, INET_CONVERTER));
    assertThat(row.getInetAddress("ending_ip_address"))
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
