/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.tests.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.*;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
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
import java.util.*;

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

  private static final URL CSV_RECORDS = ClassLoader.getSystemResource("ip-by-country-sample.csv");
  public static final URL CSV_RECORDS_UNIQUE =
      ClassLoader.getSystemResource("ip-by-country-unique.csv");
  public static final URL CSV_RECORDS_PARTIAL_BAD =
      ClassLoader.getSystemResource("ip-by-country-partial-bad.csv");
  public static final URL CSV_RECORDS_SKIP =
      ClassLoader.getSystemResource("ip-by-country-skip-bad.csv");
  public static final URL CSV_RECORDS_ERROR =
      ClassLoader.getSystemResource("ip-by-country-error.csv");

  private static final Map<String, Record> RECORD_MAP =
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

  public static BoundStatement toBoundStatement(PreparedStatement ps, Record record) {
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

  public static Flowable<Record> csvRecords() {
    return csvRecords(CSV_RECORDS);
  }

  public static Flowable<Record> csvRecords(URL url) {
    return Flowable.create(
        e -> {
          CsvParserSettings settings = new CsvParserSettings();
          settings.setHeaderExtractionEnabled(true);
          CsvParser parser = new CsvParser(settings);
          try (InputStream is = new BufferedInputStream(url.openStream())) {
            parser.beginParsing(is, "UTF-8");
            Record row;
            while ((row = parser.parseNextRecord()) != null) {
              if (e.isCancelled()) break;
              e.onNext(row);
            }
            e.onComplete();
            parser.stopParsing();
          }
        },
        BackpressureStrategy.BUFFER);
  }

  public static RequestPrime createSimpleParameterizedQuery(String query) {
    Map<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("country_code", "ascii");
    paramTypes.put("country_name", "ascii");
    paramTypes.put("beginning_ip_address", "inet");
    paramTypes.put("ending_ip_address", "inet");
    paramTypes.put("beginning_ip_number", "bigint");
    paramTypes.put("ending_ip_number", "bigint");
    Query when = new Query(query, Collections.emptyList(), new HashMap<>(), paramTypes);
    SuccessResult then = new SuccessResult(new ArrayList<>(), new HashMap<>());
    return new RequestPrime(when, then);
  }

  public static RequestPrime createParameterizedQuery(
      String query, Map<String, Object> params, Result then) {
    Map<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("country_code", "ascii");
    paramTypes.put("country_name", "ascii");
    paramTypes.put("beginning_ip_address", "inet");
    paramTypes.put("ending_ip_address", "inet");
    paramTypes.put("beginning_ip_number", "bigint");
    paramTypes.put("ending_ip_number", "bigint");

    Map<String, Object> defaultParams = new LinkedHashMap<>();
    defaultParams.put("country_code", "*");
    defaultParams.put("country_name", "*");
    defaultParams.put("beginning_ip_address", "*");
    defaultParams.put("ending_ip_address", "*");
    defaultParams.put("beginning_ip_number", "*");
    defaultParams.put("ending_ip_number", "*");

    for (String key : params.keySet()) {
      defaultParams.put(key, params.get(key));
    }

    Query when = new Query(query, Collections.emptyList(), defaultParams, paramTypes);

    return new RequestPrime(when, then);
  }
}
