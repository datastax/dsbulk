/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import org.junit.Ignore;
import org.junit.Test;

/** TODO temporary, remove when end-to-end integration tests are available */
@Ignore
public class CSVLoadWorkflowIT {

  @Test
  public void should_write() throws Exception {

    Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    Session session = cluster.connect();
    session.execute(
        "create keyspace if not exists test with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    session.execute("USE test");
    CsvUtils.createIpByCountryTable(session);

    String[] args = {
      "load",
      "--log.directory",
      "./target",
      "--connector.csv.header",
      "true",
      "--connector.csv.url",
      CsvUtils.CSV_RECORDS.toExternalForm(),
      "--schema.keyspace",
      "test",
      "--schema.table",
      "ip_by_country",
      "--schema.mapping",
      "{"
          + "\"beginning IP Address\"=beginning_ip_address,"
          + "\"ending IP Address\"=ending_ip_address,"
          + "\"beginning IP Number\"=beginning_ip_number,"
          + "\"ending IP Number\"=ending_ip_number,"
          + "\"ISO 3166 Country Code\"=country_code,"
          + "\"Country Name\"=country_name"
          + "}"
    };

    new Main(args);
  }
}
