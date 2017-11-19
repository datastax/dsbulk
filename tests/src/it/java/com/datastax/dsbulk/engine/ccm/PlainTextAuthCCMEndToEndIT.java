/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createIpByCountryTable;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.deleteIfExists;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateOutputFiles;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import com.datastax.dsbulk.tests.ccm.annotations.ClusterConfig;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import org.junit.Test;

@CCMTest
@CCMConfig(
  config = "authenticator:PasswordAuthenticator",
  jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0"
)
public class PlainTextAuthCCMEndToEndIT extends AbstractCCMEndToEndIT {

  @Inject
  @ClusterConfig(credentials = {"cassandra", "cassandra"})
  private static Session session;

  @Test
  public void full_load_unload() throws Exception {
    createIpByCountryTable(session);

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_UNIQUE.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);
    args.add("--driver.auth.provider");
    args.add("PlainTextAuthProvider");
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, SELECT_FROM_IP_BY_COUNTRY, session);

    Path unloadDir = createTempDirectory("test");
    Path outputFile = unloadDir.resolve("output-000001.csv");
    deleteIfExists(unloadDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--driver.auth.provider");
    args.add("PlainTextAuthProvider");
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, outputFile);
  }
}
