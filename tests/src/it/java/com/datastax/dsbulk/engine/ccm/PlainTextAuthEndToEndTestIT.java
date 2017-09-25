/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.tests.utils.CsvUtils.createIpByCountryTable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.categories.LongTests;
import com.datastax.dsbulk.tests.ccm.CCMCluster;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import com.datastax.dsbulk.tests.utils.EndToEndUtils;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import javax.inject.Inject;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@CCMTest
@CCMConfig(
  numberOfNodes = 1,
  config = "authenticator:PasswordAuthenticator",
  jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0"
)
@Category(LongTests.class)
public class PlainTextAuthEndToEndTestIT extends AbstractEndToEndTestIT {

  @Inject private static CCMCluster ccm;

  @Test
  public void full_load_unload() throws Exception {
    InetAddress cp = ccm.getInitialContactPoints().get(0);
    PlainTextAuthProvider authProvider = new PlainTextAuthProvider("cassandra", "cassandra");
    Cluster cluster =
        Cluster.builder()
            .addContactPoints(cp)
            .withPort(ccm.getBinaryPort())
            .withAuthProvider(authProvider)
            .build();
    session = cluster.connect();
    Host host = cluster.getMetadata().getAllHosts().iterator().next();
    contact_point = host.getAddress().toString().replaceFirst("^/", "");
    port = Integer.toString(host.getSocketAddress().getPort());
    session.execute(createKeyspace);
    session.execute(USE_KEYSPACE);

    createIpByCountryTable(session);

    /* Simple test case which attempts to load and unload data using ccm. */
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("load");
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS_UNIQUE.toExternalForm());
    customLoadArgs.add("--schema.query");
    customLoadArgs.add(INSERT_INTO_IP_BY_COUNTRY);
    customLoadArgs.add("--schema.mapping");
    customLoadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name");
    customLoadArgs.add("--driver.auth.provider");
    customLoadArgs.add("PlainTextAuthProvider");
    customLoadArgs.add("--driver.auth.username");
    customLoadArgs.add("cassandra");
    customLoadArgs.add("--driver.auth.password");
    customLoadArgs.add("cassandra");

    new Main(fetchCompleteArgs("load", customLoadArgs));
    validateResultSetSize(24, READ_SUCCESSFUL_IP_BY_COUNTRY);
    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_load_dir);
    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("--connector.csv.url");
    customUnloadArgs.add(full_load_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads");
    customUnloadArgs.add("1");
    customUnloadArgs.add("--schema.query");
    customUnloadArgs.add(READ_SUCCESSFUL_IP_BY_COUNTRY.toString());
    customUnloadArgs.add("--driver.auth.provider");
    customUnloadArgs.add("PlainTextAuthProvider");
    customUnloadArgs.add("--driver.auth.username");
    customUnloadArgs.add("cassandra");
    customUnloadArgs.add("--driver.auth.password");
    customUnloadArgs.add("cassandra");

    new Main(fetchCompleteArgs("unload", customUnloadArgs));

    EndToEndUtils.validateOutputFiles(24, full_load_output_file);
  }

  private void validateResultSetSize(int numOfQueries, SimpleStatement statement) {
    ResultSet set = session.execute(statement);
    List<Row> results = set.all();
    Assertions.assertThat(results.size()).isEqualTo(numOfQueries);
  }

  private String[] fetchCompleteArgs(String op, List<String> customArgs) {
    customArgs.add("--schema.mapping");
    customArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name");
    customArgs.add("--connector.csv.maxThreads");
    customArgs.add("1");
    return EndToEndUtils.fetchCompleteArgs(op, contact_point, port, customArgs);
  }

  @Override
  public String getKeyspace() {
    return "plainTextAuth";
  }
}
