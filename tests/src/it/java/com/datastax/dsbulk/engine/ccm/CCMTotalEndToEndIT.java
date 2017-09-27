/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_WITH_SPACES;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createComplexTable;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createIpByCountryTable;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createWithSpacesTable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.categories.LongTests;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import com.datastax.dsbulk.tests.ccm.annotations.DSERequirement;
import com.datastax.dsbulk.tests.ccm.annotations.SessionConfig;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import com.datastax.dsbulk.tests.utils.EndToEndUtils;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import javax.inject.Inject;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@CCMTest
@CCMConfig(numberOfNodes = 1)
@DSERequirement(min = "5.1")
@Category(LongTests.class)
public class CCMTotalEndToEndIT extends AbstractEndToEndTestIT {

  private static final String KS = "ccmtotal";

  @Inject
  @SessionConfig(useKeyspace = SessionConfig.UseKeyspaceMode.FIXED, loggedKeyspaceName = KS)
  private static Session session;

  private static final String INSERT_INTO_IP_BY_COUNTRY_COMPLEX =
      "INSERT INTO "
          + "country_complex "
          + "(country_name, country_tuple, country_map, country_list, country_set, country_contacts) "
          + "VALUES (?,?,?,?,?,?)";

  private static final SimpleStatement READ_SUCCESSFUL_IP_BY_COUNTRY =
      new SimpleStatement("SELECT * FROM " + KS + ".ip_by_country");

  private static final SimpleStatement READ_SUCCESSFUL_COMPLEX =
      new SimpleStatement("SELECT * FROM " + KS + ".country_complex");

  private static final SimpleStatement READ_SUCCESSFUL_WITH_SPACES =
      new SimpleStatement(SELECT_FROM_WITH_SPACES);

  @SuppressWarnings("unused")
  @Inject
  private static Cluster cluster;

  @Override
  public String getKeyspace() {
    return KS;
  }

  @Test
  public void full_load_unload() throws Exception {
    /* Simple test case which attempts to load and unload data using ccm. */
    createIpByCountryTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS_UNIQUE.toExternalForm());
    customLoadArgs.add("--schema.query");
    customLoadArgs.add(INSERT_INTO_IP_BY_COUNTRY);
    customLoadArgs.add("--schema.mapping");
    customLoadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name");

    new Main(fetchCompleteArgs("load", customLoadArgs));
    validateResultSetSize(24, READ_SUCCESSFUL_IP_BY_COUNTRY);
    Path full_unload_dir = Paths.get("./target/full_unload_dir");
    Path full_unload_output_file = Paths.get("./target/full_unload_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_unload_dir);
    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("--connector.csv.url");
    customUnloadArgs.add(full_unload_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads");
    customUnloadArgs.add("1");
    customUnloadArgs.add("--schema.query");
    customUnloadArgs.add(READ_SUCCESSFUL_IP_BY_COUNTRY.toString());
    customUnloadArgs.add("--schema.mapping");
    customUnloadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name");

    new Main(fetchCompleteArgs("unload", customUnloadArgs));

    EndToEndUtils.validateOutputFiles(24, full_unload_output_file);
  }

  @Test
  public void full_load_unload_complex() throws Exception {
    /* Attempts to load and unload complex types (Collections, UDTs, etc). */
    createComplexTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("--connector.csv.url=" + CsvUtils.CSV_RECORDS_COMPLEX.toExternalForm());
    customLoadArgs.add("--schema.query=" + INSERT_INTO_IP_BY_COUNTRY_COMPLEX);
    customLoadArgs.add("--schema.keyspace=" + KS);
    customLoadArgs.add("--schema.mapping");
    customLoadArgs.add(
        "0=country_name, 1=country_tuple, 2=country_map, 3=country_list, 4=country_set, 5=country_contacts");

    new Main(fetchCompleteArgs("load", customLoadArgs));
    validateResultSetSize(5, READ_SUCCESSFUL_COMPLEX);

    Path full_unload_dir = Paths.get("./target/full_unload_dir");
    Path full_unload_output_file = Paths.get("./target/full_unload_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_unload_dir);

    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("--connector.csv.url=" + full_unload_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads=1");
    customUnloadArgs.add("--schema.keyspace=" + KS);
    customUnloadArgs.add("--schema.query=" + READ_SUCCESSFUL_COMPLEX.toString());
    customUnloadArgs.add("--schema.mapping");
    customUnloadArgs.add(
        "0=country_name, 1=country_tuple, 2=country_map, 3=country_list, 4=country_set, 5=country_contacts");

    new Main(fetchCompleteArgs("unload", customUnloadArgs));

    EndToEndUtils.validateOutputFiles(5, full_unload_output_file);
  }

  @Test
  public void full_load_unload_large_batches() throws Exception {
    /* Attempts to load and unload a larger dataset which can be batched. */
    createIpByCountryTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS.toExternalForm());
    customLoadArgs.add("--schema.query");
    customLoadArgs.add(INSERT_INTO_IP_BY_COUNTRY);
    customLoadArgs.add("--schema.mapping");
    customLoadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name");

    new Main(fetchCompleteArgs("load", customLoadArgs));
    validateResultSetSize(500, READ_SUCCESSFUL_IP_BY_COUNTRY);

    Path full_unload_dir = Paths.get("./target/full_unload_dir");
    Path full_unload_output_file = Paths.get("./target/full_unload_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_unload_dir);
    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("--connector.csv.url=" + full_unload_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads=1");
    customUnloadArgs.add("--schema.query=" + READ_SUCCESSFUL_IP_BY_COUNTRY.toString());
    customUnloadArgs.add("--schema.mapping");
    customUnloadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name");

    new Main(fetchCompleteArgs("unload", customUnloadArgs));

    EndToEndUtils.validateOutputFiles(500, full_unload_output_file);
  }

  @Test
  public void full_load_unload_with_spaces() throws Exception {
    // Attempt to load and unload data using ccm for a keyspace and table that is case-sensitive,
    // and with a column name containing spaces. The source data also has a header row containing
    // spaces, and the source data contains a multi-line value.

    // Test load
    createWithSpacesTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("-url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS_WITH_SPACES.toExternalForm());
    customLoadArgs.add("--schema.mapping");
    customLoadArgs.add("key=key,my source=my destination");
    customLoadArgs.add("-header");
    customLoadArgs.add("true");
    customLoadArgs.add("-k");
    customLoadArgs.add("MYKS");
    customLoadArgs.add("-t");
    customLoadArgs.add("WITH_SPACES");

    String[] args = fetchCompleteArgs("load", customLoadArgs);
    new Main(args);
    validateResultSetSize(1, READ_SUCCESSFUL_WITH_SPACES);

    // Test unload
    Path full_unload_dir = Paths.get("./target/full_unload_dir");
    Path full_unload_output_file = Paths.get("./target/full_unload_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_unload_dir);
    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("-url");
    customUnloadArgs.add(full_unload_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads");
    customUnloadArgs.add("1");
    customUnloadArgs.add("--schema.mapping");
    customUnloadArgs.add("key=key,my source=my destination");
    customUnloadArgs.add("-header");
    customUnloadArgs.add("true");
    customUnloadArgs.add("-k");
    customUnloadArgs.add("MYKS");
    customUnloadArgs.add("-t");
    customUnloadArgs.add("WITH_SPACES");

    new Main(fetchCompleteArgs("unload", customUnloadArgs));

    EndToEndUtils.validateOutputFiles(3, full_unload_output_file);
  }

  @Test
  public void skip_test_load_unload() throws Exception {
    /* Attempts to load and unload data, some of which will be unsuccessful. */
    createIpByCountryTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("--connector.csv.url=" + CsvUtils.CSV_RECORDS_SKIP.toExternalForm());
    customLoadArgs.add("--schema.query=" + INSERT_INTO_IP_BY_COUNTRY);
    customLoadArgs.add("--schema.mapping");
    customLoadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name");
    customLoadArgs.add("--connector.csv.skipLines=3");
    customLoadArgs.add("--connector.csv.maxLines=24");
    customLoadArgs.add("-driver.query.consistency=LOCAL_ONE");

    new Main(fetchCompleteArgs("load", customLoadArgs));
    validateResultSetSize(21, READ_SUCCESSFUL_IP_BY_COUNTRY);
    EndToEndUtils.validateBadOps(3);
    EndToEndUtils.validateExceptionsLog(3, "Source  :", "record-mapping-errors.log");

    Path full_unload_dir = Paths.get("./target/full_unload_dir");
    Path full_unload_output_file = Paths.get("./target/full_unload_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_unload_dir);

    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("--connector.csv.url=" + full_unload_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads=1");
    customUnloadArgs.add("--schema.query=" + READ_SUCCESSFUL_IP_BY_COUNTRY.toString());
    customUnloadArgs.add("--schema.mapping");
    customUnloadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name");
    new Main(fetchCompleteArgs("unload", customUnloadArgs));
    EndToEndUtils.validateOutputFiles(21, full_unload_output_file);
  }

  private void validateResultSetSize(int numOfQueries, SimpleStatement statement) {
    ResultSet set = session.execute(statement);
    List<Row> results = set.all();
    Assertions.assertThat(results.size()).isEqualTo(numOfQueries);
  }

  private String[] fetchCompleteArgs(String op, List<String> customArgs) {
    Host host = cluster.getMetadata().getAllHosts().iterator().next();
    String contact_point = host.getAddress().toString().replaceFirst("^/", "");
    String port = Integer.toString(host.getSocketAddress().getPort());
    return EndToEndUtils.fetchCompleteArgs(op, contact_point, port, customArgs);
  }

  @After
  public void clearKeyspace() {
    session.execute("DROP KEYSPACE IF EXISTS " + KS);
  }
}
