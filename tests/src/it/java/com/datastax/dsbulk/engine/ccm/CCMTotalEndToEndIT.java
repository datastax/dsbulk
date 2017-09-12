/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.tests.utils.CsvUtils.createComplexTable;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createIpByCountryTable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.categories.LongTests;
import com.datastax.dsbulk.tests.ccm.CCMRule;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@CCMTest
@CCMConfig(numberOfNodes = 1)
@Category(LongTests.class)
public class CCMTotalEndToEndIT {
  @Rule @ClassRule public static CCMRule ccm = new CCMRule();

  private static final String KS = "test12";

  @Inject
  @SessionConfig(useKeyspace = SessionConfig.UseKeyspaceMode.FIXED, loggedKeyspaceName = KS)
  static Session session;

  public static final String INSERT_INTO_IP_BY_COUNTRY =
      "INSERT INTO "
          + KS
          + ".ip_by_country "
          + "(country_code, country_name, beginning_ip_address, ending_ip_address, beginning_ip_number, ending_ip_number) "
          + "VALUES (?,?,?,?,?,?)";

  public static final String INSERT_INTO_IP_BY_COUNTRY_COMPLEX =
      "INSERT INTO "
          + KS
          + ".country_complex "
          + "(country_name, country_tuple, country_map, country_list, country_set, country_contacts) "
          + "VALUES (?,?,?,?,?,?)";

  private static final SimpleStatement READ_SUCCESFUL_IP_BY_COUNTRY =
      new SimpleStatement("SELECT * FROM " + KS + ".ip_by_country");

  private static final SimpleStatement READ_SUCCESFUL_COMPLEX =
      new SimpleStatement("SELECT * FROM " + KS + ".country_complex");

  @Inject static Cluster cluster;
  String contact_point;
  String port;

  @Before
  public void setupCCM() {
    Host host = cluster.getMetadata().getAllHosts().iterator().next();
    contact_point = host.getAddress().toString().replaceFirst("^/", "");
    port = Integer.toString(host.getSocketAddress().getPort());
  }

  @After
  public void clearKeyspace() {
    session.execute("DROP KEYSPACE IF EXISTS " + KS);
  }

  @Test
  public void full_load_unload() throws Exception {
    /* Simple test case which attempts to load and unload data using ccm. */
    createIpByCountryTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("load");
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS_UNIQUE.toExternalForm());
    customLoadArgs.add("--schema.statement");
    customLoadArgs.add(INSERT_INTO_IP_BY_COUNTRY);
    customLoadArgs.add(
        "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}");

    new Main(fetchCompleteArgs(customLoadArgs));
    validateResultSetSize(24, READ_SUCCESFUL_IP_BY_COUNTRY);
    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_load_dir);
    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("unload");
    customUnloadArgs.add("--connector.csv.url");
    customUnloadArgs.add(full_load_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads");
    customUnloadArgs.add("1");
    customUnloadArgs.add("--schema.statement");
    customUnloadArgs.add(READ_SUCCESFUL_IP_BY_COUNTRY.toString());
    customUnloadArgs.add(
        "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}");

    new Main(fetchCompleteArgs(customUnloadArgs));

    EndToEndUtils.validateOutputFiles(24, full_load_output_file);
  }

  @Test
  public void full_load_unload_complex() throws Exception {
    /* Attempts to load and unload complex types (Collections, UDTs, etc). */
    createComplexTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("load");
    customLoadArgs.add("--connector.csv.url=" + CsvUtils.CSV_RECORDS_COMPLEX.toExternalForm());
    customLoadArgs.add("--schema.statement=" + INSERT_INTO_IP_BY_COUNTRY_COMPLEX);
    customLoadArgs.add(
        "--schema.mapping={0=country_name, 1=country_tuple, 2=country_map, 3=country_list, 4=country_set, 5=country_contacts}");

    new Main(fetchCompleteArgs(customLoadArgs));
    validateResultSetSize(5, READ_SUCCESFUL_COMPLEX);

    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_load_dir);

    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("unload");
    customUnloadArgs.add("--connector.csv.url=" + full_load_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads=1");
    customUnloadArgs.add("--schema.statement=" + READ_SUCCESFUL_COMPLEX.toString());
    customUnloadArgs.add(
        "--schema.mapping={0=country_name, 1=country_tuple, 2=country_map, 3=country_list, 4=country_set, 5=country_contacts}");

    new Main(fetchCompleteArgs(customUnloadArgs));

    EndToEndUtils.validateOutputFiles(5, full_load_output_file);
  }

  @Test
  public void full_load_unload_large_batches() throws Exception {
    /* Attempts to load and unload a larger dataset which can be batched. */
    createIpByCountryTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("load");
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS.toExternalForm());
    customLoadArgs.add("--schema.statement");
    customLoadArgs.add(INSERT_INTO_IP_BY_COUNTRY);
    customLoadArgs.add(
        "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}");

    new Main(fetchCompleteArgs(customLoadArgs));
    validateResultSetSize(500, READ_SUCCESFUL_IP_BY_COUNTRY);

    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_load_dir);
    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("unload");
    customUnloadArgs.add("--connector.csv.url=" + full_load_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads=1");
    customUnloadArgs.add("--schema.statement=" + READ_SUCCESFUL_IP_BY_COUNTRY.toString());
    customUnloadArgs.add(
        "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}");

    new Main(fetchCompleteArgs(customUnloadArgs));

    EndToEndUtils.validateOutputFiles(500, full_load_output_file);
  }

  @Test
  public void skip_test_load_unload() throws Exception {
    /* Attempts to load and unload data, some of which will be unsuccessful. */
    createIpByCountryTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("load");
    customLoadArgs.add("--connector.csv.url=" + CsvUtils.CSV_RECORDS_SKIP.toExternalForm());
    customLoadArgs.add("--schema.statement=" + INSERT_INTO_IP_BY_COUNTRY);
    customLoadArgs.add(
        "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}");
    customLoadArgs.add("--connector.csv.skipLines=3");
    customLoadArgs.add("--connector.csv.maxLines=24");
    customLoadArgs.add("-driver.query.consistency=LOCAL_ONE");

    new Main(fetchCompleteArgs(customLoadArgs));
    validateResultSetSize(21, READ_SUCCESFUL_IP_BY_COUNTRY);
    EndToEndUtils.validateBadOps(3);
    EndToEndUtils.validateExceptionsLog(3, "Source  :", "record-mapping-errors.log");

    Path full_load_dir = Paths.get("./full_load_dir");
    Path full_load_output_file = Paths.get("./full_load_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_load_dir);

    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("unload");
    customUnloadArgs.add("--connector.csv.url=" + full_load_dir.toString());
    customUnloadArgs.add("--connector.csv.maxThreads=1");
    customUnloadArgs.add("--schema.statement=" + READ_SUCCESFUL_IP_BY_COUNTRY.toString());
    customUnloadArgs.add(
        "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}");
    new Main(fetchCompleteArgs(customUnloadArgs));
    EndToEndUtils.validateOutputFiles(21, full_load_output_file);
  }

  private void validateResultSetSize(int numOfQueries, SimpleStatement statement) {
    ResultSet set = session.execute(statement);
    List<Row> results = set.all();
    Assertions.assertThat(results.size()).isEqualTo(numOfQueries);
  }

  private String[] fetchCompleteArgs(List<String> customArgs) {
    List<String> commonArgs = new LinkedList<>();
    commonArgs.add("--log.outputDirectory");
    commonArgs.add("./target");
    commonArgs.add("--connector.name");
    commonArgs.add("csv");
    commonArgs.add("--driver.query.consistency");
    commonArgs.add("ONE");
    commonArgs.add("--driver.hosts");
    commonArgs.add(contact_point);
    commonArgs.add("--driver.port");
    commonArgs.add(port);
    customArgs.addAll(commonArgs);
    return customArgs.stream().toArray(String[]::new);
  }
}
