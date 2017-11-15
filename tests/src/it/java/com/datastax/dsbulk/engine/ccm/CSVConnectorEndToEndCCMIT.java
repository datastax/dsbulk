/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_COMPLEX;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_SKIP;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_WITH_SPACES;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_COMPLEX_MAPPING;
import static com.datastax.dsbulk.tests.utils.CsvUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY_COMPLEX;
import static com.datastax.dsbulk.tests.utils.CsvUtils.SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createComplexTable;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createIpByCountryTable;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createWithSpacesTable;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateBadOps;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateExceptionsLog;
import static com.datastax.dsbulk.tests.utils.EndToEndUtils.validateOutputFiles;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.ccm.CCMCluster;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(numberOfNodes = 1)
@Tag("ccm")
class CSVConnectorEndToEndCCMIT extends EndToEndCCMITBase {

  private Path unloadDir;
  private Path outputFile;

  CSVConnectorEndToEndCCMIT(CCMCluster ccm, Session session) {
    super(ccm, session);
  }

  @BeforeAll
  void createTables() {
    createIpByCountryTable(session);
    createComplexTable(session);
    createWithSpacesTable(session);
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    unloadDir = createTempDirectory("test");
    outputFile = unloadDir.resolve("output-000001.csv");
  }

  @AfterEach
  void deleteDirs() throws IOException {
    deleteRecursively(unloadDir, ALLOW_INSECURE);
  }

  /** Simple test case which attempts to load and unload data using ccm. */
  @Test
  void full_load_unload() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
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

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, SELECT_FROM_IP_BY_COUNTRY);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
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
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, outputFile);
  }

  /** Simple test case which attempts to load and unload data using ccm and compression (LZ4). */
  @Test
  void full_load_unload_lz4() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("LZ4");
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

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, SELECT_FROM_IP_BY_COUNTRY);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("LZ4");
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
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, outputFile);
  }

  /** Simple test case which attempts to load and unload data using ccm and compression (Snappy). */
  @Test
  void full_load_unload_snappy() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
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

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, SELECT_FROM_IP_BY_COUNTRY);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
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
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, outputFile);
  }

  /** Attempts to load and unload complex types (Collections, UDTs, etc). */
  @Test
  void full_load_unload_complex() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_COMPLEX.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("country_complex");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_COMPLEX_MAPPING);

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(5, SELECT_FROM_IP_BY_COUNTRY_COMPLEX);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("country_complex");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_COMPLEX_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(5, outputFile);
  }

  /** Attempts to load and unload a larger dataset which can be batched. */
  @Test
  void full_load_unload_large_batches() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS.toExternalForm());
    args.add("--connector.csv.header");
    args.add("true");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(500, SELECT_FROM_IP_BY_COUNTRY);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
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
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(500, outputFile);
  }

  /**
   * Attempt to load and unload data using ccm for a keyspace and table that is case-sensitive, and
   * with a column name containing spaces. The source data also has a header row containing spaces,
   * and the source data contains a multi-line value.
   */
  @Test
  void full_load_unload_with_spaces() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("-url");
    args.add(CSV_RECORDS_WITH_SPACES.toExternalForm());
    args.add("--schema.mapping");
    args.add("key=key,my source=my destination");
    args.add("-header");
    args.add("true");
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(1, SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("-url");
    args.add(unloadDir.toString());
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.mapping");
    args.add("key=key,my source=my destination");
    args.add("-header");
    args.add("true");
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(3, outputFile);
  }

  /** Attempts to load and unload data, some of which will be unsuccessful. */
  @Test
  void skip_test_load_unload() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_SKIP.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);
    args.add("--connector.csv.skipRecords");
    args.add("3");
    args.add("--connector.csv.maxRecords");
    args.add("24");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(21, SELECT_FROM_IP_BY_COUNTRY);
    validateBadOps(3);
    validateExceptionsLog(3, "Source  :", "mapping-errors.log");

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
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
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(21, outputFile);
  }
}
