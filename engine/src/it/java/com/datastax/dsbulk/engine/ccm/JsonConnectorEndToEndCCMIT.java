/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DDAC;
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.readAllLinesInDirectoryAsStream;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.ccm.CSVConnectorEndToEndCCMIT.assertComplexRows;
import static com.datastax.dsbulk.engine.ccm.CSVConnectorEndToEndCCMIT.checkNumbersWritten;
import static com.datastax.dsbulk.engine.ccm.CSVConnectorEndToEndCCMIT.checkTemporalsWritten;
import static com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy.REJECT;
import static com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy.TRUNCATE;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_NAMED;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createIpByCountryTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createWithSpacesTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateExceptionsLog;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateNumberOfBadRecords;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS_SKIP;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS_UNIQUE_PART_1;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS_UNIQUE_PART_2;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS_WITH_SPACES;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(numberOfNodes = 1)
@Tag("medium")
@CCMRequirements(compatibleTypes = {DSE, DDAC})
class JsonConnectorEndToEndCCMIT extends EndToEndCCMITBase {

  private static final Version V3 = Version.parse("3.0");
  private static final Version V2_1 = Version.parse("2.1");
  private static String URLFILE;

  private Path unloadDir;
  private Path logDir;

  JsonConnectorEndToEndCCMIT(CCMCluster ccm, Session session) {
    super(ccm, session);
  }

  @BeforeAll
  void createTables() {
    createIpByCountryTable(session);
    createWithSpacesTable(session);
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @AfterEach
  void truncateTable() {
    session.execute("TRUNCATE ip_by_country");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
    deleteDirectory(unloadDir);
  }

  @BeforeAll
  static void setup() throws IOException {
    URLFILE = createURLFile(JSON_RECORDS_UNIQUE_PART_1, JSON_RECORDS_UNIQUE_PART_2);
  }

  @AfterAll
  static void cleanup() throws IOException {
    Files.delete(Paths.get(URLFILE));
  }

  /** Simple test case which attempts to load and unload data using ccm. */
  @Test
  void full_load_unload() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("json");
    args.add("--connector.json.url");
    args.add(quoteJson(JSON_RECORDS_UNIQUE));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("json");
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  @Test
  void full_load_unload_using_urlfile() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("json");
    args.add("--connector.json.urlfile");
    args.add(quoteJson(URLFILE));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("json");
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /** DAT-307: Test to validate that missing primary keys will fail to load. */
  @Test
  void error_load_missing_primary_keys() throws Exception {

    session.execute("DROP TABLE IF EXISTS missing");
    session.execute(
        "CREATE TABLE IF NOT EXISTS missing (pk varchar, cc varchar, v varchar, PRIMARY KEY(pk, cc))");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(ClassLoader.getSystemResource("missing.json")));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("missing");
    args.add("--connector.json.mode");
    args.add("SINGLE_DOCUMENT");
    args.add("--schema.allowMissingFields");
    args.add("true");
    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(1);
    validateNumberOfBadRecords(4);
    validateExceptionsLog(
        1, "Primary key column pk cannot be mapped to null", "mapping-errors.log");
    validateExceptionsLog(
        1, "Primary key column cc cannot be mapped to null", "mapping-errors.log");
    validateExceptionsLog(1, "Primary key column pk cannot be left unmapped", "mapping-errors.log");
    validateExceptionsLog(1, "Primary key column cc cannot be left unmapped", "mapping-errors.log");
    validateResultSetSize(0, "SELECT * FROM missing");
  }

  /**
   * DAT-307: Test to validate that missing primary keys will fail to load with case-sensitive
   * identifiers.
   */
  @Test
  void error_load_missing_primary_keys_case_sensitive() throws Exception {

    session.execute("DROP TABLE IF EXISTS missing");
    session.execute(
        "CREATE TABLE missing (\"PK\" varchar, \"CC\" varchar, \"V\" varchar, "
            + "PRIMARY KEY(\"PK\", \"CC\"))");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(ClassLoader.getSystemResource("missing-case.json")));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("missing");
    args.add("--connector.json.mode");
    args.add("SINGLE_DOCUMENT");
    args.add("--schema.allowMissingFields");
    args.add("true");
    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(1);
    validateNumberOfBadRecords(4);
    validateExceptionsLog(
        1, "Primary key column \"PK\" cannot be mapped to null", "mapping-errors.log");
    validateExceptionsLog(
        1, "Primary key column \"CC\" cannot be mapped to null", "mapping-errors.log");
    validateExceptionsLog(
        1, "Primary key column \"PK\" cannot be left unmapped", "mapping-errors.log");
    validateExceptionsLog(
        1, "Primary key column \"CC\" cannot be left unmapped", "mapping-errors.log");
    validateResultSetSize(0, "SELECT * FROM missing");
  }

  /** Simple test case which attempts to load and unload data using ccm and compression (LZ4). */
  @Test
  void full_load_unload_lz4() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--driver.protocol.compression");
    args.add("LZ4");
    args.add("--connector.json.url");
    args.add(quoteJson(JSON_RECORDS_UNIQUE));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--driver.protocol.compression");
    args.add("LZ4");
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /** Simple test case which attempts to load and unload data using ccm and compression (Snappy). */
  @Test
  void full_load_unload_snappy() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
    args.add("--connector.json.url");
    args.add(quoteJson(JSON_RECORDS_UNIQUE));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /**
   * Attempts to load and unload complex types (Collections, UDTs, etc).
   *
   * @jira_ticket DAT-288
   */
  @Test
  void full_load_unload_complex() throws Exception {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V2_1) >= 0, "UDTs are not compatible with C* < 2.1");

    session.execute("DROP TABLE IF EXISTS complex");
    session.execute("DROP TYPE IF EXISTS contacts");

    session.execute(
        "CREATE TYPE contacts ("
            + "f_tuple frozen<tuple<int, text, float, timestamp>>, "
            + "f_list frozen<list<timestamp>>"
            + ")");
    session.execute(
        "CREATE TABLE complex ("
            + "pk int PRIMARY KEY, "
            + "c_text text, "
            + "c_int int, "
            + "c_tuple frozen<tuple<int, text, float, timestamp>>, "
            + "c_map map<timestamp, varchar>,"
            + "c_list list<timestamp>,"
            + "c_set set<varchar>,"
            + "c_udt frozen<contacts>)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(getClass().getResource("/complex.json")));
    args.add("--connector.json.mode");
    args.add("SINGLE_DOCUMENT");
    args.add("--codec.nullStrings");
    args.add("N/A");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("complex");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertComplexRows(session);

    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.mode");
    args.add("SINGLE_DOCUMENT");
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--codec.nullStrings");
    args.add("N/A");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("complex");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    // 2 documents + 2 lines for single document mode
    validateOutputFiles(4, unloadDir);

    args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.mode");
    args.add("SINGLE_DOCUMENT");
    args.add("--codec.nullStrings");
    args.add("N/A");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("complex");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertComplexRows(session);
  }

  /** Attempts to load and unload a larger dataset which can be batched. */
  @Test
  void full_load_unload_large_batches() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(JSON_RECORDS));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(500, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(500, unloadDir);
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
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("-url");
    args.add(quoteJson(JSON_RECORDS_WITH_SPACES));
    args.add("--schema.mapping");
    args.add(quoteJson("key=key,\"my source\"=\"my destination\""));
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(1, "SELECT * FROM \"MYKS\".\"WITH_SPACES\"");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("-url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.mapping");
    args.add(quoteJson("key=key,\"my source\"=\"my destination\""));
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(1, unloadDir);
  }

  /** Attempts to load and unload data, some of which will be unsuccessful. */
  @Test
  void skip_test_load_unload() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(JSON_RECORDS_SKIP));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);
    args.add("--connector.json.skipRecords");
    args.add("3");
    args.add("--connector.json.maxRecords");
    args.add("24");
    args.add("--schema.allowMissingFields");
    args.add("true");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_COMPLETED_WITH_ERRORS);
    validateResultSetSize(21, "SELECT * FROM ip_by_country");
    validateNumberOfBadRecords(3);
    validateExceptionsLog(3, "Source:", "mapping-errors.log");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_NAMED);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(21, unloadDir);
  }

  /** Test for DAT-224. */
  @Test
  void should_truncate_and_round() throws Exception {

    session.execute("DROP TABLE IF EXISTS numbers");
    session.execute(
        "CREATE TABLE IF NOT EXISTS numbers (key varchar PRIMARY KEY, vdouble double, vdecimal decimal)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(ClassLoader.getSystemResource("number.json").toExternalForm()));
    args.add("--connector.json.mode");
    args.add("SINGLE_DOCUMENT");
    args.add("--codec.overflowStrategy");
    args.add("TRUNCATE");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("numbers");
    args.add("--schema.mapping");
    args.add("*=*");

    int loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkNumbersWritten(TRUNCATE, UNNECESSARY, session);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.mode");
    args.add("MULTI_DOCUMENT");
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--codec.roundingStrategy");
    args.add("FLOOR");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("SELECT key, vdouble, vdecimal FROM numbers");

    int unloadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(unloadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkNumbersRead(TRUNCATE, unloadDir);
    deleteDirectory(logDir);

    // check we can load from the unloaded dataset
    args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--codec.overflowStrategy");
    args.add("TRUNCATE");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("numbers");
    args.add("--schema.mapping");
    args.add("*=*");

    loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    // no rounding possible in json
    checkNumbersWritten(TRUNCATE, UNNECESSARY, session);
  }

  /** Test for DAT-224. */
  @Test
  void should_not_truncate_nor_round() throws Exception {

    session.execute("DROP TABLE IF EXISTS numbers");
    session.execute(
        "CREATE TABLE IF NOT EXISTS numbers (key varchar PRIMARY KEY, vdouble double, vdecimal decimal)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(ClassLoader.getSystemResource("number.json").toExternalForm()));
    args.add("--connector.json.mode");
    args.add("SINGLE_DOCUMENT");
    args.add("--codec.overflowStrategy");
    args.add("REJECT");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("numbers");
    args.add("--schema.mapping");
    args.add("*=*");

    int loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_COMPLETED_WITH_ERRORS);
    validateExceptionsLog(
        1,
        "ArithmeticException: Cannot convert 0.12345678901234567890123456789 from BigDecimal to Double",
        "mapping-errors.log");
    checkNumbersWritten(REJECT, UNNECESSARY, session);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.mode");
    args.add("MULTI_DOCUMENT");
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--codec.roundingStrategy");
    args.add("UNNECESSARY");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("SELECT key, vdouble, vdecimal FROM numbers");

    int unloadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(unloadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkNumbersRead(REJECT, unloadDir);
    deleteDirectory(logDir);

    // check we can load from the unloaded dataset
    args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--codec.overflowStrategy");
    args.add("REJECT");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("numbers");
    args.add("--schema.mapping");
    args.add("*=*");

    loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkNumbersWritten(REJECT, UNNECESSARY, session);
  }

  /** Test for DAT-236. */
  @Test
  void temporal_roundtrip() throws IOException {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(V3) >= 0,
        "CQL type date is not compatible with C* < 3.0");

    session.execute("DROP TABLE IF EXISTS temporals");
    session.execute(
        "CREATE TABLE IF NOT EXISTS temporals (key int PRIMARY KEY, vdate date, vtime time, vtimestamp timestamp)");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(ClassLoader.getSystemResource("temporal.json").toExternalForm());
    args.add("--codec.locale");
    args.add("fr_FR");
    args.add("--codec.timeZone");
    args.add("Europe/Paris");
    args.add("--codec.date");
    args.add("cccc, d MMMM uuuu");
    args.add("--codec.time");
    args.add("HHmmssSSS");
    args.add("--codec.timestamp");
    args.add("ISO_ZONED_DATE_TIME");
    args.add("--codec.unit");
    args.add("SECONDS");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("temporals");
    args.add("--schema.mapping");
    args.add("*=*");

    int loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkTemporalsWritten(session);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--codec.locale");
    args.add("fr_FR");
    args.add("--codec.timeZone");
    args.add("Europe/Paris");
    args.add("--codec.date");
    args.add("cccc, d MMMM uuuu");
    args.add("--codec.time");
    args.add("HHmmssSSS");
    args.add("--codec.timestamp");
    args.add("ISO_ZONED_DATE_TIME");
    args.add("--codec.unit");
    args.add("SECONDS");
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("SELECT key, vdate, vtime, vtimestamp FROM temporals");

    int unloadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(unloadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkTemporalsRead(unloadDir);
    deleteDirectory(logDir);

    // check we can load from the unloaded dataset
    args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--codec.locale");
    args.add("fr_FR");
    args.add("--codec.timeZone");
    args.add("Europe/Paris");
    args.add("--codec.date");
    args.add("cccc, d MMMM uuuu");
    args.add("--codec.time");
    args.add("HHmmssSSS");
    args.add("--codec.timestamp");
    args.add("ISO_ZONED_DATE_TIME");
    args.add("--codec.unit");
    args.add("SECONDS");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("temporals");
    args.add("--schema.mapping");
    args.add("*=*");

    loadStatus = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(loadStatus).isEqualTo(DataStaxBulkLoader.STATUS_OK);
    checkTemporalsWritten(session);
  }

  /** Test for DAT-377. */
  @Test
  void load_numeric_fields() {

    session.execute("drop table if exists numeric_fields");
    session.execute("create table numeric_fields (pk int, cc int, v int, primary key (pk, cc))");

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("json");
    args.add("--connector.json.url");
    args.add(quoteJson(ClassLoader.getSystemResource("numeric-fields.json")));
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("numeric_fields");
    args.add("--schema.mapping");
    args.add("0=pk,1=cc,2=v");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(1, "SELECT * FROM numeric_fields");
  }

  /** Test for DAT-400. */
  @Test
  void full_unload_text_truncation() throws Exception {

    session.execute(
        "CREATE TABLE IF NOT EXISTS test_truncation ("
            + "id text PRIMARY KEY,"
            + "text_column text,"
            + "set_text_column set<text>,"
            + "list_text_column list<text>,"
            + "map_text_column map<text, text>)");

    session.execute(
        "insert into test_truncation (id, text_column) values ('test1', 'this is text')");
    session.execute(
        "insert into test_truncation (id, text_column) values ('test2', '1234 this text started with a number')");
    session.execute(
        "insert into test_truncation (id, text_column) values ('test3', 'this text ended with a number 1234')");
    session.execute(
        "insert into test_truncation (id, text_column) values ('test4', 'this text is 1234 with a number')");
    session.execute(
        "insert into test_truncation (id, text_column) values ('test5', '1234startswithanumbernospaces')");
    session.execute(
        "update test_truncation set set_text_column = set_text_column + {'1234 test text'} where id='test6'");
    session.execute(
        "update test_truncation set set_text_column = set_text_column + {'1234 test text'} where id='test7'");
    session.execute(
        "update test_truncation set set_text_column = set_text_column + {'1234 test text', 'this starts with text'} where id='test7'");
    session.execute(
        "update test_truncation set set_text_column = set_text_column + {'this starts with text'} where id='test8'");
    session.execute(
        "update test_truncation set set_text_column = set_text_column + {'1234thisisnospaces'} where id='test9'");
    session.execute(
        "update test_truncation set set_text_column = set_text_column + {'122 more text'} where id='test9'");
    session.execute(
        "update test_truncation set set_text_column = set_text_column + {'122 more text'} where id='test10'");
    session.execute(
        "update test_truncation set set_text_column = set_text_column + {'8595 more text'} where id='test10'");
    session.execute(
        "update test_truncation set map_text_column = {'1234 test text': '789 value text'} where id='test11'");
    session.execute(
        "update test_truncation set list_text_column = ['1234 test text', '789 value text'] where id='test12'");

    List<String> args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--connector.name");
    args.add("json");
    args.add("--connector.json.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("test_truncation");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();

    assertThat(readAllLinesInDirectoryAsStream(unloadDir))
        .containsExactlyInAnyOrder(
            "{\"id\":\"test1\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[],\"text_column\":\"this is text\"}",
            "{\"id\":\"test2\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[],\"text_column\":\"1234 this text started with a number\"}",
            "{\"id\":\"test3\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[],\"text_column\":\"this text ended with a number 1234\"}",
            "{\"id\":\"test4\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[],\"text_column\":\"this text is 1234 with a number\"}",
            "{\"id\":\"test5\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[],\"text_column\":\"1234startswithanumbernospaces\"}",
            "{\"id\":\"test6\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[\"1234 test text\"],\"text_column\":null}",
            "{\"id\":\"test7\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[\"1234 test text\",\"this starts with text\"],\"text_column\":null}",
            "{\"id\":\"test8\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[\"this starts with text\"],\"text_column\":null}",
            "{\"id\":\"test9\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[\"122 more text\",\"1234thisisnospaces\"],\"text_column\":null}",
            "{\"id\":\"test10\",\"list_text_column\":[],\"map_text_column\":{},\"set_text_column\":[\"122 more text\",\"8595 more text\"],\"text_column\":null}",
            "{\"id\":\"test11\",\"list_text_column\":[],\"map_text_column\":{\"1234 test text\":\"789 value text\"},\"set_text_column\":[],\"text_column\":null}",
            "{\"id\":\"test12\",\"list_text_column\":[\"1234 test text\",\"789 value text\"],\"map_text_column\":{},\"set_text_column\":[],\"text_column\":null}");
  }

  private static void checkNumbersRead(OverflowStrategy overflowStrategy, Path unloadDir)
      throws IOException {
    Map<String, String> doubles = new HashMap<>();
    Map<String, String> bigdecimals = new HashMap<>();
    List<String> lines = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList());
    Pattern pattern = Pattern.compile("\\{\"key\":\"(.+?)\",\"vdouble\":(.+?),\"vdecimal\":(.+?)}");
    for (String line : lines) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        doubles.put(matcher.group(1), matcher.group(2));
        bigdecimals.put(matcher.group(1), matcher.group(3));
      }
    }
    // no rounding possible in Json, the nodes are numeric
    checkDoubles(doubles, overflowStrategy);
    checkBigDecimals(bigdecimals, overflowStrategy);
  }

  @SuppressWarnings("FloatingPointLiteralPrecision")
  private static void checkDoubles(Map<String, String> numbers, OverflowStrategy overflowStrategy) {
    assertThat(numbers.get("scientific_notation")).isEqualTo("1.0E7");
    assertThat(Double.valueOf(numbers.get("scientific_notation"))).isEqualTo(10_000_000d);
    assertThat(numbers.get("regular_notation")).isEqualTo("1.0E7");
    assertThat(Double.valueOf(numbers.get("regular_notation"))).isEqualTo(10_000_000d);
    assertThat(numbers.get("hex_notation")).isEqualTo("1.7976931348623157E308");
    assertThat(Double.valueOf(numbers.get("hex_notation"))).isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("irrational")).isEqualTo("0.1");
    assertThat(numbers.get("Double.NaN")).isEqualTo("\"NaN\"");
    assertThat(numbers.get("Double.POSITIVE_INFINITY")).isEqualTo("\"Infinity\"");
    assertThat(numbers.get("Double.NEGATIVE_INFINITY")).isEqualTo("\"-Infinity\"");
    assertThat(numbers.get("Double.MAX_VALUE")).isEqualTo("1.7976931348623157E308");
    assertThat(Double.valueOf(numbers.get("Double.MAX_VALUE"))).isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("Double.MIN_VALUE")).isEqualTo("4.9E-324");
    assertThat(Double.valueOf(numbers.get("Double.MIN_VALUE"))).isEqualTo(Double.MIN_VALUE);
    assertThat(numbers.get("Double.MIN_NORMAL")).isEqualTo("2.2250738585072014E-308");
    assertThat(Double.valueOf(numbers.get("Double.MIN_NORMAL"))).isEqualTo(Double.MIN_NORMAL);
    assertThat(numbers.get("Float.MAX_VALUE")).isEqualTo("3.4028235E38");
    assertThat(Float.valueOf(numbers.get("Float.MAX_VALUE"))).isEqualTo(Float.MAX_VALUE);
    assertThat(numbers.get("Float.MIN_VALUE")).isEqualTo("1.4E-45");
    if (overflowStrategy == TRUNCATE) {
      assertThat(numbers.get("too_many_digits")).isEqualTo("0.12345678901234568");
    }
  }

  @SuppressWarnings("FloatingPointLiteralPrecision")
  private static void checkBigDecimals(
      Map<String, String> numbers, OverflowStrategy overflowStrategy) {
    assertThat(numbers.get("scientific_notation")).isEqualTo("1.0E+7");
    assertThat(Double.valueOf(numbers.get("scientific_notation"))).isEqualTo(10_000_000d);
    assertThat(numbers.get("regular_notation")).isEqualTo("10000000");
    assertThat(Double.valueOf(numbers.get("regular_notation"))).isEqualTo(10_000_000d);
    assertThat(numbers.get("hex_notation")).isEqualTo("1.7976931348623157E+308");
    assertThat(Double.valueOf(numbers.get("hex_notation"))).isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("irrational")).isEqualTo("0.1");
    assertThat(numbers.get("Double.MAX_VALUE")).isEqualTo("1.7976931348623157E+308");
    assertThat(Double.valueOf(numbers.get("Double.MAX_VALUE"))).isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("Double.MIN_VALUE")).isEqualTo("4.9E-324");
    assertThat(Double.valueOf(numbers.get("Double.MIN_VALUE"))).isEqualTo(Double.MIN_VALUE);
    assertThat(numbers.get("Double.MIN_NORMAL")).isEqualTo("2.2250738585072014E-308");
    assertThat(Double.valueOf(numbers.get("Double.MIN_NORMAL"))).isEqualTo(Double.MIN_NORMAL);
    assertThat(numbers.get("Float.MAX_VALUE")).isEqualTo("340282350000000000000000000000000000000");
    assertThat(Float.valueOf(numbers.get("Float.MAX_VALUE"))).isEqualTo(Float.MAX_VALUE);
    assertThat(numbers.get("Float.MIN_VALUE")).isEqualTo("1.4E-45");
    if (overflowStrategy == TRUNCATE) {
      assertThat(numbers.get("too_many_digits")).isEqualTo("0.12345678901234567890123456789");
    }
  }

  private static void checkTemporalsRead(Path unloadDir) throws IOException {
    String line = readAllLinesInDirectoryAsStream(unloadDir).collect(Collectors.toList()).get(0);
    Pattern pattern =
        Pattern.compile(
            "\\{\"key\":(.+?),\"vdate\":\"(.+?)\",\"vtime\":\"(.+?)\",\"vtimestamp\":\"(.+?)\"}");
    Matcher matcher = pattern.matcher(line);
    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(2)).isEqualTo("vendredi, 9 mars 2018");
    assertThat(matcher.group(3)).isEqualTo("171232584");
    assertThat(matcher.group(4)).isEqualTo("2018-03-09T17:12:32+01:00[Europe/Paris]");
  }
}
