/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.engine.ccm.CSVConnectorEndToEndCCMIT.checkNumbersWritten;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateBadOps;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateExceptionsLog;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.IP_BY_COUNTRY_COMPLEX_MAPPING;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.IP_BY_COUNTRY_MAPPING;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS_COMPLEX;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS_SKIP;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.JSON_RECORDS_WITH_SPACES;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.SELECT_FROM_IP_BY_COUNTRY_COMPLEX;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.createComplexTable;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.createIpByCountryTable;
import static com.datastax.dsbulk.engine.tests.utils.JsonUtils.createWithSpacesTable;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(numberOfNodes = 1)
@Tag("ccm")
class JsonConnectorEndToEndCCMIT extends EndToEndCCMITBase {

  private Path unloadDir;

  JsonConnectorEndToEndCCMIT(CCMCluster ccm, Session session) {
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
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(unloadDir);
  }

  /** Simple test case which attempts to load and unload data using ccm. */
  @Test
  void full_load_unload() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.name");
    args.add("json");
    args.add("--connector.json.url");
    args.add(JSON_RECORDS_UNIQUE.toExternalForm());
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
    args.add("--connector.name");
    args.add("json");
    args.add("--connector.json.url");
    args.add(unloadDir.toString());
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /** Simple test case which attempts to load and unload data using ccm and compression (LZ4). */
  @Test
  void full_load_unload_lz4() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("LZ4");
    args.add("--connector.json.url");
    args.add(JSON_RECORDS_UNIQUE.toExternalForm());
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
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("LZ4");
    args.add("--connector.json.url");
    args.add(unloadDir.toString());
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
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
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
    args.add("--connector.json.url");
    args.add(JSON_RECORDS_UNIQUE.toExternalForm());
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
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--driver.protocol.compression");
    args.add("SNAPPY");
    args.add("--connector.json.url");
    args.add(unloadDir.toString());
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  /** Attempts to load and unload complex types (Collections, UDTs, etc). */
  @Test
  void full_load_unload_complex() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.json.url");
    args.add(JSON_RECORDS_COMPLEX.toExternalForm());
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
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.json.url");
    args.add(unloadDir.toString());
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("country_complex");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_COMPLEX_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(5, unloadDir);
  }

  /** Attempts to load and unload a larger dataset which can be batched. */
  @Test
  void full_load_unload_large_batches() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.json.url");
    args.add(JSON_RECORDS.toExternalForm());
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
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.json.url");
    args.add(unloadDir.toString());
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
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
    args.add(Files.createTempDirectory("test").toString());
    args.add("-url");
    args.add(JSON_RECORDS_WITH_SPACES.toExternalForm());
    args.add("--schema.mapping");
    args.add("key=key,my source=my destination");
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(1, SELECT_FROM_IP_BY_COUNTRY_WITH_SPACES);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("-url");
    args.add(unloadDir.toString());
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.mapping");
    args.add("key=key,my source=my destination");
    args.add("-k");
    args.add("MYKS");
    args.add("-t");
    args.add("WITH_SPACES");

    status = new Main(addContactPointAndPort(args)).run();
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
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.json.url");
    args.add(JSON_RECORDS_SKIP.toExternalForm());
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);
    args.add("--connector.json.skipRecords");
    args.add("3");
    args.add("--connector.json.maxRecords");
    args.add("24");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(Main.STATUS_COMPLETED_WITH_ERRORS);
    validateResultSetSize(21, SELECT_FROM_IP_BY_COUNTRY);
    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateBadOps(3, logPath);
    validateExceptionsLog(3, "Source  :", "mapping-errors.log", logPath);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.name");
    args.add("json");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.json.url");
    args.add(unloadDir.toString());
    args.add("--connector.json.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING);

    status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(21, unloadDir);
  }

  /** Test for DAT-224. */
  @Test
  void should_truncate_and_round() throws Exception {

    session.execute(
        "CREATE TABLE IF NOT EXISTS dat224 (key varchar PRIMARY KEY, vdouble double, vdecimal decimal)");

    List<String> loadArgs = new ArrayList<>();
    loadArgs.add("load");
    loadArgs.add("--connector.name");
    loadArgs.add("json");
    loadArgs.add("--log.directory");
    loadArgs.add(Files.createTempDirectory("test").toString());
    loadArgs.add("--connector.json.url");
    loadArgs.add(ClassLoader.getSystemResource("number.json").toExternalForm());
    loadArgs.add("--connector.json.mode");
    loadArgs.add("SINGLE_DOCUMENT");
    loadArgs.add("--codec.overflowStrategy");
    loadArgs.add("TRUNCATE");
    loadArgs.add("--schema.keyspace");
    loadArgs.add(session.getLoggedKeyspace());
    loadArgs.add("--schema.table");
    loadArgs.add("dat224");
    loadArgs.add("--schema.mapping");
    loadArgs.add("*=*");

    int loadStatus = new Main(addContactPointAndPort(loadArgs)).run();
    assertThat(loadStatus).isEqualTo(Main.STATUS_OK);

    checkNumbersWritten(OverflowStrategy.TRUNCATE, session);

    List<String> unloadArgs = new ArrayList<>();
    unloadArgs.add("unload");
    unloadArgs.add("--connector.name");
    unloadArgs.add("json");
    unloadArgs.add("--log.directory");
    unloadArgs.add(Files.createTempDirectory("test").toString());
    unloadArgs.add("--connector.json.url");
    unloadArgs.add(unloadDir.toString());
    unloadArgs.add("--connector.json.mode");
    unloadArgs.add("MULTI_DOCUMENT");
    unloadArgs.add("--codec.roundingStrategy");
    unloadArgs.add("FLOOR");
    unloadArgs.add("--connector.csv.maxConcurrentFiles");
    unloadArgs.add("1");
    unloadArgs.add("--schema.keyspace");
    unloadArgs.add(session.getLoggedKeyspace());
    unloadArgs.add("--schema.query");
    unloadArgs.add("SELECT key, vdouble, vdecimal FROM dat224");

    int unloadStatus = new Main(addContactPointAndPort(unloadArgs)).run();
    assertThat(unloadStatus).isEqualTo(Main.STATUS_OK);

    checkNumbersRead(OverflowStrategy.TRUNCATE, unloadDir);
  }

  /** Test for DAT-224. */
  @Test
  void should_not_truncate_nor_round() throws Exception {

    session.execute(
        "CREATE TABLE IF NOT EXISTS dat224 (key varchar PRIMARY KEY, vdouble double, vdecimal decimal)");

    List<String> loadArgs = new ArrayList<>();
    loadArgs.add("load");
    loadArgs.add("--connector.name");
    loadArgs.add("json");
    loadArgs.add("--log.directory");
    loadArgs.add(Files.createTempDirectory("test").toString());
    loadArgs.add("--connector.name");
    loadArgs.add("json");
    loadArgs.add("--connector.json.url");
    loadArgs.add(ClassLoader.getSystemResource("number.json").toExternalForm());
    loadArgs.add("--connector.json.mode");
    loadArgs.add("SINGLE_DOCUMENT");
    loadArgs.add("--codec.overflowStrategy");
    loadArgs.add("REJECT");
    loadArgs.add("--schema.keyspace");
    loadArgs.add(session.getLoggedKeyspace());
    loadArgs.add("--schema.table");
    loadArgs.add("dat224");
    loadArgs.add("--schema.mapping");
    loadArgs.add("*=*");

    int loadStatus = new Main(addContactPointAndPort(loadArgs)).run();
    assertThat(loadStatus).isEqualTo(Main.STATUS_COMPLETED_WITH_ERRORS);

    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
    validateExceptionsLog(1, "overflow", "mapping-errors.log", logPath);
    checkNumbersWritten(OverflowStrategy.REJECT, session);

    List<String> unloadArgs = new ArrayList<>();
    unloadArgs.add("unload");
    unloadArgs.add("--connector.name");
    unloadArgs.add("json");
    unloadArgs.add("--log.directory");
    unloadArgs.add(Files.createTempDirectory("test").toString());
    unloadArgs.add("--connector.json.url");
    unloadArgs.add(unloadDir.toString());
    unloadArgs.add("--connector.json.mode");
    unloadArgs.add("MULTI_DOCUMENT");
    unloadArgs.add("--connector.csv.maxConcurrentFiles");
    unloadArgs.add("1");
    unloadArgs.add("--codec.roundingStrategy");
    unloadArgs.add("UNNECESSARY");
    unloadArgs.add("--schema.keyspace");
    unloadArgs.add(session.getLoggedKeyspace());
    unloadArgs.add("--schema.query");
    unloadArgs.add("SELECT key, vdouble, vdecimal FROM dat224");

    int unloadStatus = new Main(addContactPointAndPort(unloadArgs)).run();
    assertThat(unloadStatus).isEqualTo(Main.STATUS_OK);

    checkNumbersRead(OverflowStrategy.REJECT, unloadDir);
  }

  private static void checkNumbersRead(OverflowStrategy overflowStrategy, Path unloadDir)
      throws IOException {
    Map<String, String> doubles = new HashMap<>();
    Map<String, String> bigdecimals = new HashMap<>();
    List<String> lines =
        FileUtils.readAllLinesInDirectoryAsStream(unloadDir, UTF_8).collect(Collectors.toList());
    Pattern pattern = Pattern.compile("\\{\"key\":\"(.+?)\",\"vdouble\":(.+?),\"vdecimal\":(.+?)}");
    for (String line : lines) {
      Matcher matcher = pattern.matcher(line);
      if (matcher.find()) {
        doubles.put(matcher.group(1), matcher.group(2));
        bigdecimals.put(matcher.group(1), matcher.group(3));
      }
    }
    // no rounding possible in Json, the nodes are numeric
    checkNumbers(doubles, overflowStrategy, true);
    checkNumbers(bigdecimals, overflowStrategy, false);
  }

  @SuppressWarnings("FloatingPointLiteralPrecision")
  static void checkNumbers(
      Map<String, String> numbers, OverflowStrategy overflowStrategy, boolean isDouble) {
    // when USE_BIG_DECIMAL_FOR_FLOATS is true Jackson tends to favor scientific notation for
    // decimal nodes, but apart from that the values are correct after all
    assertThat(numbers.get("scientific_notation")).isIn("1.0E7", "1E+7");
    assertThat(Double.valueOf(numbers.get("scientific_notation"))).isEqualTo(10_000_000d);
    assertThat(numbers.get("regular_notation")).isIn("1.0E7", "1E+7");
    assertThat(Double.valueOf(numbers.get("regular_notation"))).isEqualTo(10_000_000d);
    assertThat(numbers.get("hex_notation"))
        .isIn("1.7976931348623157E308", "1.7976931348623157E+308");
    assertThat(Double.valueOf(numbers.get("hex_notation"))).isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("irrational")).isEqualTo("0.1");
    if (isDouble) {
      assertThat(numbers.get("Double.NaN")).isEqualTo("\"NaN\"");
      assertThat(numbers.get("Double.POSITIVE_INFINITY")).isEqualTo("\"Infinity\"");
      assertThat(numbers.get("Double.NEGATIVE_INFINITY")).isEqualTo("\"-Infinity\"");
    }
    assertThat(numbers.get("Double.MAX_VALUE"))
        .isIn("1.7976931348623157E308", "1.7976931348623157E+308");
    assertThat(Double.valueOf(numbers.get("Double.MAX_VALUE"))).isEqualTo(Double.MAX_VALUE);
    assertThat(numbers.get("Double.MIN_VALUE")).isEqualTo("4.9E-324");
    assertThat(Double.valueOf(numbers.get("Double.MIN_VALUE"))).isEqualTo(Double.MIN_VALUE);
    assertThat(numbers.get("Double.MIN_NORMAL")).isEqualTo("2.2250738585072014E-308");
    assertThat(Double.valueOf(numbers.get("Double.MIN_NORMAL"))).isEqualTo(Double.MIN_NORMAL);
    assertThat(numbers.get("Float.MAX_VALUE")).isIn("3.4028235E38", "3.4028235E+38");
    assertThat(Float.valueOf(numbers.get("Float.MAX_VALUE"))).isEqualTo(Float.MAX_VALUE);
    assertThat(numbers.get("Float.MIN_VALUE")).isEqualTo("1.4E-45");
    if (overflowStrategy == OverflowStrategy.TRUNCATE) {
      if (isDouble) {
        assertThat(numbers.get("too_many_digits")).isEqualTo("0.12345678901234568");
      } else {
        assertThat(numbers.get("too_many_digits")).isEqualTo("0.12345678901234567890123456789");
      }
    }
  }
}
