/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDOUT;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.slf4j.event.Level.ERROR;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.engine.internal.utils.HelpUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.commons.cli.ParseException;
import org.assertj.core.util.Lists;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.AnsiString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(StreamInterceptingExtension.class)
@ExtendWith(LogInterceptingExtension.class)
class DataStaxBulkLoaderTest {

  private final StreamInterceptor stdOut;
  private final StreamInterceptor stdErr;
  private final LogInterceptor logs;
  private Path tempFolder;

  DataStaxBulkLoaderTest(
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr,
      @LogCapture(level = ERROR) LogInterceptor logs) {
    this.stdOut = stdOut;
    this.stdErr = stdErr;
    this.logs = logs;
  }

  @AfterEach
  void resetAnsi() {
    AnsiConsole.systemUninstall();
  }

  @BeforeEach
  void loadDefaultConfig() {
    DataStaxBulkLoader.DEFAULT = ConfigFactory.load().getConfig("dsbulk");
  }

  @BeforeEach
  void createTempFolder() throws IOException {
    tempFolder = Files.createTempDirectory("test");
  }

  @AfterEach
  void deleteTempFolder() {
    deleteDirectory(tempFolder);
    deleteDirectory(Paths.get("./logs"));
  }

  @AfterEach
  void clearConfigFileProperty() {
    System.clearProperty("config.file");
  }

  @Test
  void should_show_global_help_when_no_args() {
    // global help, no shortcuts, has both json and csv settings.
    new DataStaxBulkLoader(new String[] {}).run();
    assertGlobalHelp(false);
  }

  @Test
  void should_show_global_help_when_help_opt_arg() {
    // global help, no shortcuts, has both json and csv settings.
    new DataStaxBulkLoader(new String[] {"--help"}).run();
    assertGlobalHelp(false);
  }

  @Test
  void should_show_global_help_when_help_subcommand() {
    // global help, no shortcuts, has both json and csv settings.
    new DataStaxBulkLoader(new String[] {"help"}).run();
    assertGlobalHelp(false);
  }

  @Test
  void should_show_section_help_when_help_opt_arg() {
    new DataStaxBulkLoader(new String[] {"--help", "driver.auth"}).run();
    assertSectionHelp();
  }

  @Test
  void should_show_section_help_when_help_subcommand() {
    new DataStaxBulkLoader(new String[] {"help", "driver.auth"}).run();
    assertSectionHelp();
  }

  @Test
  void should_show_global_help_filtered_when_help_opt_arg() {
    // global help, with shortcuts, has only json common settings.
    new DataStaxBulkLoader(new String[] {"--help", "-c", "json"}).run();
    assertGlobalHelp(true);
  }

  @Test
  void should_show_global_help_filtered_when_help_subcommand() {
    // global help, with shortcuts, has only json common settings.
    new DataStaxBulkLoader(new String[] {"help", "-c", "json"}).run();
    assertGlobalHelp(true);
  }

  @Test
  void should_show_section_help_when_help_opt_arg_with_connector() {
    new DataStaxBulkLoader(new String[] {"--help", "-c", "json", "driver.auth"}).run();
    assertSectionHelp();
  }

  @Test
  void should_show_section_help_when_help_subcommand_with_connector() {
    new DataStaxBulkLoader(new String[] {"help", "-c", "json", "driver.auth"}).run();
    assertSectionHelp();
  }

  @Test
  void should_show_error_when_junk_subcommand() {
    new DataStaxBulkLoader(new String[] {"junk"}).run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("First argument must be subcommand")
        .contains(", or \"help\"");
  }

  @Test
  void should_show_help_without_error_when_junk_subcommand_and_help() {
    new DataStaxBulkLoader(new String[] {"junk", "--help"}).run();
    assertThat(stdErr.getStreamAsString()).doesNotContain("First argument must be subcommand");
    assertGlobalHelp(false);
  }

  @Test
  void should_show_help_without_error_when_good_subcommand_and_help() {
    new DataStaxBulkLoader(new String[] {"load", "--help"}).run();
    assertThat(stdErr.getStreamAsString()).doesNotContain("First argument must be subcommand");
    assertGlobalHelp(false);
  }

  @Test
  void should_show_error_for_help_bad_section() {
    new DataStaxBulkLoader(new String[] {"help", "noexist"}).run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("noexist is not a valid section. Available sections include")
        .contains("driver.auth");
  }

  @Test
  void should_show_section_help() {
    new DataStaxBulkLoader(new String[] {"help", "batch"}).run();
    String out = stdOut.getStreamAsString();
    assertThat(out)
        .contains("--batch.mode")
        .doesNotContain("This section has the following subsections");
  }

  @Test
  void should_show_section_help_with_subsection_pointers() {
    new DataStaxBulkLoader(new String[] {"help", "driver"}).run();
    String out = stdOut.getStreamAsString();
    assertThat(out)
        .contains("--driver.hosts")
        .contains("This section has the following subsections")
        .contains("driver.auth");
  }

  @Test
  void should_show_section_help_with_connector_shortcuts() {
    new DataStaxBulkLoader(new String[] {"help", "connector.csv"}).run();
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out).contains("-url, --connector.csv.url");
  }

  @Test
  void should_respect_custom_config_file() throws Exception {
    {
      Path f = Files.createTempFile(tempFolder, "myapp", ".conf");
      Files.write(f, "dsbulk.connector.name=junk".getBytes(UTF_8));
      new DataStaxBulkLoader(new String[] {"load", "-f", f.toString()}).run();
      String err = logs.getAllMessagesAsString();
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .contains("Cannot find connector 'junk'");
    }
    logs.clear();
    {
      Path f = Files.createTempFile(tempFolder, "myapp", ".conf");
      Files.write(
          f,
          ("dsbulk.connector.csv.url=/path/to/my/file\n"
                  + "dsbulk.schema.query=INSERT\n"
                  + "dsbulk.driver.socket.readTimeout=wonky")
              .getBytes(UTF_8));
      new DataStaxBulkLoader(new String[] {"load", "-f", f.toString()}).run();
      String err = logs.getAllMessagesAsString();
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .contains("Invalid value at 'socket.readTimeout'");
    }
    // DAT-221: -f should expand user home
    logs.clear();
    {
      Path f = Files.createTempFile(Paths.get(System.getProperty("user.home")), "myapp", ".conf");
      f.toFile().deleteOnExit();
      Files.write(f, "dsbulk.connector.name=foo".getBytes(UTF_8));
      new DataStaxBulkLoader(new String[] {"load", "-f", "~/" + f.getFileName().toString()}).run();
      String err = logs.getAllMessagesAsString();
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .doesNotContain("InvalidPathException")
          .contains("Cannot find connector 'foo'");
    }
  }

  @Test
  void should_error_out_for_bad_config_file() {
    new DataStaxBulkLoader(new String[] {"load", "-f", "noexist"}).run();
    String err = logs.getAllMessagesAsString();
    if (PlatformUtils.isWindows()) {

      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .contains("noexist (The system cannot find the file specified)");
    } else {
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .contains("noexist (No such file or directory)");
    }
  }

  @Test
  void should_accept_connector_name_in_args_over_config_file() throws Exception {
    Path f = Files.createTempFile(tempFolder, "myapp", ".conf");
    Files.write(f, "dsbulk.connector.name=junk".getBytes(UTF_8));
    new DataStaxBulkLoader(new String[] {"load", "-c", "fromargs", "-f", f.toString()}).run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  void should_error_on_populated_target_url_csv() throws Exception {
    Path unloadDir = null;
    try {
      unloadDir = createTempDirectory("test");
      Files.createFile(unloadDir.resolve("output-000001.csv"));
      new DataStaxBulkLoader(new String[] {"unload", "--connector.csv.url", quoteJson(unloadDir)})
          .run();
      String err = logs.getAllMessagesAsString();
      assertThat(err).contains("connector.csv.url: target directory").contains("must be empty");
    } finally {
      if (unloadDir != null) {
        deleteDirectory(unloadDir);
      }
    }
  }

  @Test
  void should_error_on_populated_target_url_json() throws Exception {
    Path unloadDir = null;
    try {
      unloadDir = createTempDirectory("test");
      Files.createFile(unloadDir.resolve("output-000001.json"));
      new DataStaxBulkLoader(
              new String[] {"unload", "-c", "json", "--connector.json.url", quoteJson(unloadDir)})
          .run();
      String err = logs.getAllMessagesAsString();
      assertThat(err).contains("connector.json.url: target directory").contains("must be empty");
    } finally {
      if (unloadDir != null) {
        deleteDirectory(unloadDir);
      }
    }
  }

  @Test
  void should_handle_connector_name_long_option() {
    new DataStaxBulkLoader(new String[] {"load", "--connector.name", "fromargs"}).run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  void should_handle_connector_name_long_option_with_equal() {
    new DataStaxBulkLoader(new String[] {"load", "--connector.name=fromargs"}).run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  void should_error_out_for_bad_execution_id_template() {
    new DataStaxBulkLoader(new String[] {"load", "--engine.executionId", "%4$s"}).run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("Operation failed")
        .contains("Could not generate execution ID with template: '%4$s'");
  }

  @Test
  void should_accept_escaped_control_char() throws Exception {
    // control chars should be provided escaped as valid HOCON
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {"--connector.csv.delimiter", "\\t"});
    assertThat(result.getString("connector.csv.delimiter")).isEqualTo("\t");
  }

  @Test
  void should_accept_escaped_backslash() throws Exception {
    // backslashes should be provided escaped as valid HOCON
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {"--connector.csv.url", "C:\\\\Users"});
    assertThat(result.getString("connector.csv.url")).isEqualTo("C:\\Users");
  }

  @Test
  void should_accept_escaped_double_quote() throws Exception {
    // double quotes should be provided escaped as valid HOCON
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {"--connector.csv.escape", "\\\""});
    assertThat(result.getString("connector.csv.escape")).isEqualTo("\"");
  }

  @Test
  void should_accept_escaped_double_quote_in_complex_type() throws Exception {
    // double quotes should be provided escaped as valid HOCON
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {"--codec.booleanStrings", "[\"foo\\\"bar\"]"});
    assertThat(result.getStringList("codec.booleanStrings")).containsExactly("foo\"bar");
  }

  @Test
  void should_not_add_quote_if_already_quoted() throws Exception {
    // double quotes should be provided escaped as valid HOCON
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {"--connector.csv.delimiter", "\"\\t\""});
    assertThat(result.getString("connector.csv.delimiter")).isEqualTo("\t");
  }

  @Test
  void should_not_accept_parse_error() {
    new DataStaxBulkLoader(new String[] {"load", "--codec.booleanStrings", "[a,b"}).run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("codec.booleanStrings: Expecting LIST, got '[a,b'")
        .contains("List should have ended with ] or had a comma");
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_short_options() {
    return Stream.of(
        Arguments.of("-locale", "locale", "codec.locale", "locale"),
        Arguments.of("-timeZone", "tz", "codec.timeZone", "tz"),
        Arguments.of("-c", "csv", "connector.name", "csv"),
        Arguments.of("-p", "pass", "driver.auth.password", "pass"),
        Arguments.of("-u", "user", "driver.auth.username", "user"),
        Arguments.of("-h", "host1, host2", "driver.hosts", Lists.newArrayList("host1", "host2")),
        Arguments.of("-maxRetries", "42", "driver.policy.maxRetries", 42),
        Arguments.of("-port", "9876", "driver.port", 9876),
        Arguments.of("-cl", "cl", "driver.query.consistency", "cl"),
        Arguments.of(
            "-b", "secureConnectBundle", "driver.cloud.secureConnectBundle", "secureConnectBundle"),
        Arguments.of("-maxErrors", "123", "log.maxErrors", 123),
        Arguments.of("-logDir", "logdir", "log.directory", "logdir"),
        Arguments.of("-jmx", "false", "monitoring.jmx", false),
        Arguments.of("-reportRate", "10 seconds", "monitoring.reportRate", "10 seconds"),
        Arguments.of("-k", "ks", "schema.keyspace", "ks"),
        Arguments.of(
            "-m",
            "{0:\"f1\", 1:\"f2\"}",
            "schema.mapping",
            "{0:f1, 1:f2}"), // type is forced to string
        Arguments.of(
            "-nullStrings", "[nil, nada]", "codec.nullStrings", Lists.newArrayList("nil", "nada")),
        Arguments.of("-query", "INSERT INTO foo", "schema.query", "INSERT INTO foo"),
        Arguments.of("-t", "table", "schema.table", "table"),
        Arguments.of("-dryRun", "true", "engine.dryRun", true));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_short_options(
      String shortOptionName, String shortOptionValue, String setting, Object expected)
      throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {shortOptionName, shortOptionValue});
    assertThat(result.getAnyRef(setting)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_csv_short_options() {
    return Stream.of(
        Arguments.of("-comment", "comment", "connector.csv.comment", "comment"),
        Arguments.of("-delim", "|", "connector.csv.delimiter", "|"),
        Arguments.of("-encoding", "enc", "connector.csv.encoding", "enc"),
        Arguments.of("-header", "header", "connector.csv.header", "header"),
        Arguments.of("-escape", "^", "connector.csv.escape", "^"),
        Arguments.of("-skipRecords", "3", "connector.csv.skipRecords", 3),
        Arguments.of("-maxRecords", "111", "connector.csv.maxRecords", 111),
        Arguments.of("-maxConcurrentFiles", "2C", "connector.csv.maxConcurrentFiles", "2C"),
        Arguments.of("-quote", "'", "connector.csv.quote", "'"),
        Arguments.of("-url", "http://findit", "connector.csv.url", "http://findit"));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_csv_short_options(
      String shortOptionName, String shortOptionValue, String setting, Object expected)
      throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {shortOptionName, shortOptionValue});
    assertThat(result.getAnyRef(setting)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_json_short_options() {
    return Stream.of(
        Arguments.of("-encoding", "enc", "connector.json.encoding", "enc"),
        Arguments.of("-skipRecords", "3", "connector.json.skipRecords", 3),
        Arguments.of("-maxRecords", "111", "connector.json.maxRecords", 111),
        Arguments.of("-maxConcurrentFiles", "2C", "connector.json.maxConcurrentFiles", "2C"),
        Arguments.of("-url", "http://findit", "connector.json.url", "http://findit"));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_json_short_options(
      String shortOptionName, String shortOptionValue, String setting, Object expected)
      throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {"-c", "json", shortOptionName, shortOptionValue});
    assertThat(result.getAnyRef(setting)).isEqualTo(expected);
  }

  @Test
  void should_reject_concatenated_option_value() {
    assertThrows(
        ParseException.class,
        () ->
            DataStaxBulkLoader.parseCommandLine(
                "load",
                new String[] {
                  "-kks",
                }),
        "Unrecognized option: -kks");
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_long_options() {
    return Stream.of(
        Arguments.of("driver.hosts", "host1, host2", Lists.newArrayList("host1", "host2")),
        Arguments.of("driver.port", "1", 1),
        Arguments.of("driver.protocol.compression", "NONE", "NONE"),
        Arguments.of("driver.pooling.local.connections", "2", 2),
        Arguments.of("driver.pooling.remote.connections", "3", 3),
        Arguments.of("driver.pooling.requests", "4", 4),
        Arguments.of("driver.pooling.heartbeat", "6 seconds", "6 seconds"),
        Arguments.of("driver.query.consistency", "cl", "cl"),
        Arguments.of("driver.query.serialConsistency", "serial-cl", "serial-cl"),
        Arguments.of("driver.query.fetchSize", "7", 7),
        Arguments.of("driver.query.idempotence", "false", false),
        Arguments.of("driver.socket.readTimeout", "8 seconds", "8 seconds"),
        Arguments.of("driver.auth.provider", "myauth", "myauth"),
        Arguments.of("driver.auth.username", "user", "user"),
        Arguments.of("driver.auth.password", "pass", "pass"),
        Arguments.of("driver.auth.authorizationId", "authid", "authid"),
        Arguments.of("driver.auth.principal", "user@foo.com", "user@foo.com"),
        Arguments.of("driver.auth.keyTab", "mykeytab", "mykeytab"),
        Arguments.of("driver.auth.saslService", "sasl", "sasl"),
        Arguments.of("driver.ssl.provider", "myssl", "myssl"),
        Arguments.of("driver.ssl.cipherSuites", "[TLS]", Lists.newArrayList("TLS")),
        Arguments.of("driver.ssl.truststore.path", "trust-path", "trust-path"),
        Arguments.of("driver.ssl.truststore.password", "trust-pass", "trust-pass"),
        Arguments.of("driver.ssl.truststore.algorithm", "trust-alg", "trust-alg"),
        Arguments.of("driver.ssl.keystore.path", "keystore-path", "keystore-path"),
        Arguments.of("driver.ssl.keystore.password", "keystore-pass", "keystore-pass"),
        Arguments.of("driver.ssl.keystore.algorithm", "keystore-alg", "keystore-alg"),
        Arguments.of("driver.ssl.openssl.keyCertChain", "key-cert-chain", "key-cert-chain"),
        Arguments.of("driver.ssl.openssl.privateKey", "key", "key"),
        Arguments.of("driver.timestampGenerator", "ts-gen", "ts-gen"),
        Arguments.of("driver.addressTranslator", "address-translator", "address-translator"),
        Arguments.of("driver.policy.lbp.localDc", "localDc", "localDc"),
        Arguments.of(
            "driver.policy.lbp.allowedHosts", "wh1, wh2", Lists.newArrayList("wh1", "wh2")),
        Arguments.of("driver.policy.maxRetries", "29", 29),
        Arguments.of(
            "driver.cloud.secureConnectBundle", "secureConnectBundle", "secureConnectBundle"),
        Arguments.of("engine.dryRun", "true", true),
        Arguments.of("engine.executionId", "MY_EXEC_ID", "MY_EXEC_ID"),
        Arguments.of("batch.mode", "batch-mode", "batch-mode"),
        Arguments.of("batch.bufferSize", "9", 9),
        Arguments.of("batch.maxBatchSize", "10", 10),
        Arguments.of("executor.maxInFlight", "12", 12),
        Arguments.of("executor.maxPerSecond", "13", 13),
        Arguments.of("executor.continuousPaging.pageUnit", "BYTES", "BYTES"),
        Arguments.of("executor.continuousPaging.pageSize", "14", 14),
        Arguments.of("executor.continuousPaging.maxPages", "15", 15),
        Arguments.of("executor.continuousPaging.maxPagesPerSecond", "16", 16),
        Arguments.of("log.directory", "log-out", "log-out"),
        Arguments.of("log.maxErrors", "18", 18),
        Arguments.of("log.stmt.level", "NORMAL", "NORMAL"),
        Arguments.of("log.stmt.maxQueryStringLength", "19", 19),
        Arguments.of("log.stmt.maxBoundValues", "20", 20),
        Arguments.of("log.stmt.maxBoundValueLength", "21", 21),
        Arguments.of("log.stmt.maxInnerStatements", "22", 22),
        Arguments.of("codec.locale", "locale", "locale"),
        Arguments.of("codec.timeZone", "tz", "tz"),
        Arguments.of("codec.booleanStrings", "[\"Si\", \"No\"]", Lists.newArrayList("Si", "No")),
        Arguments.of("codec.number", "codec-number", "codec-number"),
        Arguments.of("codec.timestamp", "codec-ts", "codec-ts"),
        Arguments.of("codec.date", "codec-date", "codec-date"),
        Arguments.of("codec.time", "codec-time", "codec-time"),
        Arguments.of("monitoring.reportRate", "23 sec", "23 sec"),
        Arguments.of("monitoring.rateUnit", "rate-unit", "rate-unit"),
        Arguments.of("monitoring.durationUnit", "duration-unit", "duration-unit"),
        Arguments.of("monitoring.expectedWrites", "24", 24),
        Arguments.of("monitoring.expectedReads", "25", 25),
        Arguments.of("monitoring.jmx", "false", false),
        Arguments.of("schema.keyspace", "ks", "ks"),
        Arguments.of("schema.table", "table", "table"),
        Arguments.of("schema.query", "SELECT JUNK", "SELECT JUNK"),
        Arguments.of("schema.queryTimestamp", "2018-05-18T15:00:00Z", "2018-05-18T15:00:00Z"),
        Arguments.of("schema.queryTtl", "28", 28),
        Arguments.of("codec.nullStrings", "[NIL, NADA]", Lists.newArrayList("NIL", "NADA")),
        Arguments.of("schema.nullToUnset", "false", false),
        Arguments.of(
            "schema.mapping", "{0:\"f1\", 1:\"f2\"}", "{0:f1, 1:f2}"), // type is forced to string
        Arguments.of("connector.name", "conn", "conn"));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_long_options(String settingName, String settingValue, Object expected)
      throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {"--" + settingName, settingValue});
    assertThat(result.getAnyRef(settingName)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_csv_long_options() {
    return Stream.of(
        Arguments.of("connector.csv.url", "url", "url"),
        Arguments.of("connector.csv.fileNamePattern", "pat", "pat"),
        Arguments.of("connector.csv.fileNameFormat", "fmt", "fmt"),
        Arguments.of("connector.csv.recursive", "true", true),
        Arguments.of("connector.csv.maxConcurrentFiles", "2C", "2C"),
        Arguments.of("connector.csv.encoding", "enc", "enc"),
        Arguments.of("connector.csv.header", "false", false),
        Arguments.of("connector.csv.delimiter", "|", "|"),
        Arguments.of("connector.csv.quote", "'", "'"),
        Arguments.of("connector.csv.escape", "*", "*"),
        Arguments.of("connector.csv.comment", "#", "#"),
        Arguments.of("connector.csv.skipRecords", "2", 2),
        Arguments.of("connector.csv.maxRecords", "3", 3));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_csv_long_options(String settingName, String settingValue, Object expected)
      throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {"--" + settingName, settingValue});
    assertThat(result.getAnyRef(settingName)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_json_long_options() {
    return Stream.of(
        Arguments.of("connector.json.url", "url", "url"),
        Arguments.of("connector.json.fileNamePattern", "pat", "pat"),
        Arguments.of("connector.json.fileNameFormat", "fmt", "fmt"),
        Arguments.of("connector.json.recursive", "true", true),
        Arguments.of("connector.json.maxConcurrentFiles", "2C", "2C"),
        Arguments.of("connector.json.encoding", "enc", "enc"),
        Arguments.of("connector.json.skipRecords", "2", 2),
        Arguments.of("connector.json.maxRecords", "3", 3),
        Arguments.of("connector.json.mode", "SINGLE_DOCUMENT", "SINGLE_DOCUMENT"),
        Arguments.of(
            "connector.json.parserFeatures",
            "{f1 = true, f2 = false}",
            ImmutableMap.of("f1", true, "f2", false)),
        Arguments.of(
            "connector.json.generatorFeatures",
            "{g1 = true, g2 = false}",
            ImmutableMap.of("g1", true, "g2", false)),
        Arguments.of(
            "connector.json.serializationFeatures",
            "{s1 = true, s2 = false}",
            ImmutableMap.of("s1", true, "s2", false)),
        Arguments.of(
            "connector.json.deserializationFeatures",
            "{d1 = true, d2 = false}",
            ImmutableMap.of("d1", true, "d2", false)),
        Arguments.of("connector.json.prettyPrint", "true", true));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_json_long_options(String settingName, String settingValue, Object expected)
      throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load", new String[] {"--" + settingName, settingValue});
    assertThat(result.getAnyRef(settingName)).isEqualTo(expected);
  }

  @Test
  void should_show_version_message_when_asked() {
    new DataStaxBulkLoader(new String[] {"--version"}).run();
    String out = stdOut.getStreamAsString();
    assertThat(out).isEqualTo(String.format("%s%n", HelpUtils.getVersionMessage()));
  }

  @Test
  void should_show_error_when_unload_and_dryRun() {
    new DataStaxBulkLoader(
            new String[] {"unload", "-dryRun", "true", "-url", "/foo/bar", "-k", "k1", "-t", "t1"})
        .run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("Dry-run is not supported for unload");
  }

  @Test
  void should_error_on_backslash() throws URISyntaxException {
    Path badJson = Paths.get(ClassLoader.getSystemResource("bad-json.conf").toURI());
    new DataStaxBulkLoader(
            new String[] {
              "load",
              "-dryRun",
              "true",
              "-url",
              "/foo/bar",
              "-k",
              "k1",
              "-t",
              "t1",
              "-f",
              badJson.toString()
            })
        .run();
    assertThat(stdErr.getStreamAsString())
        .contains(
            String.format(
                "Error parsing configuration file %s at line 1. "
                    + "Please make sure its format is compliant with HOCON syntax. "
                    + "If you are using \\ (backslash) to define a path, "
                    + "escape it with \\\\ or use / (forward slash) instead.",
                badJson));
  }

  private void assertGlobalHelp(boolean jsonOnly) {
    String out =
        new AnsiString(stdOut.getStreamAsString()).getPlain().toString().replaceAll("[\\s]+", " ");

    assertThat(out).contains(HelpUtils.getVersionMessage());
    assertThat(out).contains("-v, --version Show program's version number and exit");

    for (WorkflowType workflowType : WorkflowType.values()) {
      assertThat(out).contains(workflowType.getTitle());
      assertThat(out).contains(workflowType.getDescription());
    }

    // The following assures us that we're looking at global help, not section help.
    assertThat(out).contains("GETTING MORE HELP");

    // The tests try restricting global help to json connector, or show all connectors.
    // If all, shortcut options for connector settings should not be shown.
    // If restricted to json, show the shorcut options for common json settings.
    assertThat(out).contains("--connector.json.url");
    if (jsonOnly) {
      assertThat(out).contains("-url, --connector.json.url");
      assertThat(out).doesNotContain("--connector.csv.url");
    } else {
      assertThat(out).contains("--connector.csv.url");
      assertThat(out).doesNotContain("-url, --connector.csv.url");
    }
    assertThat(out).doesNotContain("First argument must be subcommand");
    assertThat(out).containsPattern("-f <string>\\s+Load options from the given file");
  }

  private void assertSectionHelp() {
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out).contains(HelpUtils.getVersionMessage());

    // The following assures us that we're looking at section help, not global help.
    assertThat(out).doesNotContain("GETTING MORE HELP");
    assertThat(out).doesNotContain("--connector.json.url");
    assertThat(out).doesNotContain("--connector.csv.url");

    assertThat(out).contains("-p, --driver.auth.password");
  }
}
