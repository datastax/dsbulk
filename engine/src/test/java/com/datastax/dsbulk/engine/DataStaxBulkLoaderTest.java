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
import org.apache.commons.cli.ParseException;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.AnsiString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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

  @Test
  void should_process_short_options() throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load",
            new String[] {
              "-locale",
              "locale",
              "-timeZone",
              "tz",
              "-c",
              "csv",
              "-p",
              "pass",
              "-u",
              "user",
              "-h",
              "host1, host2",
              "-maxRetries",
              "42",
              "-port",
              "9876",
              "-cl",
              "cl",
              "-maxErrors",
              "123",
              "-logDir",
              "logdir",
              "-jmx",
              "false",
              "-reportRate",
              "456",
              "-k",
              "ks",
              "-m",
              "{0:\"f1\", 1:\"f2\"}",
              "-nullStrings",
              "[nil, nada]",
              "-query",
              "INSERT INTO foo",
              "-t",
              "table",
              "-dryRun",
              "true",
              // CSV-specific options
              "-comment",
              "comment",
              "-delim",
              "|",
              "-encoding",
              "enc",
              "-escape",
              "^",
              "-header",
              "header",
              "-skipRecords",
              "3",
              "-maxRecords",
              "111",
              "-maxConcurrentFiles",
              "222",
              "-quote",
              "'",
              "-url",
              "http://findit"
            });
    assertThat(result.getString("codec.locale")).isEqualTo("locale");
    assertThat(result.getString("codec.timeZone")).isEqualTo("tz");
    assertThat(result.getString("connector.name")).isEqualTo("csv");
    assertThat(result.getString("driver.auth.password")).isEqualTo("pass");
    assertThat(result.getString("driver.auth.username")).isEqualTo("user");
    assertThat(result.getStringList("driver.hosts")).containsExactly("host1", "host2");
    assertThat(result.getInt("driver.policy.maxRetries")).isEqualTo(42);
    assertThat(result.getInt("driver.port")).isEqualTo(9876);
    assertThat(result.getString("driver.query.consistency")).isEqualTo("cl");
    assertThat(result.getBoolean("engine.dryRun")).isTrue();
    assertThat(result.getInt("log.maxErrors")).isEqualTo(123);
    assertThat(result.getString("log.directory")).isEqualTo("logdir");
    assertThat(result.getBoolean("monitoring.jmx")).isFalse();
    assertThat(result.getInt("monitoring.reportRate")).isEqualTo(456);
    assertThat(result.getString("schema.keyspace")).isEqualTo("ks");
    assertThat(result.getString("schema.mapping")).isEqualTo("{0:f1, 1:f2}");
    assertThat(result.getStringList("codec.nullStrings")).containsExactly("nil", "nada");
    assertThat(result.getString("schema.query")).isEqualTo("INSERT INTO foo");
    assertThat(result.getString("schema.table")).isEqualTo("table");

    // CSV short options
    assertThat(result.getString("connector.csv.comment")).isEqualTo("comment");
    assertThat(result.getString("connector.csv.delimiter")).isEqualTo("|");
    assertThat(result.getString("connector.csv.encoding")).isEqualTo("enc");
    assertThat(result.getString("connector.csv.escape")).isEqualTo("^");
    assertThat(result.getString("connector.csv.header")).isEqualTo("header");
    assertThat(result.getInt("connector.csv.skipRecords")).isEqualTo(3);
    assertThat(result.getInt("connector.csv.maxRecords")).isEqualTo(111);
    assertThat(result.getInt("connector.csv.maxConcurrentFiles")).isEqualTo(222);
    assertThat(result.getString("connector.csv.quote")).isEqualTo("'");
    assertThat(result.getString("connector.csv.url")).isEqualTo("http://findit");
  }

  @Test
  void should_process_csv_short_options_by_default() throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load",
            new String[] {
              "-comment",
              "comment",
              "-delim",
              "|",
              "-encoding",
              "enc",
              "-escape",
              "^",
              "-header",
              "header",
              "-skipRecords",
              "3",
              "-maxRecords",
              "111",
              "-maxConcurrentFiles",
              "222",
              "-quote",
              "'",
              "-url",
              "http://findit"
            });

    assertThat(result.getString("connector.csv.comment")).isEqualTo("comment");
    assertThat(result.getString("connector.csv.delimiter")).isEqualTo("|");
    assertThat(result.getString("connector.csv.encoding")).isEqualTo("enc");
    assertThat(result.getString("connector.csv.escape")).isEqualTo("^");
    assertThat(result.getString("connector.csv.header")).isEqualTo("header");
    assertThat(result.getInt("connector.csv.skipRecords")).isEqualTo(3);
    assertThat(result.getInt("connector.csv.maxRecords")).isEqualTo(111);
    assertThat(result.getInt("connector.csv.maxConcurrentFiles")).isEqualTo(222);
    assertThat(result.getString("connector.csv.quote")).isEqualTo("'");
    assertThat(result.getString("connector.csv.url")).isEqualTo("http://findit");
  }

  @Test
  void should_process_json_short_options() throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load",
            new String[] {
              "-c",
              "json",
              "-encoding",
              "enc",
              "-skipRecords",
              "3",
              "-maxRecords",
              "111",
              "-maxConcurrentFiles",
              "222",
              "-url",
              "http://findit"
            });

    assertThat(result.getString("connector.json.encoding")).isEqualTo("enc");
    assertThat(result.getInt("connector.json.skipRecords")).isEqualTo(3);
    assertThat(result.getInt("connector.json.maxRecords")).isEqualTo(111);
    assertThat(result.getInt("connector.json.maxConcurrentFiles")).isEqualTo(222);
    assertThat(result.getString("connector.json.url")).isEqualTo("http://findit");
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

  @Test
  void should_process_core_long_options() throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load",
            new String[] {
              "--driver.hosts",
              "host1, host2",
              "--driver.port",
              "1",
              "--driver.protocol.compression",
              "NONE",
              "--driver.pooling.local.connections",
              "2",
              "--driver.pooling.requests",
              "3",
              "--driver.pooling.remote.connections",
              "4",
              "--driver.pooling.heartbeat",
              "6 seconds",
              "--driver.query.consistency",
              "cl",
              "--driver.query.serialConsistency",
              "serial-cl",
              "--driver.query.fetchSize",
              "7",
              "--driver.query.idempotence",
              "false",
              "--driver.socket.readTimeout",
              "8 seconds",
              "--driver.auth.provider",
              "myauth",
              "--driver.auth.username",
              "user",
              "--driver.auth.password",
              "pass",
              "--driver.auth.authorizationId",
              "authid",
              "--driver.auth.principal",
              "user@foo.com",
              "--driver.auth.keyTab",
              "mykeytab",
              "--driver.auth.saslService",
              "sasl",
              "--driver.ssl.provider",
              "myssl",
              "--driver.ssl.cipherSuites",
              "[TLS]",
              "--driver.ssl.truststore.path",
              "trust-path",
              "--driver.ssl.truststore.password",
              "trust-pass",
              "--driver.ssl.truststore.algorithm",
              "trust-alg",
              "--driver.ssl.keystore.path",
              "keystore-path",
              "--driver.ssl.keystore.password",
              "keystore-pass",
              "--driver.ssl.keystore.algorithm",
              "keystore-alg",
              "--driver.ssl.openssl.keyCertChain",
              "key-cert-chain",
              "--driver.ssl.openssl.privateKey",
              "key",
              "--driver.timestampGenerator",
              "ts-gen",
              "--driver.addressTranslator",
              "address-translator",
              "--driver.policy.lbp.localDc",
              "localDc",
              "--driver.policy.lbp.whiteList",
              "wh1, wh2",
              "--driver.policy.maxRetries",
              "29",
              "--engine.dryRun",
              "true",
              "--engine.executionId",
              "MY_EXEC_ID",
              "--batch.mode",
              "batch-mode",
              "--batch.bufferSize",
              "9",
              "--batch.maxBatchSize",
              "10",
              "--executor.maxInFlight",
              "12",
              "--executor.maxPerSecond",
              "13",
              "--executor.continuousPaging.pageUnit",
              "BYTES",
              "--executor.continuousPaging.pageSize",
              "14",
              "--executor.continuousPaging.maxPages",
              "15",
              "--executor.continuousPaging.maxPagesPerSecond",
              "16",
              "--log.directory",
              "log-out",
              "--log.maxErrors",
              "18",
              "--log.stmt.level",
              "NORMAL",
              "--log.stmt.maxQueryStringLength",
              "19",
              "--log.stmt.maxBoundValues",
              "20",
              "--log.stmt.maxBoundValueLength",
              "21",
              "--log.stmt.maxInnerStatements",
              "22",
              "--codec.locale",
              "locale",
              "--codec.timeZone",
              "tz",
              "--codec.booleanStrings",
              "[\"Si\", \"No\"]",
              "--codec.number",
              "codec-number",
              "--codec.timestamp",
              "codec-ts",
              "--codec.date",
              "codec-date",
              "--codec.time",
              "codec-time",
              "--monitoring.reportRate",
              "23 sec",
              "--monitoring.rateUnit",
              "rate-unit",
              "--monitoring.durationUnit",
              "duration-unit",
              "--monitoring.expectedWrites",
              "24",
              "--monitoring.expectedReads",
              "25",
              "--monitoring.jmx",
              "false",
              "--schema.keyspace",
              "ks",
              "--schema.table",
              "table",
              "--schema.query",
              "SELECT JUNK",
              "--schema.queryTimestamp",
              "2018-05-18T15:00:00Z",
              "--schema.queryTtl",
              "28",
              "--codec.nullStrings",
              "[NIL, NADA]",
              "--schema.nullToUnset",
              "false",
              "--schema.mapping",
              "{0:\"f1\", 1:\"f2\"}",
              "--connector.name",
              "conn"
            });
    assertThat(result.getStringList("driver.hosts")).containsExactly("host1", "host2");
    assertThat(result.getInt("driver.port")).isEqualTo(1);
    assertThat(result.getString("driver.protocol.compression")).isEqualTo("NONE");
    assertThat(result.getInt("driver.pooling.local.connections")).isEqualTo(2);
    assertThat(result.getInt("driver.pooling.requests")).isEqualTo(3);
    assertThat(result.getInt("driver.pooling.remote.connections")).isEqualTo(4);
    assertThat(result.getString("driver.pooling.heartbeat")).isEqualTo("6 seconds");
    assertThat(result.getString("driver.query.consistency")).isEqualTo("cl");
    assertThat(result.getString("driver.query.serialConsistency")).isEqualTo("serial-cl");
    assertThat(result.getInt("driver.query.fetchSize")).isEqualTo(7);
    assertThat(result.getBoolean("driver.query.idempotence")).isFalse();
    assertThat(result.getString("driver.socket.readTimeout")).isEqualTo("8 seconds");
    assertThat(result.getString("driver.auth.provider")).isEqualTo("myauth");
    assertThat(result.getString("driver.auth.username")).isEqualTo("user");
    assertThat(result.getString("driver.auth.password")).isEqualTo("pass");
    assertThat(result.getString("driver.auth.authorizationId")).isEqualTo("authid");
    assertThat(result.getString("driver.auth.principal")).isEqualTo("user@foo.com");
    assertThat(result.getString("driver.auth.keyTab")).isEqualTo("mykeytab");
    assertThat(result.getString("driver.auth.saslService")).isEqualTo("sasl");
    assertThat(result.getString("driver.ssl.provider")).isEqualTo("myssl");
    assertThat(result.getStringList("driver.ssl.cipherSuites")).containsExactly("TLS");
    assertThat(result.getString("driver.ssl.truststore.path")).isEqualTo("trust-path");
    assertThat(result.getString("driver.ssl.truststore.password")).isEqualTo("trust-pass");
    assertThat(result.getString("driver.ssl.truststore.algorithm")).isEqualTo("trust-alg");
    assertThat(result.getString("driver.ssl.keystore.path")).isEqualTo("keystore-path");
    assertThat(result.getString("driver.ssl.keystore.password")).isEqualTo("keystore-pass");
    assertThat(result.getString("driver.ssl.keystore.algorithm")).isEqualTo("keystore-alg");
    assertThat(result.getString("driver.ssl.openssl.keyCertChain")).isEqualTo("key-cert-chain");
    assertThat(result.getString("driver.ssl.openssl.privateKey")).isEqualTo("key");
    assertThat(result.getString("driver.timestampGenerator")).isEqualTo("ts-gen");
    assertThat(result.getString("driver.addressTranslator")).isEqualTo("address-translator");
    assertThat(result.getString("driver.policy.lbp.localDc")).isEqualTo("localDc");
    assertThat(result.getStringList("driver.policy.lbp.whiteList")).containsExactly("wh1", "wh2");
    assertThat(result.getInt("driver.policy.maxRetries")).isEqualTo(29);
    assertThat(result.getBoolean("engine.dryRun")).isTrue();
    assertThat(result.getString("engine.executionId")).isEqualTo("MY_EXEC_ID");
    assertThat(result.getString("batch.mode")).isEqualTo("batch-mode");
    assertThat(result.getInt("batch.bufferSize")).isEqualTo(9);
    assertThat(result.getInt("batch.maxBatchSize")).isEqualTo(10);
    assertThat(result.getInt("executor.maxInFlight")).isEqualTo(12);
    assertThat(result.getInt("executor.maxPerSecond")).isEqualTo(13);
    assertThat(result.getString("executor.continuousPaging.pageUnit")).isEqualTo("BYTES");
    assertThat(result.getInt("executor.continuousPaging.pageSize")).isEqualTo(14);
    assertThat(result.getInt("executor.continuousPaging.maxPages")).isEqualTo(15);
    assertThat(result.getInt("executor.continuousPaging.maxPagesPerSecond")).isEqualTo(16);
    assertThat(result.getString("log.directory")).isEqualTo("log-out");
    assertThat(result.getInt("log.maxErrors")).isEqualTo(18);
    assertThat(result.getString("log.stmt.level")).isEqualTo("NORMAL");
    assertThat(result.getInt("log.stmt.maxQueryStringLength")).isEqualTo(19);
    assertThat(result.getInt("log.stmt.maxBoundValues")).isEqualTo(20);
    assertThat(result.getInt("log.stmt.maxBoundValueLength")).isEqualTo(21);
    assertThat(result.getInt("log.stmt.maxInnerStatements")).isEqualTo(22);
    assertThat(result.getString("codec.locale")).isEqualTo("locale");
    assertThat(result.getString("codec.timeZone")).isEqualTo("tz");
    assertThat(result.getStringList("codec.booleanStrings")).containsExactly("Si", "No");
    assertThat(result.getString("codec.number")).isEqualTo("codec-number");
    assertThat(result.getString("codec.timestamp")).isEqualTo("codec-ts");
    assertThat(result.getString("codec.date")).isEqualTo("codec-date");
    assertThat(result.getString("codec.time")).isEqualTo("codec-time");
    assertThat(result.getString("monitoring.reportRate")).isEqualTo("23 sec");
    assertThat(result.getString("monitoring.rateUnit")).isEqualTo("rate-unit");
    assertThat(result.getString("monitoring.durationUnit")).isEqualTo("duration-unit");
    assertThat(result.getInt("monitoring.expectedWrites")).isEqualTo(24);
    assertThat(result.getInt("monitoring.expectedReads")).isEqualTo(25);
    assertThat(result.getBoolean("monitoring.jmx")).isFalse();
    assertThat(result.getString("schema.keyspace")).isEqualTo("ks");
    assertThat(result.getString("schema.table")).isEqualTo("table");
    assertThat(result.getString("schema.query")).isEqualTo("SELECT JUNK");
    assertThat(result.getString("schema.queryTimestamp")).isEqualTo("2018-05-18T15:00:00Z");
    assertThat(result.getInt("schema.queryTtl")).isEqualTo(28);
    assertThat(result.getStringList("codec.nullStrings")).containsExactly("NIL", "NADA");
    assertThat(result.getString("schema.nullToUnset")).isEqualTo("false");
    assertThat(result.getString("schema.mapping")).isEqualTo("{0:f1, 1:f2}");
    assertThat(result.getString("connector.name")).isEqualTo("conn");
  }

  @Test
  void should_process_csv_long_options() throws Exception {
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load",
            new String[] {
              "--connector.csv.url",
              "url",
              "--connector.csv.fileNamePattern",
              "pat",
              "--connector.csv.fileNameFormat",
              "fmt",
              "--connector.csv.recursive",
              "true",
              "--connector.csv.maxConcurrentFiles",
              "1",
              "--connector.csv.encoding",
              "enc",
              "--connector.csv.header",
              "false",
              "--connector.csv.delimiter",
              "|",
              "--connector.csv.quote",
              "'",
              "--connector.csv.escape",
              "*",
              "--connector.csv.comment",
              "#",
              "--connector.csv.skipRecords",
              "2",
              "--connector.csv.maxRecords",
              "3"
            });
    assertThat(result.getString("connector.csv.url")).isEqualTo("url");
    assertThat(result.getString("connector.csv.fileNamePattern")).isEqualTo("pat");
    assertThat(result.getString("connector.csv.fileNameFormat")).isEqualTo("fmt");
    assertThat(result.getBoolean("connector.csv.recursive")).isTrue();
    assertThat(result.getInt("connector.csv.maxConcurrentFiles")).isEqualTo(1);
    assertThat(result.getString("connector.csv.encoding")).isEqualTo("enc");
    assertThat(result.getBoolean("connector.csv.header")).isFalse();
    assertThat(result.getString("connector.csv.delimiter")).isEqualTo("|");
    assertThat(result.getString("connector.csv.quote")).isEqualTo("'");
    assertThat(result.getString("connector.csv.escape")).isEqualTo("*");
    assertThat(result.getString("connector.csv.comment")).isEqualTo("#");
    assertThat(result.getInt("connector.csv.skipRecords")).isEqualTo(2);
    assertThat(result.getInt("connector.csv.maxRecords")).isEqualTo(3);
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

  @Test
  void should_process_json_long_options() throws Exception {
    DataStaxBulkLoader.DEFAULT = ConfigFactory.load().getConfig("dsbulk");
    Config result =
        DataStaxBulkLoader.parseCommandLine(
            "load",
            new String[] {
              "-c",
              "json",
              "--connector.json.url",
              "url",
              "--connector.json.mode",
              "SINGLE_DOCUMENT",
              "--connector.json.fileNamePattern",
              "pat",
              "--connector.json.fileNameFormat",
              "fmt",
              "--connector.json.recursive",
              "true",
              "--connector.json.maxConcurrentFiles",
              "1",
              "--connector.json.encoding",
              "enc",
              "--connector.json.skipRecords",
              "2",
              "--connector.json.maxRecords",
              "3",
              "--connector.json.parserFeatures",
              "{f1 = true, f2 = false}",
              "--connector.json.generatorFeatures",
              "{g1 = true, g2 = false}",
              "--connector.json.serializationFeatures",
              "{s1 = true, s2 = false}",
              "--connector.json.deserializationFeatures",
              "{d1 = true, d2 = false}",
              "--connector.json.prettyPrint",
              "true",
            });
    assertThat(result.getString("connector.json.url")).isEqualTo("url");
    assertThat(result.getString("connector.json.mode")).isEqualTo("SINGLE_DOCUMENT");
    assertThat(result.getString("connector.json.fileNamePattern")).isEqualTo("pat");
    assertThat(result.getString("connector.json.fileNameFormat")).isEqualTo("fmt");
    assertThat(result.getBoolean("connector.json.recursive")).isTrue();
    assertThat(result.getInt("connector.json.maxConcurrentFiles")).isEqualTo(1);
    assertThat(result.getString("connector.json.encoding")).isEqualTo("enc");
    assertThat(result.getInt("connector.json.skipRecords")).isEqualTo(2);
    assertThat(result.getInt("connector.json.maxRecords")).isEqualTo(3);
    assertThat(result.getObject("connector.json.parserFeatures").unwrapped())
        .isEqualTo(ImmutableMap.of("f1", true, "f2", false));
    assertThat(result.getObject("connector.json.generatorFeatures").unwrapped())
        .isEqualTo(ImmutableMap.of("g1", true, "g2", false));
    assertThat(result.getObject("connector.json.serializationFeatures").unwrapped())
        .isEqualTo(ImmutableMap.of("s1", true, "s2", false));
    assertThat(result.getObject("connector.json.deserializationFeatures").unwrapped())
        .isEqualTo(ImmutableMap.of("d1", true, "d2", false));
    assertThat(result.getBoolean("connector.json.prettyPrint")).isTrue();
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
