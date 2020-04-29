/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.runner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.slf4j.event.Level.ERROR;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.commons.utils.ConsoleUtils;
import com.datastax.oss.dsbulk.runner.cli.CommandLineParser;
import com.datastax.oss.dsbulk.runner.cli.ParseException;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogConfigurationResource;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamType;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import com.datastax.oss.dsbulk.workflow.api.WorkflowProvider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import java.util.stream.Stream;
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
@LogConfigurationResource("logback.xml")
class DataStaxBulkLoaderTest {

  private final StreamInterceptor stdOut;
  private final StreamInterceptor stdErr;
  private final LogInterceptor logs;
  private Path tempFolder;

  DataStaxBulkLoaderTest(
      @StreamCapture(StreamType.STDOUT) StreamInterceptor stdOut,
      @StreamCapture(StreamType.STDERR) StreamInterceptor stdErr,
      @LogCapture(level = ERROR, loggerName = "com.datastax.oss.dsbulk") LogInterceptor logs) {
    this.stdOut = stdOut;
    this.stdErr = stdErr;
    this.logs = logs;
  }

  @AfterEach
  void resetAnsi() {
    AnsiConsole.systemUninstall();
  }

  @BeforeEach
  void createTempFolder() throws IOException {
    tempFolder = Files.createTempDirectory("test");
  }

  @AfterEach
  void deleteTempFolder() {
    FileUtils.deleteDirectory(tempFolder);
    FileUtils.deleteDirectory(Paths.get("./logs"));
  }

  @BeforeEach
  @AfterEach
  void resetConfig() {
    ConfigFactory.invalidateCaches();
    System.clearProperty("config.file");
  }

  @Test
  void should_show_global_help_when_no_args() {
    // global help, no shortcuts, has both json and csv settings.
    new DataStaxBulkLoader().run();
    assertGlobalHelp(false);
  }

  @Test
  void should_show_global_help_when_help_opt_arg() {
    // global help, no shortcuts, has both json and csv settings.
    new DataStaxBulkLoader("--help").run();
    assertGlobalHelp(false);
  }

  @Test
  void should_show_global_help_when_help_subcommand() {
    // global help, no shortcuts, has both json and csv settings.
    new DataStaxBulkLoader("help").run();
    assertGlobalHelp(false);
  }

  @Test
  void should_show_section_help_when_help_opt_arg() {
    new DataStaxBulkLoader("--help", "dsbulk.batch").run();
    assertSectionHelp();
  }

  @Test
  void should_show_section_help_when_help_subcommand() {
    new DataStaxBulkLoader("help", "dsbulk.batch").run();
    assertSectionHelp();
  }

  @Test
  void should_show_global_help_filtered_when_help_opt_arg() {
    // global help, with shortcuts, has only json common settings.
    new DataStaxBulkLoader("--help", "-c", "json").run();
    assertGlobalHelp(true);
  }

  @Test
  void should_show_global_help_filtered_when_help_subcommand() {
    // global help, with shortcuts, has only json common settings.
    new DataStaxBulkLoader("help", "-c", "json").run();
    assertGlobalHelp(true);
  }

  @Test
  void should_show_section_help_when_help_opt_arg_with_connector() {
    new DataStaxBulkLoader("--help", "-c", "json", "dsbulk.batch").run();
    assertSectionHelp();
  }

  @Test
  void should_show_section_help_when_help_subcommand_with_connector() {
    new DataStaxBulkLoader("help", "-c", "json", "dsbulk.batch").run();
    assertSectionHelp();
  }

  @Test
  void should_show_error_when_junk_subcommand() {
    new DataStaxBulkLoader("junk").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("First argument must be subcommand")
        .contains(", or \"help\"");
  }

  @Test
  void should_show_help_without_error_when_junk_subcommand_and_help() {
    new DataStaxBulkLoader("junk", "--help").run();
    assertThat(stdErr.getStreamAsString()).doesNotContain("First argument must be subcommand");
    assertGlobalHelp(false);
  }

  @Test
  void should_show_help_without_error_when_good_subcommand_and_help() {
    new DataStaxBulkLoader("load", "--help").run();
    assertThat(stdErr.getStreamAsString()).doesNotContain("First argument must be subcommand");
    assertGlobalHelp(false);
  }

  @Test
  void should_show_error_for_help_bad_section() {
    new DataStaxBulkLoader("help", "noexist").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("noexist is not a valid section. Available sections include")
        .contains("dsbulk.connector.csv")
        .contains("dsbulk.batch")
        .contains("driver");
  }

  @Test
  void should_show_section_help() {
    new DataStaxBulkLoader("help", "dsbulk.batch").run();
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertSectionHelp();
    assertThat(out)
        .contains("--batch.mode,")
        .contains("--dsbulk.batch.mode <string>")
        .doesNotContain("This section has the following subsections");
  }

  @Test
  void should_show_section_help_with_subsection_pointers() {
    new DataStaxBulkLoader("help", "dsbulk.connector").run();
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out)
        .contains("--connector.name,")
        .contains("--dsbulk.connector.name")
        .contains("This section has the following subsections")
        .contains("connector.csv")
        .contains("connector.json");
  }

  @Test
  void should_show_section_help_with_connector_shortcuts() {
    new DataStaxBulkLoader("help", "dsbulk.connector.csv").run();
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out).contains("-url,");
    assertThat(out).contains("--connector.csv.url,");
    assertThat(out).contains("--dsbulk.connector.csv.url <string>");
  }

  @Test
  void should_show_section_help_for_driver() {
    new DataStaxBulkLoader("help", "driver").run();
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out).contains("Any valid driver setting can be specified on the command line");
    assertThat(out).contains("-h,");
    assertThat(out).contains("--driver.basic.contact-points,");
    assertThat(out).contains("--datastax-java-driver.basic.contact-points <list<string>>");
    assertThat(out).contains("See the Java Driver online documentation for more information");
  }

  @Test
  void should_respect_custom_config_file() throws Exception {
    {
      Path f = Files.createTempFile(tempFolder, "myapp", ".conf");
      Files.write(f, "dsbulk.connector.name=junk".getBytes(UTF_8));
      new DataStaxBulkLoader("load", "-f", f.toString()).run();
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
      new DataStaxBulkLoader("load", "-f", f.toString()).run();
      String err = logs.getAllMessagesAsString();
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .contains("Invalid value for dsbulk.driver.socket.readTimeout");
    }
    // DAT-221: -f should expand user home
    logs.clear();
    {
      Path f = Files.createTempFile(Paths.get(System.getProperty("user.home")), "myapp", ".conf");
      f.toFile().deleteOnExit();
      Files.write(f, "dsbulk.connector.name=foo".getBytes(UTF_8));
      new DataStaxBulkLoader("load", "-f", "~/" + f.getFileName().toString()).run();
      String err = logs.getAllMessagesAsString();
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .doesNotContain("InvalidPathException")
          .contains("Cannot find connector 'foo'");
    }
  }

  @Test
  void should_error_out_for_bad_config_file() {
    new DataStaxBulkLoader("load", "-f", "noexist").run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .doesNotContain("First argument must be subcommand")
        .contains("Operation failed: Application file")
        .contains("noexist does not exist");
  }

  @Test
  void should_accept_connector_name_in_args_over_config_file() throws Exception {
    Path f = Files.createTempFile(tempFolder, "myapp", ".conf");
    Files.write(f, "dsbulk.connector.name=junk".getBytes(UTF_8));
    new DataStaxBulkLoader("load", "-c", "fromargs", "-f", f.toString()).run();
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
      new DataStaxBulkLoader("unload", "--connector.csv.url", StringUtils.quoteJson(unloadDir))
          .run();
      String err = logs.getAllMessagesAsString();
      assertThat(err).contains("connector.csv.url: target directory").contains("must be empty");
    } finally {
      if (unloadDir != null) {
        FileUtils.deleteDirectory(unloadDir);
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
              "unload", "-c", "json", "--connector.json.url", StringUtils.quoteJson(unloadDir))
          .run();
      String err = logs.getAllMessagesAsString();
      assertThat(err).contains("connector.json.url: target directory").contains("must be empty");
    } finally {
      if (unloadDir != null) {
        FileUtils.deleteDirectory(unloadDir);
      }
    }
  }

  @Test
  void should_handle_connector_name_long_option() {
    new DataStaxBulkLoader("load", "--connector.name", "fromargs").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
    new DataStaxBulkLoader("load", "--connector.name", "\"fromargs\"").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
    new DataStaxBulkLoader("load", "--connector.name", "'fromargs'").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  void should_handle_connector_name_long_option_with_equal() {
    new DataStaxBulkLoader("load", "--connector.name=fromargs").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
    new DataStaxBulkLoader("load", "--\"connector.name\"=\"fromargs\"").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
    new DataStaxBulkLoader("load", "--'connector.name'='fromargs'").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  void should_error_out_for_bad_execution_id_template() {
    new DataStaxBulkLoader("load", "--engine.executionId", "%4$s").run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("Operation failed")
        .contains("Could not generate execution ID with template: '%4$s'");
  }

  @Test
  void should_accept_escaped_control_char() throws Exception {
    // control chars should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--connector.csv.delimiter", "\\t").parse().getConfig();
    assertThat(result.getString("dsbulk.connector.csv.delimiter")).isEqualTo("\t");
  }

  @Test
  void should_accept_escaped_backslash() throws Exception {
    // backslashes should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--connector.csv.url", "C:\\\\Users").parse().getConfig();
    assertThat(result.getString("dsbulk.connector.csv.url")).isEqualTo("C:\\Users");
  }

  @Test
  void should_accept_escaped_double_quote() throws Exception {
    // double quotes should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--connector.csv.escape", "\\\"").parse().getConfig();
    assertThat(result.getString("dsbulk.connector.csv.escape")).isEqualTo("\"");
  }

  @Test
  void should_accept_escaped_double_quote_in_complex_type() throws Exception {
    // double quotes should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--codec.booleanStrings", "[\"foo\\\"bar\"]")
            .parse()
            .getConfig();
    assertThat(result.getStringList("dsbulk.codec.booleanStrings")).containsExactly("foo\"bar");
  }

  @Test
  void should_not_add_quote_if_already_quoted() throws Exception {
    // double quotes should be provided escaped as valid HOCON
    Config result =
        new CommandLineParser("load", "--connector.csv.delimiter", "\"\\t\"").parse().getConfig();
    assertThat(result.getString("dsbulk.connector.csv.delimiter")).isEqualTo("\t");
  }

  @Test
  void should_not_accept_parse_error() {
    ParseException error =
        catchThrowableOfType(
            () -> new CommandLineParser("load", "--codec.booleanStrings", "[a,b").parse(),
            ParseException.class);
    assertThat(error)
        .hasMessageContaining(
            "Invalid value for dsbulk.codec.booleanStrings, expecting LIST, got: '[a,b'")
        .hasCauseInstanceOf(IllegalArgumentException.class);
    assertThat(error.getCause()).hasMessageContaining("h");
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_short_options() {
    return Stream.of(
        Arguments.of("-locale", "locale", "dsbulk.codec.locale", "locale"),
        Arguments.of("-timeZone", "tz", "dsbulk.codec.timeZone", "tz"),
        Arguments.of("-c", "csv", "dsbulk.connector.name", "csv"),
        Arguments.of("-p", "pass", "datastax-java-driver.advanced.auth-provider.password", "pass"),
        Arguments.of("-u", "user", "datastax-java-driver.advanced.auth-provider.username", "user"),
        Arguments.of(
            "-h",
            "host1, host2",
            "datastax-java-driver.basic.contact-points",
            Lists.newArrayList("host1", "host2")),
        Arguments.of(
            "-maxRetries", "42", "datastax-java-driver.advanced.retry-policy.max-retries", 42),
        Arguments.of("-port", "9876", "datastax-java-driver.basic.default-port", 9876),
        Arguments.of("-cl", "cl", "datastax-java-driver.basic.request.consistency", "cl"),
        Arguments.of(
            "-dc",
            "dc1",
            "datastax-java-driver.basic.load-balancing-policy.local-datacenter",
            "dc1"),
        Arguments.of(
            "-b",
            "/path/to/bundle",
            "datastax-java-driver.basic.cloud.secure-connect-bundle",
            "/path/to/bundle"),
        Arguments.of("-maxErrors", "123", "dsbulk.log.maxErrors", 123),
        Arguments.of("-logDir", "logdir", "dsbulk.log.directory", "logdir"),
        Arguments.of("-jmx", "false", "dsbulk.monitoring.jmx", false),
        Arguments.of("-reportRate", "10 seconds", "dsbulk.monitoring.reportRate", "10 seconds"),
        Arguments.of("-k", "ks", "dsbulk.schema.keyspace", "ks"),
        Arguments.of(
            "-m",
            "{0:\"f1\", 1:\"f2\"}",
            "dsbulk.schema.mapping",
            "{0:f1, 1:f2}"), // type is forced to string
        Arguments.of(
            "-nullStrings",
            "[nil, nada]",
            "dsbulk.codec.nullStrings",
            Lists.newArrayList("nil", "nada")),
        Arguments.of("-query", "INSERT INTO foo", "dsbulk.schema.query", "INSERT INTO foo"),
        Arguments.of("-t", "table", "dsbulk.schema.table", "table"),
        Arguments.of("-dryRun", "true", "dsbulk.engine.dryRun", true));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_short_options(
      String shortOptionName, String shortOptionValue, String setting, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", shortOptionName, shortOptionValue).parse().getConfig();
    assertThat(result.getAnyRef(setting)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_csv_short_options() {
    return Stream.of(
        Arguments.of("-comment", "comment", "dsbulk.connector.csv.comment", "comment"),
        Arguments.of("-delim", "|", "dsbulk.connector.csv.delimiter", "|"),
        Arguments.of("-encoding", "enc", "dsbulk.connector.csv.encoding", "enc"),
        Arguments.of("-header", "header", "dsbulk.connector.csv.header", "header"),
        Arguments.of("-escape", "^", "dsbulk.connector.csv.escape", "^"),
        Arguments.of("-skipRecords", "3", "dsbulk.connector.csv.skipRecords", 3),
        Arguments.of("-maxRecords", "111", "dsbulk.connector.csv.maxRecords", 111),
        Arguments.of("-maxConcurrentFiles", "2C", "dsbulk.connector.csv.maxConcurrentFiles", "2C"),
        Arguments.of("-quote", "'", "dsbulk.connector.csv.quote", "'"),
        Arguments.of("-url", "http://findit", "dsbulk.connector.csv.url", "http://findit"));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_csv_short_options(
      String shortOptionName, String shortOptionValue, String setting, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", shortOptionName, shortOptionValue).parse().getConfig();
    assertThat(result.getAnyRef(setting)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_json_short_options() {
    return Stream.of(
        Arguments.of("-encoding", "enc", "dsbulk.connector.json.encoding", "enc"),
        Arguments.of("-skipRecords", "3", "dsbulk.connector.json.skipRecords", 3),
        Arguments.of("-maxRecords", "111", "dsbulk.connector.json.maxRecords", 111),
        Arguments.of("-maxConcurrentFiles", "2C", "dsbulk.connector.json.maxConcurrentFiles", "2C"),
        Arguments.of("-url", "http://findit", "dsbulk.connector.json.url", "http://findit"));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_json_short_options(
      String shortOptionName, String shortOptionValue, String setting, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", "-c", "json", shortOptionName, shortOptionValue)
            .parse()
            .getConfig();
    assertThat(result.getAnyRef(setting)).isEqualTo(expected);
  }

  @Test
  void should_reject_concatenated_option_value() {
    assertThrows(
        ParseException.class,
        () -> new CommandLineParser("load", "-kks").parse(),
        "Unrecognized option: -kks");
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_long_options() {
    return Stream.of(
        Arguments.of("dsbulk.driver.hosts", "host1, host2", Lists.newArrayList("host1", "host2")),
        Arguments.of("dsbulk.driver.port", "1", 1),
        Arguments.of("dsbulk.driver.protocol.compression", "NONE", "NONE"),
        Arguments.of("dsbulk.driver.pooling.local.connections", "2", 2),
        Arguments.of("dsbulk.driver.pooling.remote.connections", "3", 3),
        Arguments.of("dsbulk.driver.pooling.local.requests", "4", 4),
        Arguments.of("dsbulk.driver.pooling.remote.requests", "5", 5),
        Arguments.of("dsbulk.driver.pooling.heartbeat", "6 seconds", "6 seconds"),
        Arguments.of("dsbulk.driver.query.consistency", "cl", "cl"),
        Arguments.of("dsbulk.driver.query.serialConsistency", "serial-cl", "serial-cl"),
        Arguments.of("dsbulk.driver.query.fetchSize", "7", 7),
        Arguments.of("dsbulk.driver.query.idempotence", "false", false),
        Arguments.of("dsbulk.driver.socket.readTimeout", "8 seconds", "8 seconds"),
        Arguments.of("dsbulk.driver.auth.provider", "myauth", "myauth"),
        Arguments.of("dsbulk.driver.auth.username", "user", "user"),
        Arguments.of("dsbulk.driver.auth.password", "pass", "pass"),
        Arguments.of("dsbulk.driver.auth.authorizationId", "authid", "authid"),
        Arguments.of("dsbulk.driver.auth.principal", "user@foo.com", "user@foo.com"),
        Arguments.of("dsbulk.driver.auth.keyTab", "mykeytab", "mykeytab"),
        Arguments.of("dsbulk.driver.auth.saslService", "sasl", "sasl"),
        Arguments.of("dsbulk.driver.ssl.provider", "myssl", "myssl"),
        Arguments.of("dsbulk.driver.ssl.cipherSuites", "[TLS]", Lists.newArrayList("TLS")),
        Arguments.of("dsbulk.driver.ssl.truststore.path", "trust-path", "trust-path"),
        Arguments.of("dsbulk.driver.ssl.truststore.password", "trust-pass", "trust-pass"),
        Arguments.of("dsbulk.driver.ssl.truststore.algorithm", "trust-alg", "trust-alg"),
        Arguments.of("dsbulk.driver.ssl.keystore.path", "keystore-path", "keystore-path"),
        Arguments.of("dsbulk.driver.ssl.keystore.password", "keystore-pass", "keystore-pass"),
        Arguments.of("dsbulk.driver.ssl.keystore.algorithm", "keystore-alg", "keystore-alg"),
        Arguments.of("dsbulk.driver.ssl.openssl.keyCertChain", "key-cert-chain", "key-cert-chain"),
        Arguments.of("dsbulk.driver.ssl.openssl.privateKey", "key", "key"),
        Arguments.of("dsbulk.driver.timestampGenerator", "ts-gen", "ts-gen"),
        Arguments.of("dsbulk.driver.addressTranslator", "address-translator", "address-translator"),
        Arguments.of("dsbulk.driver.policy.lbp.dcAwareRoundRobin.localDc", "localDc", "localDc"),
        Arguments.of(
            "dsbulk.driver.policy.lbp.whiteList.hosts",
            "wh1, wh2",
            Lists.newArrayList("wh1", "wh2")),
        Arguments.of("dsbulk.driver.policy.maxRetries", "29", 29),
        Arguments.of("dsbulk.engine.dryRun", "true", true),
        Arguments.of("dsbulk.engine.executionId", "MY_EXEC_ID", "MY_EXEC_ID"),
        Arguments.of("dsbulk.batch.mode", "batch-mode", "batch-mode"),
        Arguments.of("dsbulk.batch.bufferSize", "9", 9),
        Arguments.of("dsbulk.batch.maxBatchSize", "10", 10),
        Arguments.of("dsbulk.executor.maxInFlight", "12", 12),
        Arguments.of("dsbulk.executor.maxPerSecond", "13", 13),
        Arguments.of("dsbulk.executor.continuousPaging.pageUnit", "BYTES", "BYTES"),
        Arguments.of("dsbulk.executor.continuousPaging.pageSize", "14", 14),
        Arguments.of("dsbulk.executor.continuousPaging.maxPages", "15", 15),
        Arguments.of("dsbulk.executor.continuousPaging.maxPagesPerSecond", "16", 16),
        Arguments.of("dsbulk.log.directory", "log-out", "log-out"),
        Arguments.of("dsbulk.log.maxErrors", "18", 18),
        Arguments.of("dsbulk.log.stmt.level", "NORMAL", "NORMAL"),
        Arguments.of("dsbulk.log.stmt.maxQueryStringLength", "19", 19),
        Arguments.of("dsbulk.log.stmt.maxBoundValues", "20", 20),
        Arguments.of("dsbulk.log.stmt.maxBoundValueLength", "21", 21),
        Arguments.of("dsbulk.log.stmt.maxInnerStatements", "22", 22),
        Arguments.of("dsbulk.codec.locale", "locale", "locale"),
        Arguments.of("dsbulk.codec.timeZone", "tz", "tz"),
        Arguments.of(
            "dsbulk.codec.booleanStrings", "[\"Si\", \"No\"]", Lists.newArrayList("Si", "No")),
        Arguments.of("dsbulk.codec.number", "codec-number", "codec-number"),
        Arguments.of("dsbulk.codec.timestamp", "codec-ts", "codec-ts"),
        Arguments.of("dsbulk.codec.date", "codec-date", "codec-date"),
        Arguments.of("dsbulk.codec.time", "codec-time", "codec-time"),
        Arguments.of("dsbulk.monitoring.reportRate", "23 sec", "23 sec"),
        Arguments.of("dsbulk.monitoring.rateUnit", "rate-unit", "rate-unit"),
        Arguments.of("dsbulk.monitoring.durationUnit", "duration-unit", "duration-unit"),
        Arguments.of("dsbulk.monitoring.expectedWrites", "24", 24),
        Arguments.of("dsbulk.monitoring.expectedReads", "25", 25),
        Arguments.of("dsbulk.monitoring.jmx", "false", false),
        Arguments.of("dsbulk.schema.keyspace", "ks", "ks"),
        Arguments.of("dsbulk.schema.table", "table", "table"),
        Arguments.of("dsbulk.schema.query", "SELECT JUNK", "SELECT JUNK"),
        Arguments.of(
            "dsbulk.schema.queryTimestamp", "2018-05-18T15:00:00Z", "2018-05-18T15:00:00Z"),
        Arguments.of("dsbulk.schema.queryTtl", "28", 28),
        Arguments.of("dsbulk.codec.nullStrings", "[NIL, NADA]", Lists.newArrayList("NIL", "NADA")),
        Arguments.of("dsbulk.schema.nullToUnset", "false", false),
        Arguments.of(
            "dsbulk.schema.mapping",
            "{0:\"f1\", 1:\"f2\"}",
            "{0:f1, 1:f2}"), // type is forced to string
        Arguments.of("dsbulk.connector.name", "conn", "conn"));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_long_options(String settingName, String settingValue, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", "--" + settingName, settingValue).parse().getConfig();
    assertThat(result.getAnyRef(settingName)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_csv_long_options() {
    return Stream.of(
        Arguments.of("dsbulk.connector.csv.url", "url", "url"),
        Arguments.of("dsbulk.connector.csv.fileNamePattern", "pat", "pat"),
        Arguments.of("dsbulk.connector.csv.fileNameFormat", "fmt", "fmt"),
        Arguments.of("dsbulk.connector.csv.recursive", "true", true),
        Arguments.of("dsbulk.connector.csv.maxConcurrentFiles", "2C", "2C"),
        Arguments.of("dsbulk.connector.csv.encoding", "enc", "enc"),
        Arguments.of("dsbulk.connector.csv.header", "false", false),
        Arguments.of("dsbulk.connector.csv.delimiter", "|", "|"),
        Arguments.of("dsbulk.connector.csv.quote", "'", "'"),
        Arguments.of("dsbulk.connector.csv.escape", "*", "*"),
        Arguments.of("dsbulk.connector.csv.comment", "#", "#"),
        Arguments.of("dsbulk.connector.csv.skipRecords", "2", 2),
        Arguments.of("dsbulk.connector.csv.maxRecords", "3", 3));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_csv_long_options(String settingName, String settingValue, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", "--" + settingName, settingValue).parse().getConfig();
    assertThat(result.getAnyRef(settingName)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_process_json_long_options() {
    return Stream.of(
        Arguments.of("dsbulk.connector.json.url", "url", "url"),
        Arguments.of("dsbulk.connector.json.fileNamePattern", "pat", "pat"),
        Arguments.of("dsbulk.connector.json.fileNameFormat", "fmt", "fmt"),
        Arguments.of("dsbulk.connector.json.recursive", "true", true),
        Arguments.of("dsbulk.connector.json.maxConcurrentFiles", "2C", "2C"),
        Arguments.of("dsbulk.connector.json.encoding", "enc", "enc"),
        Arguments.of("dsbulk.connector.json.skipRecords", "2", 2),
        Arguments.of("dsbulk.connector.json.maxRecords", "3", 3),
        Arguments.of("dsbulk.connector.json.mode", "SINGLE_DOCUMENT", "SINGLE_DOCUMENT"),
        Arguments.of(
            "dsbulk.connector.json.parserFeatures",
            "{f1 = true, f2 = false}",
            ImmutableMap.of("f1", true, "f2", false)),
        Arguments.of(
            "dsbulk.connector.json.generatorFeatures",
            "{g1 = true, g2 = false}",
            ImmutableMap.of("g1", true, "g2", false)),
        Arguments.of(
            "dsbulk.connector.json.serializationFeatures",
            "{s1 = true, s2 = false}",
            ImmutableMap.of("s1", true, "s2", false)),
        Arguments.of(
            "dsbulk.connector.json.deserializationFeatures",
            "{d1 = true, d2 = false}",
            ImmutableMap.of("d1", true, "d2", false)),
        Arguments.of("dsbulk.connector.json.prettyPrint", "true", true));
  }

  @ParameterizedTest
  @MethodSource
  void should_process_json_long_options(String settingName, String settingValue, Object expected)
      throws Exception {
    Config result =
        new CommandLineParser("load", "--" + settingName, settingValue).parse().getConfig();
    assertThat(result.getAnyRef(settingName)).isEqualTo(expected);
  }

  @Test
  void should_show_version_message_when_asked_long_option() {
    new DataStaxBulkLoader("--version").run();
    String out = stdOut.getStreamAsString();
    assertThat(out).isEqualTo(String.format("%s%n", ConsoleUtils.getBulkLoaderNameAndVersion()));
  }

  @Test
  void should_show_version_message_when_asked_short_option() {
    new DataStaxBulkLoader("-v").run();
    String out = stdOut.getStreamAsString();
    assertThat(out).isEqualTo(String.format("%s%n", ConsoleUtils.getBulkLoaderNameAndVersion()));
  }

  @Test
  void should_show_error_when_unload_and_dryRun() {
    new DataStaxBulkLoader("unload", "-dryRun", "true", "-url", "/foo/bar", "-k", "k1", "-t", "t1")
        .run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("Dry-run is not supported for unload");
  }

  @Test
  void should_error_on_backslash() throws URISyntaxException {
    Path badJson = Paths.get(ClassLoader.getSystemResource("bad-json.conf").toURI());
    new DataStaxBulkLoader(
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
            badJson.toString())
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

    assertThat(out).contains(ConsoleUtils.getBulkLoaderNameAndVersion());
    assertThat(out).contains("-v, --version Show program's version number and exit");

    ServiceLoader<WorkflowProvider> loader = ServiceLoader.load(WorkflowProvider.class);
    for (WorkflowProvider workflowProvider : loader) {
      assertThat(out).contains(workflowProvider.getTitle());
      assertThat(out).contains(workflowProvider.getDescription());
    }

    // The following assures us that we're looking at global help, not section help.
    assertThat(out).contains("GETTING MORE HELP");

    // The tests try restricting global help to json connector, or show all connectors.
    // If all, shortcut options for connector settings should not be shown.
    // If restricted to json, show the shortcut options for common json settings.
    assertThat(out).contains("--connector.json.url");
    assertThat(out).contains("--dsbulk.connector.json.url");
    if (jsonOnly) {
      assertThat(out).contains("-url,");
      assertThat(out).contains("--connector.json.url,");
      assertThat(out).contains("--dsbulk.connector.json.url <string>");
      assertThat(out).doesNotContain("connector.csv.url");
    } else {
      assertThat(out).contains("--connector.csv.url,");
      assertThat(out).contains("--dsbulk.connector.csv.url <string>");
      assertThat(out).doesNotContain("-url");
    }
    assertThat(out).doesNotContain("First argument must be subcommand");
    assertThat(out).containsPattern("-f <string>\\s+Load options from the given file");
  }

  private void assertSectionHelp() {
    CharSequence out = new AnsiString(stdOut.getStreamAsString()).getPlain();
    assertThat(out).contains(ConsoleUtils.getBulkLoaderNameAndVersion());

    // The following assures us that we're looking at section help, not global help.
    assertThat(out).doesNotContain("GETTING MORE HELP");
    assertThat(out).doesNotContain("--connector.json.url");
    assertThat(out).doesNotContain("--dsbulk.connector.json.url");
    assertThat(out).doesNotContain("--connector.csv.url");
    assertThat(out).doesNotContain("--dsbulk.connector.csv.url");

    assertThat(out).contains("--batch.mode,");
    assertThat(out).contains("--dsbulk.batch.mode <string>");
  }
}
