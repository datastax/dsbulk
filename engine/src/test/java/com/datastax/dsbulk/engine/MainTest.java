/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

package com.datastax.dsbulk.engine;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.datastax.dsbulk.engine.internal.HelpUtils;
import com.datastax.dsbulk.engine.internal.OptionUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

public class MainTest {

  private PrintStream originalStderr;
  private PrintStream originalStdout;
  private ByteArrayOutputStream stderr;
  private ByteArrayOutputStream stdout;
  private Logger root;
  private Level oldLevel;

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    originalStdout = System.out;
    originalStderr = System.err;
    stdout = new ByteArrayOutputStream();
    System.setOut(new PrintStream(stdout));
    stderr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(stderr));
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    oldLevel = root.getLevel();
    root.setLevel(Level.INFO);
  }

  @After
  public void tearDown() throws Exception {
    System.setOut(originalStdout);
    System.setErr(originalStderr);
    root.setLevel(oldLevel);
    System.clearProperty("config.file");
    ConfigFactory.invalidateCaches();
    OptionUtils.DEFAULT = ConfigFactory.load().getConfig("dsbulk");
  }

  @Test
  public void should_show_help_when_no_args() throws Exception {
    new Main(new String[] {}).run();
    assertGlobalHelp();
  }

  @Test
  public void should_show_help_when_help_opt_arg() throws Exception {
    new Main(new String[] {"--help"}).run();
    assertGlobalHelp();
  }

  @Test
  public void should_show_help_when_help_subcommand() throws Exception {
    new Main(new String[] {"help"}).run();
    assertGlobalHelp();
  }

  @Test
  public void should_show_error_when_junk_subcommand() throws Exception {
    new Main(new String[] {"junk"}).run();
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("First argument must be subcommand \"load\", \"unload\", or \"help\"");
  }

  @Test
  public void should_show_help_without_error_when_junk_subcommand_and_help() throws Exception {
    new Main(new String[] {"junk", "--help"}).run();
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).doesNotContain("First argument must be subcommand");
    assertGlobalHelp();
  }

  @Test
  public void should_show_help_without_error_when_good_subcommand_and_help() throws Exception {
    new Main(new String[] {"load", "--help"}).run();
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).doesNotContain("First argument must be subcommand");
    assertGlobalHelp();
  }

  @Test
  public void should_show_error_for_help_bad_section() throws Exception {
    new Main(new String[] {"help", "noexist"}).run();
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err)
        .contains("noexist is not a valid section. Available sections include")
        .contains("driver.auth");
  }

  @Test
  public void should_show_section_help() throws Exception {
    new Main(new String[] {"help", "batch"}).run();
    String out = new String(stdout.toByteArray(), StandardCharsets.UTF_8);
    assertThat(out)
        .contains("--batch.mode")
        .doesNotContain("This section has the following subsections");
  }

  @Test
  public void should_show_section_help_with_subsection_pointers() throws Exception {
    new Main(new String[] {"help", "driver"}).run();
    String out = new String(stdout.toByteArray(), StandardCharsets.UTF_8);
    assertThat(out)
        .contains("--driver.hosts")
        .contains("This section has the following subsections")
        .contains("driver.auth");
  }

  @Test
  public void should_show_section_help_with_connector_shortcuts() throws Exception {
    new Main(new String[] {"help", "connector.csv"}).run();
    String out = new String(stdout.toByteArray(), StandardCharsets.UTF_8);
    assertThat(out).contains("-url, --connector.csv.url");
  }

  @Test
  public void should_show_help_without_error_when_no_subcommand_and_help() throws Exception {
    new Main(new String[] {"-k", "k1", "--help"}).run();
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).doesNotContain("First argument must be subcommand");
    assertGlobalHelp();
  }

  @Test
  public void should_respect_custom_config_file() throws Exception {
    {
      File f = tempFolder.newFile("myapp.conf");
      Files.write(f.toPath(), "dsbulk.connector.name=junk".getBytes("UTF-8"));
      new Main(new String[] {"load", "-f", f.getPath()}).run();
      String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .contains("Cannot find connector 'junk'");
    }
    {
      File f = tempFolder.newFile("myapp3.conf");
      Files.write(f.toPath(), "dsbulk.driver.socket.readTimeout=wonky".getBytes("UTF-8"));
      new Main(new String[] {"load", "-f", f.getPath()}).run();
      String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
      assertThat(err)
          .doesNotContain("First argument must be subcommand")
          .contains("Invalid value at 'socket.readTimeout'");
    }
  }

  @Test
  public void should_error_out_for_bad_config_file() throws Exception {
    new Main(new String[] {"load", "-f", "noexist"}).run();
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err)
        .doesNotContain("First argument must be subcommand")
        .contains("noexist (No such file or directory)");
  }

  @Test
  public void should_accept_connector_name_in_args_over_config_file() throws Exception {
    File f = tempFolder.newFile("myapp.conf");
    Files.write(f.toPath(), "dsbulk.connector.name=junk".getBytes("UTF-8"));
    new Main(new String[] {"load", "-c", "fromargs", "-f", f.getPath()}).run();
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err)
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  public void should_handle_connector_name_long_option() throws Exception {
    new Main(new String[] {"load", "--connector.name", "fromargs"}).run();
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err)
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  public void should_handle_connector_name_long_option_with_equal() throws Exception {
    new Main(new String[] {"load", "--connector.name=fromargs"}).run();
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err)
        .doesNotContain("First argument must be subcommand")
        .contains("Cannot find connector 'fromargs'");
  }

  @Test
  public void should_process_short_options() throws Exception {
    Config result =
        Main.parseCommandLine(
            "csv",
            "load",
            new String[] {
              "-locale", "locale",
              "-timeZone", "tz",
              "-c", "conn",
              "-p", "pass",
              "-u", "user",
              "-h", "host1, host2",
              "-lbp", "lbp",
              "-maxRetries", "42",
              "-port", "9876",
              "-cl", "cl",
              "-maxErrors", "123",
              "-logDir", "logdir",
              "-jmx", "false",
              "-reportRate", "456",
              "-k", "ks",
              "-m", "{0:\"f1\", 1:\"f2\"}",
              "-nullStrings", "[nil, nada]",
              "-query", "INSERT INTO foo",
              "-t", "table",
              "-comment", "comment",
              "-delim", "|",
              "-encoding", "enc",
              "-escape", "^",
              "-header", "header",
              "-skipLines", "3",
              "-maxLines", "111",
              "-maxThreads", "222",
              "-quote", "'",
              "-url", "http://findit"
            });
    assertThat(result.getString("codec.locale")).isEqualTo("locale");
    assertThat(result.getString("codec.timeZone")).isEqualTo("tz");
    assertThat(result.getString("connector.name")).isEqualTo("conn");
    assertThat(result.getString("driver.auth.password")).isEqualTo("pass");
    assertThat(result.getString("driver.auth.username")).isEqualTo("user");
    assertThat(result.getString("driver.hosts")).isEqualTo("host1, host2");
    assertThat(result.getString("driver.policy.lbp.name")).isEqualTo("lbp");
    assertThat(result.getInt("driver.policy.maxRetries")).isEqualTo(42);
    assertThat(result.getInt("driver.port")).isEqualTo(9876);
    assertThat(result.getString("driver.query.consistency")).isEqualTo("cl");
    assertThat(result.getInt("log.maxErrors")).isEqualTo(123);
    assertThat(result.getString("log.directory")).isEqualTo("logdir");
    assertThat(result.getBoolean("monitoring.jmx")).isFalse();
    assertThat(result.getInt("monitoring.reportRate")).isEqualTo(456);
    assertThat(result.getString("schema.keyspace")).isEqualTo("ks");
    assertThat(result.getString("schema.mapping")).isEqualTo("{0:f1, 1:f2}");
    assertThat(result.getString("schema.nullStrings")).isEqualTo("[nil, nada]");
    assertThat(result.getString("schema.query")).isEqualTo("INSERT INTO foo");
    assertThat(result.getString("schema.table")).isEqualTo("table");

    // CSV short options
    assertThat(result.getString("connector.csv.comment")).isEqualTo("comment");
    assertThat(result.getString("connector.csv.delimiter")).isEqualTo("|");
    assertThat(result.getString("connector.csv.encoding")).isEqualTo("enc");
    assertThat(result.getString("connector.csv.escape")).isEqualTo("^");
    assertThat(result.getString("connector.csv.header")).isEqualTo("header");
    assertThat(result.getInt("connector.csv.skipLines")).isEqualTo(3);
    assertThat(result.getInt("connector.csv.maxLines")).isEqualTo(111);
    assertThat(result.getInt("connector.csv.maxThreads")).isEqualTo(222);
    assertThat(result.getString("connector.csv.quote")).isEqualTo("'");
    assertThat(result.getString("connector.csv.url")).isEqualTo("http://findit");
  }

  @Test
  public void should_process_core_long_options() throws Exception {
    Config result =
        Main.parseCommandLine(
            "csv",
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
              "--driver.pooling.local.requests",
              "3",
              "--driver.pooling.remote.connections",
              "4",
              "--driver.pooling.remote.requests",
              "5",
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
              "--driver.auth.saslProtocol",
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
              "--driver.policy.lbp.name",
              "lbp",
              "--driver.policy.lbp.dse.childPolicy",
              "dseChild",
              "--driver.policy.lbp.dcAwareRoundRobin.localDc",
              "localDc",
              "--driver.policy.lbp.dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel",
              "true",
              "--driver.policy.lbp.dcAwareRoundRobin.usedHostsPerRemoteDc",
              "28",
              "--driver.policy.lbp.tokenAware.childPolicy",
              "tokenAwareChild",
              "--driver.policy.lbp.tokenAware.shuffleReplicas",
              "false",
              "--driver.policy.lbp.whiteList.childPolicy",
              "whiteListChild",
              "--driver.policy.lbp.whiteList.hosts",
              "wh1, wh2",
              "--driver.policy.maxRetries",
              "29",
              "--batch.mode",
              "batch-mode",
              "--batch.bufferSize",
              "9",
              "--batch.maxBatchSize",
              "10",
              "--executor.maxThreads",
              "11",
              "--executor.maxConcurrentOps",
              "27",
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
              "--log.maxThreads",
              "17",
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
              "--codec.booleanWords",
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
              "--schema.nullStrings",
              "NIL, NADA",
              "--schema.nullToUnset",
              "false",
              "--schema.mapping",
              "{0:\"f1\", 1:\"f2\"}",
              "--schema.recordMetadata",
              "{0:\"f3\", 1:\"f4\"}",
              "--connector.name",
              "conn",
              "--engine.maxMappingThreads",
              "26"
            });
    assertThat(result.getString("driver.hosts")).isEqualTo("host1, host2");
    assertThat(result.getInt("driver.port")).isEqualTo(1);
    assertThat(result.getString("driver.protocol.compression")).isEqualTo("NONE");
    assertThat(result.getInt("driver.pooling.local.connections")).isEqualTo(2);
    assertThat(result.getInt("driver.pooling.local.requests")).isEqualTo(3);
    assertThat(result.getInt("driver.pooling.remote.connections")).isEqualTo(4);
    assertThat(result.getInt("driver.pooling.remote.requests")).isEqualTo(5);
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
    assertThat(result.getString("driver.auth.saslProtocol")).isEqualTo("sasl");
    assertThat(result.getString("driver.ssl.provider")).isEqualTo("myssl");
    assertThat(result.getStringList("driver.ssl.cipherSuites"))
        .isEqualTo(Collections.singletonList("TLS"));
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
    assertThat(result.getString("driver.policy.lbp.name")).isEqualTo("lbp");
    assertThat(result.getString("driver.policy.lbp.dse.childPolicy")).isEqualTo("dseChild");
    assertThat(result.getString("driver.policy.lbp.dcAwareRoundRobin.localDc"))
        .isEqualTo("localDc");
    assertThat(
            result.getBoolean(
                "driver.policy.lbp.dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel"))
        .isTrue();
    assertThat(result.getInt("driver.policy.lbp.dcAwareRoundRobin.usedHostsPerRemoteDc"))
        .isEqualTo(28);
    assertThat(result.getString("driver.policy.lbp.tokenAware.childPolicy"))
        .isEqualTo("tokenAwareChild");
    assertThat(result.getBoolean("driver.policy.lbp.tokenAware.shuffleReplicas")).isFalse();
    assertThat(result.getString("driver.policy.lbp.whiteList.childPolicy"))
        .isEqualTo("whiteListChild");
    assertThat(result.getString("driver.policy.lbp.whiteList.hosts")).isEqualTo("wh1, wh2");
    assertThat(result.getInt("driver.policy.maxRetries")).isEqualTo(29);
    assertThat(result.getString("batch.mode")).isEqualTo("batch-mode");
    assertThat(result.getInt("batch.bufferSize")).isEqualTo(9);
    assertThat(result.getInt("batch.maxBatchSize")).isEqualTo(10);
    assertThat(result.getInt("executor.maxThreads")).isEqualTo(11);
    assertThat(result.getInt("executor.maxConcurrentOps")).isEqualTo(27);
    assertThat(result.getInt("executor.maxInFlight")).isEqualTo(12);
    assertThat(result.getInt("executor.maxPerSecond")).isEqualTo(13);
    assertThat(result.getString("executor.continuousPaging.pageUnit")).isEqualTo("BYTES");
    assertThat(result.getInt("executor.continuousPaging.pageSize")).isEqualTo(14);
    assertThat(result.getInt("executor.continuousPaging.maxPages")).isEqualTo(15);
    assertThat(result.getInt("executor.continuousPaging.maxPagesPerSecond")).isEqualTo(16);
    assertThat(result.getString("log.directory")).isEqualTo("log-out");
    assertThat(result.getInt("log.maxThreads")).isEqualTo(17);
    assertThat(result.getInt("log.maxErrors")).isEqualTo(18);
    assertThat(result.getString("log.stmt.level")).isEqualTo("NORMAL");
    assertThat(result.getInt("log.stmt.maxQueryStringLength")).isEqualTo(19);
    assertThat(result.getInt("log.stmt.maxBoundValues")).isEqualTo(20);
    assertThat(result.getInt("log.stmt.maxBoundValueLength")).isEqualTo(21);
    assertThat(result.getInt("log.stmt.maxInnerStatements")).isEqualTo(22);
    assertThat(result.getString("codec.locale")).isEqualTo("locale");
    assertThat(result.getString("codec.timeZone")).isEqualTo("tz");
    assertThat(result.getStringList("codec.booleanWords")).isEqualTo(Arrays.asList("Si", "No"));
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
    assertThat(result.getString("schema.nullStrings")).isEqualTo("NIL, NADA");
    assertThat(result.getString("schema.nullToUnset")).isEqualTo("false");
    assertThat(result.getString("schema.mapping")).isEqualTo("{0:f1, 1:f2}");
    assertThat(result.getString("schema.recordMetadata")).isEqualTo("{0:f3, 1:f4}");
    assertThat(result.getString("connector.name")).isEqualTo("conn");
    assertThat(result.getInt("engine.maxMappingThreads")).isEqualTo(26);
  }

  @Test
  public void should_process_csv_long_options() throws Exception {
    Config result =
        Main.parseCommandLine(
            "csv",
            "load",
            new String[] {
              "--connector.csv.url", "url",
              "--connector.csv.fileNamePattern", "pat",
              "--connector.csv.fileNameFormat", "fmt",
              "--connector.csv.recursive", "true",
              "--connector.csv.maxThreads", "1",
              "--connector.csv.encoding", "enc",
              "--connector.csv.header", "false",
              "--connector.csv.delimiter", "|",
              "--connector.csv.quote", "'",
              "--connector.csv.escape", "*",
              "--connector.csv.comment", "#",
              "--connector.csv.skipLines", "2",
              "--connector.csv.maxLines", "3"
            });
    assertThat(result.getString("connector.csv.url")).isEqualTo("url");
    assertThat(result.getString("connector.csv.fileNamePattern")).isEqualTo("pat");
    assertThat(result.getString("connector.csv.fileNameFormat")).isEqualTo("fmt");
    assertThat(result.getBoolean("connector.csv.recursive")).isTrue();
    assertThat(result.getInt("connector.csv.maxThreads")).isEqualTo(1);
    assertThat(result.getString("connector.csv.encoding")).isEqualTo("enc");
    assertThat(result.getBoolean("connector.csv.header")).isFalse();
    assertThat(result.getString("connector.csv.delimiter")).isEqualTo("|");
    assertThat(result.getString("connector.csv.quote")).isEqualTo("'");
    assertThat(result.getString("connector.csv.escape")).isEqualTo("*");
    assertThat(result.getString("connector.csv.comment")).isEqualTo("#");
    assertThat(result.getInt("connector.csv.skipLines")).isEqualTo(2);
    assertThat(result.getInt("connector.csv.maxLines")).isEqualTo(3);
  }

  @Test
  public void should_show_version_message_when_asked() throws Exception {
    new Main(new String[] {"--version"}).run();
    String out = new String(stdout.toByteArray(), StandardCharsets.UTF_8);
    assertThat(out).isEqualTo(String.format("%s%n", HelpUtils.getVersionMessage()));
  }

  private void assertGlobalHelp() {
    String out = new String(stdout.toByteArray(), StandardCharsets.UTF_8);
    assertThat(out).contains(HelpUtils.getVersionMessage());
    assertThat(out).doesNotContain("First argument must be subcommand");
    assertThat(out).containsPattern("-f <string>\\s+Load settings from the given file");
  }
}
