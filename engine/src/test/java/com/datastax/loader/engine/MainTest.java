/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

package com.datastax.loader.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.typesafe.config.Config;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MainTest {
  private PrintStream originalStderr;
  private ByteArrayOutputStream baos;

  @Before
  public void setUp() throws Exception {
    originalStderr = System.err;

    baos = new ByteArrayOutputStream();
    System.setErr(new PrintStream(baos));
  }

  @After
  public void tearDown() throws Exception {
    System.setErr(originalStderr);
  }

  @Test
  public void should_show_help_with_error_when_no_args() throws Exception {
    new Main(new String[] {});
    String err = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("First argument must be subcommand");
  }

  @Test
  public void should_show_help_without_error_when_help_arg() throws Exception {
    new Main(new String[] {"--help"});
    String err = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).doesNotContain("First argument must be subcommand");
  }

  @Test
  public void should_show_help_with_error_when_junk_subcommand() throws Exception {
    new Main(new String[] {"junk"});
    String err = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("First argument must be subcommand");
  }

  @Test
  public void should_show_help_without_error_when_junk_subcommand_and_help() throws Exception {
    new Main(new String[] {"junk", "--help"});
    String err = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).doesNotContain("First argument must be subcommand");
  }

  @Test
  public void should_show_help_without_error_when_good_subcommand_and_help() throws Exception {
    new Main(new String[] {"load", "--help"});
    String err = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).doesNotContain("First argument must be subcommand");
  }

  @Test
  public void should_show_help_without_error_when_no_subcommand_and_help() throws Exception {
    new Main(new String[] {"-k", "k1", "--help"});
    String err = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).doesNotContain("First argument must be subcommand");
  }

  @Test
  public void should_show_help_with_short_opts_when_name_set() throws Exception {
    new Main(new String[] {"-c", "csv", "--help"});
    String err = new String(baos.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err)
        .doesNotContain("First argument must be subcommand")
        .contains("-url,--connector.csv.url");
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
              "-h", "[\"host\"]",
              "-lbp", "lbp",
              "-retry", "retry",
              "-port", "9876",
              "-cl", "cl",
              "-sslKeystorePw", "sslpass",
              "-maxErrors", "123",
              "-logDir", "logdir",
              "-jmx", "false",
              "-reportRate", "456",
              "-k", "ks",
              "-m", "{0:\"f1\", 1:\"f2\"}",
              "-nullStrings", "[nil, nada]",
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
    assertThat(result.getStringList("driver.hosts")).isEqualTo(Collections.singletonList("host"));
    assertThat(result.getString("driver.policy.lbp")).isEqualTo("lbp");
    assertThat(result.getString("driver.policy.retry")).isEqualTo("retry");
    assertThat(result.getInt("driver.port")).isEqualTo(9876);
    assertThat(result.getString("driver.query.consistency")).isEqualTo("cl");
    assertThat(result.getString("driver.ssl.keystore.password")).isEqualTo("sslpass");
    assertThat(result.getInt("log.maxErrors")).isEqualTo(123);
    assertThat(result.getString("log.outputDirectory")).isEqualTo("logdir");
    assertThat(result.getBoolean("monitoring.jmx")).isFalse();
    assertThat(result.getInt("monitoring.reportRate")).isEqualTo(456);
    assertThat(result.getString("schema.keyspace")).isEqualTo("ks");

    Map<String, Object> mapping = result.getConfig("schema.mapping").root().unwrapped();
    assertThat(mapping).contains(entry("0", "f1"), entry("1", "f2"));
    assertThat(result.getStringList("schema.nullStrings")).containsOnly("nil", "nada");
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
}
