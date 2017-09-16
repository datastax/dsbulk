/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.engine.internal.OptionUtils;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SettingValidatorTest {
  private PrintStream originalStderr;
  private PrintStream originalStdout;
  private ByteArrayOutputStream stderr;
  private ByteArrayOutputStream stdout;

  @Before
  public void setUp() throws Exception {
    originalStdout = System.out;
    originalStderr = System.err;

    stdout = new ByteArrayOutputStream();
    System.setOut(new PrintStream(stdout));

    stderr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(stderr));
  }

  @After
  public void tearDown() throws Exception {
    System.setOut(originalStdout);
    System.setErr(originalStderr);
    System.clearProperty("config.file");
    ConfigFactory.invalidateCaches();
    OptionUtils.DEFAULT = ConfigFactory.load().getConfig("dsbulk");
  }

  @Test
  public void should_error_bad_connector() throws Exception {
    new Main(new String[] {"load", "-c", "BadConnector"});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("Cannot find connector");
  }

  @Test
  public void should_error_bad_type_connector_option() throws Exception {
    new Main(new String[] {"load", "--connector.csv.recursive", "tralse"});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("Configuration entry of");
    assertThat(err).contains("recursive has type STRING rather than BOOLEAN");
  }

  @Test
  public void should_error_bad_parse_driver_option() throws Exception {
    new Main(new String[] {"load", "--driver.socket.readTimeout", "I am not a duration"});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("Invalid value at 'socket.readTimeout'");
  }

  @Test
  public void should_error_invalid_auth_provider() throws Exception {
    new Main(new String[] {"load", "--driver.auth.provide", "InvalidAuthProvider"});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("InvalidAuthProvider is not a valid auth provider");
  }

  @Test
  public void should_error_invalid_auth_combinations_missing_principal() throws Exception {
    new Main(
        new String[] {
          "load", "--driver.auth.provider=DseGSSAPIAuthProvider", "--driver.auth.principal", ""
        });
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("must be provided with auth.principal");
  }

  @Test
  public void should_error_invalid_auth_combinations_missing_username() throws Exception {
    new Main(
        new String[] {
          "load", "--driver.auth.provider=PlainTextAuthProvider", "--driver.auth.username", ""
        });
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("must be provided with both auth.username and auth.password");
  }

  @Test
  public void should_error_invalid_auth_combinations_missing_password() throws Exception {
    new Main(
        new String[] {
          "load", "--driver.auth.provider=DsePlainTextAuthProvider", "--driver.auth.password", ""
        });
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("must be provided with both auth.username and auth.password");
  }

  @Test
  public void should_error_invalid_schema_settings() throws Exception {
    new Main(new String[] {"load"});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("schema.mapping, or schema.keyspace and schema.table must be defined");
  }

  @Test
  public void should_connect_error_with_schema_defined() throws Exception {
    new Main(new String[] {"load", "--schema.keyspace=keyspace", "--schema.table=table"});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains(" All host(s) tried for query");
  }

  @Test
  public void should_error_bad_type_batch_option() throws Exception {
    new Main(
        new String[] {
          "load",
          "--schema.keyspace=keyspace",
          "--schema.table=table",
          "--batch.bufferSize=notanumber"
        });
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("batch.bufferSize has type STRING rather than NUMBER");
  }

  @Test
  public void should_error_bad_type_executor_option() throws Exception {
    new Main(
        new String[] {
          "load",
          "--schema.keyspace=keyspace",
          "--schema.table=table",
          "--executor.maxInFlight=notanumber"
        });
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("executor.maxInFlight has type STRING rather than NUMBER");
  }

  @Test
  public void should_error_bad_type_monitor_option() throws Exception {
    new Main(
        new String[] {
          "load",
          "--schema.keyspace=keyspace",
          "--schema.table=table",
          "--monitoring.jmx=notaboolean"
        });
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("monitoring.jmx has type STRING rather than BOOLEAN");
  }
}
