/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SettingValidatorTest {
  private PrintStream originalStderr;
  private PrintStream originalStdout;
  private ByteArrayOutputStream stderr;
  private ByteArrayOutputStream stdout;

  private final String[] BADPARAMSWRONGTYPE = {
    "--connector.csv.recursive=tralse",
    "--log.stmt.maxQueryStringLength=NotANumber",
    "--log.stmt.maxBoundValueLength=NotANumber",
    "--log.stmt.maxBoundValues=NotANumber",
    "--log.stmt.maxInnerStatements=NotANumber",
    "--log.maxThreads=badValue",
    "--driver.port=notANumber",
    "--driver.pooling.local.connections=NotANumber",
    "--driver.pooling.remote.connections=notANumber",
    "--driver.pooling.local.requests=NotANumber",
    "--driver.pooling.remote.requests=notANumber",
    "--driver.query.fetchSize=NotANumber",
    "--driver.query.idempotence=notAboolean",
    "--driver.timestampGenerator=badInstance",
    "--driver.addressTranslator=badINstance",
    "--connector.csv.skipLines=notANumber",
    "--connector.csv.maxLines=notANumber",
    "--connector.csv.recursive=tralse",
    "--connector.csv.header=tralse",
    "--connector.csv.encoding=1",
    "--connector.csv.delimiter=",
    "--connector.csv.quote=",
    "--connector.csv.escape=",
    "--connector.csv.comment=",
    "--connector.csv.maxThreads=notANumber",
    "--schema.nullToUnset=tralse",
    "--batch.maxBatchSize=NotANumber",
    "--batch.bufferSize=NotANumber",
    "--executor.maxPerSecond=NotANumber",
    "--executor.maxInFlight=NotANumber",
    "--executor.maxThreads=NotANumber",
    "--monitoring.expectedWrites=NotANumber",
    "--monitoring.expectedWrites=expectedReads",
    "--monitoring.jmx=tralse",
    "--engine.maxMappingThreads=NotANumber",
    "--engine.maxConcurrentReads=NotANumber"
  };

  private final String[] BADENUM = {
    "--log.stmt.level=badValue",
    "--driver.protocol.compression=badValue",
    "--driver.query.consistency=badValue",
    "--driver.query.serialConsistency=badValue",
    "--batch.mode=badEnum",
    "--monitoring.rateUnit=badValue",
    "--monitoring.durationUnit=badValue",
  };

  private final String[] BADDURATION = {
    "--driver.socket.readTimeout=badValue",
    "--driver.pooling.heartbeat=badValue",
    "--monitoring.reportRate=NotANumber",
  };

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
  }

  @Test
  public void should_error_on_bad_arguments() {
    for (String arugment : Arrays.asList(BADPARAMSWRONGTYPE)) {
      int starting_index = arugment.indexOf("-", arugment.indexOf("-arugment") + 1) + 2;
      int ending_index = arugment.indexOf("=");
      String argPrefix = arugment.substring(starting_index, ending_index);
      new Main(
          new String[] {
            "load",
            "--schema.keyspace=keyspace",
            "--schema.table=table",
            "--connector.csv.url=127.0.1.1",
            arugment
          });
      String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
      assertThat(err).contains(argPrefix + " has type");
      assertThat(err).contains(" rather than");
      stderr = new ByteArrayOutputStream();
      System.setErr(new PrintStream(stderr));
    }
    // These errors will look like this; String: 1: Invalid value at 'protocol.compression':
    // The enum class Compression has no constant of the name 'badValue' (should be one of [NONE, SNAPPY, LZ4].
    for (String arugment : Arrays.asList(BADENUM)) {
      new Main(
          new String[] {
            "load",
            "--schema.keyspace=keyspace",
            "--schema.table=table",
            "--connector.csv.url=127.0.1.1",
            arugment
          });
      String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
      assertThat(err).contains("Invalid value at");
      assertThat(err).contains("The enum class");
      stderr = new ByteArrayOutputStream();
      System.setErr(new PrintStream(stderr));
    }
    // These Errors will look like this; String: 1: Invalid value at 'socket.readTimeout': No number in duration value
    // 'badValue
    for (String arugment : Arrays.asList(BADDURATION)) {
      int starting_index = arugment.indexOf("-", arugment.indexOf("-arugment") + 1) + 2;
      int ending_index = arugment.indexOf("=");
      String argPrefix = arugment.substring(starting_index, ending_index);
      new Main(
          new String[] {
            "load",
            "--schema.keyspace=keyspace",
            "--schema.table=table",
            "--connector.csv.url=127.0.1.1",
            arugment
          });
      String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
      assertThat(err).contains("Invalid value at");
      assertThat(err).contains(" No number in duration value");
      stderr = new ByteArrayOutputStream();
      System.setErr(new PrintStream(stderr));
    }
  }

  @Test
  public void should_error_bad_connector() throws Exception {
    new Main(new String[] {"load", "-c", "BadConnector"});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("Cannot find connector");
  }

  @Test
  public void should_error_on_empty_hosts() throws Exception {
    new Main(new String[] {"load", "--driver.hosts", ""});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err)
        .contains(
            "driver.hosts is mandatory. Please set driver.hosts and try again. See settings.md or help for more information");
  }

  @Test
  public void should_error_on_empty_url() throws Exception {
    new Main(new String[] {"load", "--driver.hosts=hostshere", "--connector.csv.url", ""});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err)
        .contains(
            "url is mandatory when using the csv connector. Please set connector.csv.url and try again. See settings.md or help for more information.");
  }

  @Test
  public void should_error_bad_parse_driver_option() throws Exception {
    new Main(new String[] {"load", "--driver.socket.readTimeout", "I am not a duration"});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("Invalid value at 'socket.readTimeout'");
  }

  @Test
  public void should_error_invalid_auth_provider() throws Exception {
    new Main(new String[] {"load", "--driver.auth.provider", "InvalidAuthProvider"});
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
    new Main(new String[] {"load", "--connector.csv.url=/path/to/my/file"});
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains("schema.mapping, or schema.keyspace and schema.table must be defined");
  }

  @Test
  public void should_connect_error_with_schema_defined() throws Exception {
    new Main(
        new String[] {
          "load",
          "--driver.hosts=255.255.255.23",
          "--connector.csv.url=/path/to/my/file",
          "--schema.keyspace=keyspace",
          "--schema.table=table"
        });
    String err = new String(stderr.toByteArray(), StandardCharsets.UTF_8);
    assertThat(err).contains(" All host(s) tried for query");
  }
}
