/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.commons.internal.logging.StreamType.STDERR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.event.Level.ERROR;

import com.datastax.dsbulk.commons.internal.logging.LogCapture;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptor;
import com.datastax.dsbulk.commons.internal.logging.StreamCapture;
import com.datastax.dsbulk.commons.internal.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.StreamInterceptor;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
class SettingsValidatorTest {

  private static final String[] BAD_PARAMS_WRONG_TYPE = {
    "--connector.csv.recursive=tralse",
    "--log.stmt.maxQueryStringLength=NotANumber",
    "--log.stmt.maxBoundValueLength=NotANumber",
    "--log.stmt.maxBoundValues=NotANumber",
    "--log.stmt.maxInnerStatements=NotANumber",
    "--driver.port=notANumber",
    "--driver.pooling.local.connections=NotANumber",
    "--driver.pooling.remote.connections=notANumber",
    "--driver.pooling.local.requests=NotANumber",
    "--driver.pooling.remote.requests=notANumber",
    "--driver.query.fetchSize=NotANumber",
    "--driver.query.idempotence=notAboolean",
    "--driver.timestampGenerator=badInstance",
    "--driver.addressTranslator=badINstance",
    "--driver.policy.maxRetries=notANumber",
    "--connector.csv.skipRecords=notANumber",
    "--connector.csv.maxRecords=notANumber",
    "--connector.csv.recursive=tralse",
    "--connector.csv.header=tralse",
    "--connector.csv.encoding=1",
    "--connector.csv.delimiter=",
    "--connector.csv.quote=",
    "--connector.csv.escape=",
    "--connector.csv.comment=",
    "--connector.csv.maxConcurrentFiles=notANumber",
    "--schema.nullToUnset=tralse",
    "--batch.maxBatchSize=NotANumber",
    "--batch.bufferSize=NotANumber",
    "--executor.maxPerSecond=NotANumber",
    "--executor.maxInFlight=NotANumber",
    "--monitoring.expectedWrites=NotANumber",
    "--monitoring.expectedWrites=expectedReads",
    "--monitoring.jmx=tralse"
  };

  private static final String[] BAD_ENUM = {
    "--log.stmt.level=badValue",
    "--driver.protocol.compression=badValue",
    "--driver.query.consistency=badValue",
    "--driver.query.serialConsistency=badValue",
    "--batch.mode=badEnum",
    "--monitoring.rateUnit=badValue",
    "--monitoring.durationUnit=badValue",
  };

  private static final String[] BAD_DURATION = {
    "--driver.socket.readTimeout=badValue",
    "--driver.pooling.heartbeat=badValue",
    "--monitoring.reportRate=NotANumber",
  };

  private final LogInterceptor logs;
  private final StreamInterceptor stdErr;

  public SettingsValidatorTest(
      @LogCapture(value = Main.class, level = ERROR) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {
    this.logs = logs;
    this.stdErr = stdErr;
  }

  @Test
  void should_error_on_bad_arguments() {
    for (String argument : Arrays.asList(BAD_PARAMS_WRONG_TYPE)) {
      int starting_index = argument.indexOf("-", argument.indexOf("-argument") + 1) + 2;
      int ending_index = argument.indexOf("=");
      String argPrefix = argument.substring(starting_index, ending_index);
      new Main(
              new String[] {
                "load",
                "--schema.keyspace=keyspace",
                "--schema.table=table",
                "--connector.csv.url=/path/to/file",
                argument
              })
          .run();
      String err = logs.getAllMessagesAsString();
      assertThat(err).contains(argPrefix + " has type");
      assertThat(err).contains(" rather than");
      logs.clear();
    }

    // These errors will look like this; String: 1: Invalid value at 'protocol.compression':
    // The enum class Compression has no constant of the name 'badValue' (should be one of [NONE,
    // SNAPPY, LZ4].
    for (String argument : Arrays.asList(BAD_ENUM)) {
      new Main(
              new String[] {
                "load",
                "--schema.keyspace=keyspace",
                "--schema.table=table",
                "--connector.csv.url=127.0.1.1",
                argument
              })
          .run();
      String err = logs.getAllMessagesAsString();
      assertThat(err).contains("Invalid value at");
      assertThat(err).contains("The enum class");
      logs.clear();
    }

    // These errors will look like this; String: 1: Invalid value at 'socket.readTimeout': No number
    // in duration value 'badValue'
    for (String argument : Arrays.asList(BAD_DURATION)) {
      new Main(
              new String[] {
                "load",
                "--schema.keyspace=keyspace",
                "--schema.table=table",
                "--connector.csv.url=127.0.1.1",
                argument
              })
          .run();
      String err = logs.getAllMessagesAsString();
      assertThat(err).contains("Invalid value at");
      assertThat(err).contains(" No number in duration value");
      logs.clear();
    }
  }

  @Test
  void should_error_bad_connector() {
    new Main(new String[] {"load", "-c", "BadConnector"}).run();
    assertThat(stdErr.getStreamAsString())
        .contains(logs.getLoggedMessages())
        .contains("Cannot find connector");
  }

  @Test
  void should_error_on_empty_hosts() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "--schema.query",
              "INSERT",
              "--driver.hosts",
              ""
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "driver.hosts is mandatory. Please set driver.hosts and try again. "
                + "See settings.md or help for more information");
  }

  @Test
  void should_error_on_empty_url() {
    new Main(new String[] {"load", "--driver.hosts=hostshere", "--connector.csv.url", ""}).run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "url is mandatory when using the csv connector. Please set connector.csv.url and "
                + "try again. See settings.md or help for more information.");
  }

  @Test
  void should_error_bad_parse_driver_option() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url",
              "/path/to/file",
              "--schema.query",
              "INSERT",
              "--driver.socket.readTimeout",
              "I am not a duration"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("Invalid value at 'socket.readTimeout'");
  }

  @Test
  void should_error_invalid_auth_provider() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url",
              "/path/to/file",
              "--schema.query",
              "INSERT",
              "--driver.auth.provider",
              "InvalidAuthProvider"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("InvalidAuthProvider is not a valid auth provider");
  }

  @Test
  void should_error_invalid_auth_combinations_missing_principal() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url",
              "/path/to/file",
              "--schema.query",
              "INSERT",
              "--driver.auth.provider=DseGSSAPIAuthProvider",
              "--driver.auth.principal",
              ""
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("must be provided with auth.principal");
  }

  @Test
  void should_error_invalid_auth_combinations_missing_username() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "--schema.query",
              "INSERT",
              "--driver.auth.provider=PlainTextAuthProvider",
              "--driver.auth.username",
              ""
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("must be provided with both auth.username and auth.password");
  }

  @Test
  void should_error_invalid_auth_combinations_missing_password() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "--schema.query=INSERT",
              "--driver.auth.provider=DsePlainTextAuthProvider",
              "--driver.auth.password",
              ""
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("must be provided with both auth.username and auth.password");
  }

  @Test
  void should_error_invalid_schema_settings() {
    new Main(new String[] {"load", "--connector.csv.url=/path/to/my/file"}).run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("schema.mapping, or schema.keyspace and schema.table must be defined");
  }

  @Test
  void should_error_invalid_schema_query_with_ttl() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "--schema.query=xyz",
              "--schema.queryTtl=30",
              "--schema.keyspace=keyspace",
              "--schema.table=table"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "schema.query must not be defined if schema.queryTtl or schema.queryTimestamp "
                + "is defined");
  }

  @Test
  void should_error_invalid_schema_query_with_timestamp() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "--schema.query=xyz",
              "--schema.queryTimestamp=9876123",
              "--schema.keyspace=keyspace",
              "--schema.table=table"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "schema.query must not be defined if schema.queryTtl or schema.queryTimestamp "
                + "is defined");
  }

  @Test
  void should_error_invalid_schema_query_with_mapped_timestamp() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "--schema.query=xyz",
              "--schema.mapping",
              "f1=__timestamp",
              "--schema.keyspace=keyspace",
              "--schema.table=table"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains("schema.query must not be defined when mapping a field to query-timestamp");
  }

  @Test
  void should_error_invalid_schema_query_with_keyspace_and_table() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "--schema.query=xyz",
              "--schema.mapping",
              "f1=__ttl",
              "--schema.keyspace=keyspace",
              "--schema.table=table"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("schema.query must not be defined when mapping a field to query-ttl");
  }

  @Test
  void should_error_invalid_timestamp() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "--schema.queryTimestamp=junk",
              "--schema.keyspace=keyspace",
              "--schema.table=table"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("Could not parse schema.queryTimestamp 'junk'");
  }

  @Test
  void should_connect_error_with_schema_defined() {
    new Main(
            new String[] {
              "load",
              "--driver.hosts=255.255.255.23",
              "--connector.csv.url=/path/to/my/file",
              "--schema.keyspace=keyspace",
              "--schema.table=table"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains(" All host(s) tried for query");
  }

  @Test
  void should_error_invalid_schema_missing_keyspace() {
    new Main(new String[] {"load", "--connector.csv.url=/path/to/my/file", "--schema.table=table"})
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("schema.keyspace must accompany schema.table in the configuration");
  }

  @Test
  void should_error_invalid_schema_mapping_missing_keyspace_and_table() {
    new Main(new String[] {"load", "--connector.csv.url=/path/to/my/file", "-m", "c1=c2"}).run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("schema.query, or schema.keyspace and schema.table must be defined");
  }

  @Test
  void should_connect_error_with_schema_mapping_query_defined() {
    new Main(
            new String[] {
              "load",
              "--driver.hosts=255.255.255.23",
              "--connector.csv.url=/path/to/my/file",
              "-m",
              "c1=c2",
              "--schema.query",
              "INSERT INTO KEYSPACE (f1, f2) VALUES (:f1, :f2)"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains(" All host(s) tried for query");
  }

  @Test
  void should_error_invalid_schema_inferred_mapping_query_defined() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "-m",
              "*=*, c1=c2",
              "--schema.query",
              "INSERT INTO KEYSPACE (f1, f2) VALUES (:f1, :f2)"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains("schema.keyspace and schema.table must be defined when using inferred mapping");
  }

  @Test
  void should_error_unknown_lbp() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "-m",
              "c1=c2",
              "--driver.policy.lbp.name",
              "unknown",
              "--schema.query",
              "INSERT INTO KEYSPACE (f1, f2) VALUES (:f1, :f2)"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("Invalid value at 'policy.lbp.name'");
  }

  @Test
  void should_error_lbp_bad_child_policy() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "-m",
              "c1=c2",
              "--driver.policy.lbp.name",
              "dse",
              "--driver.policy.lbp.dse.childPolicy",
              "junk",
              "--schema.query",
              "INSERT INTO KEYSPACE (f1, f2) VALUES (:f1, :f2)"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("Invalid value at 'dse.childPolicy'");
  }

  @Test
  void should_error_lbp_chaining_loop_self() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "-m",
              "c1=c2",
              "--driver.policy.lbp.name",
              "dse",
              "--driver.policy.lbp.dse.childPolicy",
              "dse",
              "--schema.query",
              "INSERT INTO KEYSPACE (f1, f2) VALUES (:f1, :f2)"
            })
        .run();

    String err = logs.getAllMessagesAsString();
    assertThat(err).contains("Load balancing policy chaining loop detected: dse,dse");
  }

  @Test
  void should_error_lbp_chaining_loop() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "-m",
              "c1=c2",
              "--driver.policy.lbp.name",
              "dse",
              "--driver.policy.lbp.dse.childPolicy",
              "whiteList",
              "--driver.policy.lbp.whiteList.childPolicy",
              "tokenAware",
              "--driver.policy.lbp.tokenAware.childPolicy",
              "whiteList",
              "--schema.query",
              "INSERT INTO KEYSPACE (f1, f2) VALUES (:f1, :f2)"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "Load balancing policy chaining loop detected: dse,whiteList,tokenAware,whiteList");
  }
}
