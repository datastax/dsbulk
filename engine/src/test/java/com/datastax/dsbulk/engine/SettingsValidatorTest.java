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
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.event.Level.ERROR;

import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

  private Path tempFolder;

  public SettingsValidatorTest(
      @LogCapture(value = Main.class, level = ERROR) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {
    this.logs = logs;
    this.stdErr = stdErr;
  }

  @BeforeEach
  void createTempFolder() throws IOException {
    tempFolder = Files.createTempDirectory("test");
  }

  @AfterEach
  void deleteTempFolder() {
    deleteDirectory(tempFolder);
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
      assertThat(err).contains(argPrefix);
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
      assertThat(err).contains("Expecting one of");
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
    assertThat(err)
        .contains(
            "schema.mapping, schema.query, or schema.keyspace and schema.table must be defined");
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
              "f1=__timestamp"
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
              "f1=__timestamp",
              "--schema.keyspace=keyspace",
              "--schema.table=table"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "schema.query must not be defined if schema.keyspace and schema.table are defined");
  }

  @Test
  void should_error_invalid_schema_query_with_mapped_ttl() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "--schema.query=xyz",
              "--schema.mapping",
              "f1=__ttl"
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
    new Main(new String[] {"load", "--connector.csv.url=/path/to/my/file", "-m", "*=*, c1=c2"})
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "schema.query, or schema.keyspace and schema.table must be defined when using inferred mapping");
  }

  @Test
  void should_error_invalid_schema_query_present_and_function_present() {
    new Main(
            new String[] {
              "load", "--connector.csv.url=/path/to/my/file", "-m", "now() = c1", "-query", "INSERT"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains("schema.query must not be defined when mapping a function to a column");
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

  @Test
  void should_error_nonexistent_keytab() {
    new Main(
            new String[] {
              "load",
              "--driver.auth.provider",
              "DseGSSAPIAuthProvider",
              "--driver.auth.keyTab",
              "noexist.keytab"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*Keytab file .*noexist.keytab does not exist.*");
  }

  @Test
  void should_error_keytab_is_a_dir() {
    new Main(
            new String[] {
              "load", "--driver.auth.provider", "DseGSSAPIAuthProvider", "--driver.auth.keyTab", "."
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*Keytab file .* is not a file.*");
  }

  @Test
  void should_error_keytab_has_no_keys() throws Exception {
    Path keytabPath = Files.createTempFile(tempFolder, "my", ".keytab");
    new Main(
            new String[] {
              "load",
              "--driver.auth.provider",
              "DseGSSAPIAuthProvider",
              "--driver.auth.keyTab",
              escapeUserInput(keytabPath.toString())
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*Could not find any principals in.*");
  }

  @Test
  void should_error_DseGSSAPIAuthProvider_and_no_sasl_protocol() {
    new Main(
            new String[] {
              "load",
              "--connector.csv.url=/path/to/my/file",
              "-m",
              "c1=c2",
              "--driver.auth.provider",
              "DseGSSAPIAuthProvider",
              "--driver.auth.saslService",
              "",
              "--schema.query",
              "INSERT INTO KEYSPACE (f1, f2) VALUES (:f1, :f2)"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "DseGSSAPIAuthProvider must be provided with auth.saslService. "
                + "auth.principal, auth.keyTab, and auth.authorizationId are optional.");
  }

  @Test
  void should_error_keystore_without_password() throws IOException {
    Path keystore = Files.createTempFile(tempFolder, "my", "keystore");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "JDK",
              "--driver.ssl.keystore.path",
              escapeUserInput(keystore.toString())
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "ssl.keystore.path, ssl.keystore.password and ssl.truststore.algorithm must be provided together");
  }

  @Test
  void should_error_password_without_keystore() {
    new Main(
            new String[] {
              "load", "--driver.ssl.provider", "JDK", "--driver.ssl.keystore.password", "mypass"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "ssl.keystore.path, ssl.keystore.password and ssl.truststore.algorithm must be provided together");
  }

  @Test
  void should_error_openssl_keycertchain_without_key() throws IOException {
    Path chain = Files.createTempFile(tempFolder, "my", ".chain");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "OpenSSL",
              "--driver.ssl.openssl.keyCertChain",
              escapeUserInput(chain.toString()),
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains("ssl.openssl.keyCertChain and ssl.openssl.privateKey must be provided together");
  }

  @Test
  void should_error_key_without_openssl_keycertchain() throws IOException {
    Path key = Files.createTempFile(tempFolder, "my", ".key");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "OpenSSL",
              "--driver.ssl.openssl.privateKey",
              escapeUserInput(key.toString()),
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains("ssl.openssl.keyCertChain and ssl.openssl.privateKey must be provided together");
  }

  @Test
  void should_error_truststore_without_password() throws IOException {
    Path truststore = Files.createTempFile(tempFolder, "my", "truststore");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "JDK",
              "--driver.ssl.truststore.path",
              escapeUserInput(truststore.toString())
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "ssl.truststore.path, ssl.truststore.password and ssl.truststore.algorithm must be provided");
  }

  @Test
  void should_error_password_without_truststore() {
    new Main(
            new String[] {
              "load", "--driver.ssl.provider", "JDK", "--driver.ssl.truststore.password", "mypass"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .contains(
            "ssl.truststore.path, ssl.truststore.password and ssl.truststore.algorithm must be provided together");
  }

  @Test
  void should_error_nonexistent_truststore() {
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "JDK",
              "--driver.ssl.truststore.path",
              "noexist.truststore",
              "--driver.ssl.truststore.password",
              "mypass"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*SSL truststore file .*noexist.truststore does not exist.*");
  }

  @Test
  void should_error_truststore_is_a_dir() {
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "JDK",
              "--driver.ssl.truststore.path",
              ".",
              "--driver.ssl.truststore.password",
              "mypass"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*SSL truststore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_truststore() throws IOException {
    Path truststore = Files.createTempFile(tempFolder, "my", ".truststore");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.truststore.path",
              escapeUserInput(truststore.toString()),
              "--driver.ssl.truststore.password",
              "mypass"
            })
        .run();
    String err = logs.getAllMessagesAsString();

    // So, success in this case is that we *didn't* error out on the setting under test,
    // but rather fail on a missing option.
    assertThat(err)
        .contains(
            "schema.mapping, schema.query, or schema.keyspace and schema.table must be defined");
  }

  @Test
  void should_error_nonexistent_keystore() {
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "JDK",
              "--driver.ssl.keystore.path",
              "noexist.keystore",
              "--driver.ssl.keystore.password",
              "mypass"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*SSL keystore file .*noexist.keystore does not exist.*");
  }

  @Test
  void should_error_keystore_is_a_dir() {
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "JDK",
              "--driver.ssl.keystore.path",
              ".",
              "--driver.ssl.keystore.password",
              "mypass"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*SSL keystore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_keystore() throws IOException {
    Path keystore = Files.createTempFile(tempFolder, "my", ".keystore");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.keystore.path",
              escapeUserInput(keystore.toString()),
              "--driver.ssl.keystore.password",
              "mypass"
            })
        .run();
    String err = logs.getAllMessagesAsString();

    // So, success in this case is that we *didn't* error out on the setting under test,
    // but rather fail on a missing option.
    assertThat(err)
        .contains(
            "schema.mapping, schema.query, or schema.keyspace and schema.table must be defined");
  }

  @Test
  void should_error_nonexistent_openssl_keycertchain() throws IOException {
    Path key = Files.createTempFile(tempFolder, "my", ".key");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "OpenSSL",
              "--driver.ssl.openssl.keyCertChain",
              "noexist.chain",
              "--driver.ssl.openssl.privateKey",
              escapeUserInput(key.toString())
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err)
        .matches(".*OpenSSL key certificate chain file .*noexist.chain does not exist.*");
  }

  @Test
  void should_error_openssl_keycertchain_is_a_dir() throws IOException {
    Path key = Files.createTempFile(tempFolder, "my", ".key");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "OpenSSL",
              "--driver.ssl.openssl.keyCertChain",
              ".",
              "--driver.ssl.openssl.privateKey",
              escapeUserInput(key.toString())
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*OpenSSL key certificate chain file .* is not a file.*");
  }

  @Test
  void should_accept_existing_openssl_keycertchain_and_key() throws IOException {
    Path key = Files.createTempFile(tempFolder, "my", ".key");
    Path chain = Files.createTempFile(tempFolder, "my", ".chain");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "OpenSSL",
              "--driver.ssl.openssl.keyCertChain",
              escapeUserInput(chain.toString()),
              "--driver.ssl.openssl.privateKey",
              escapeUserInput(key.toString())
            })
        .run();
    String err = logs.getAllMessagesAsString();

    // So, success in this case is that we *didn't* error out on the setting under test,
    // but rather fail on a missing option.
    assertThat(err)
        .contains(
            "schema.mapping, schema.query, or schema.keyspace and schema.table must be defined");
  }

  @Test
  void should_error_nonexistent_openssl_key() throws IOException {
    Path chain = Files.createTempFile(tempFolder, "my", ".chain");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "OpenSSL",
              "--driver.ssl.openssl.keyCertChain",
              escapeUserInput(chain.toString()),
              "--driver.ssl.openssl.privateKey",
              "noexist.key"
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*OpenSSL private key file .*noexist.key does not exist.*");
  }

  @Test
  void should_error_openssl_key_is_a_dir() throws IOException {
    Path chain = Files.createTempFile(tempFolder, "my", ".chain");
    new Main(
            new String[] {
              "load",
              "--driver.ssl.provider",
              "OpenSSL",
              "--driver.ssl.openssl.keyCertChain",
              escapeUserInput(chain.toString()),
              "--driver.ssl.openssl.privateKey",
              "."
            })
        .run();
    String err = logs.getAllMessagesAsString();
    assertThat(err).matches(".*OpenSSL private key file .* is not a file.*");
  }
}
