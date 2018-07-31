/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.nio.file.Files.createTempDirectory;
import static org.slf4j.event.Level.ERROR;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@Tag("medium")
class CountEndToEndCCMITBase extends EndToEndCCMITBase {

  private final LogInterceptor logs;
  private final StreamInterceptor stderr;
  private Path logDir;
  private Path unloadDir;

  CountEndToEndCCMITBase(
      CCMCluster ccm,
      Session session,
      @LogCapture(level = ERROR) LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    super(ccm, session);
    this.logs = logs;
    this.stderr = stderr;
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
    deleteDirectory(unloadDir);
  }

  @BeforeEach
  void clearLogs() {
    logs.clear();
    stderr.clear();
  }

  @Test
  void count_custom_query_invalid_missing_pk() {

    session.execute("DROP TABLE IF EXISTS count_invalid1");
    session.execute(
        "CREATE TABLE IF NOT EXISTS count_invalid1 "
            + "(\"PK1\" int, \"PK2\" int, \"CC\" int, \"V\" int, "
            + "PRIMARY KEY ((\"PK1\", \"PK2\"), \"CC\"))");

    List<String> args = new ArrayList<>();
    args.add("count");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("-stats");
    args.add("partitions");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("\"SELECT \\\"PK1\\\" FROM count_invalid1\"");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    String expected = "Missing required partition key column \"PK2\" from schema.query";
    assertThat(logs).hasMessageContaining(expected);
    assertThat(stderr.getStreamAsString()).contains(expected);
  }

  @Test
  void count_custom_query_invalid_extraneous_column() {

    session.execute("DROP TABLE IF EXISTS count_invalid2");
    session.execute(
        "CREATE TABLE IF NOT EXISTS count_invalid2 "
            + "(\"PK\" int, \"CC\" int, \"V\" int, "
            + "PRIMARY KEY (\"PK\", \"CC\"))");

    List<String> args = new ArrayList<>();
    args.add("count");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("-stats");
    args.add("partitions");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.query");
    args.add("\"SELECT \\\"PK\\\",\\\"CC\\\",\\\"V\\\" FROM count_invalid2\"");

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_FATAL_ERROR);
    String expected =
        "The provided statement (schema.query) contains extraneous columns in the SELECT clause: \"CC\", \"V\"; it should contain only partition key columns.";
    assertThat(logs).hasMessageContaining(expected);
    assertThat(stderr.getStreamAsString()).contains(expected);
  }
}
