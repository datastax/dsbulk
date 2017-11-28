/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.internal.settings.LogSettings.PRODUCTION_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.google.common.base.Charsets;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
class LogSettingsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettingsTest.class);

  private Cluster cluster;

  @SuppressWarnings("Duplicates")
  @BeforeEach
  void setUp() throws Exception {
    cluster = mock(Cluster.class);
    Configuration configuration = mock(Configuration.class);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
    when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Test
  void should_create_log_manager_with_default_output_directory() throws Exception {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.log"));
    LogSettings settings = new LogSettings(config, "test");
    settings.init(false);
    LogManager logManager = settings.newLogManager(WorkflowType.LOAD, cluster);
    try {
      logManager.init();
      assertThat(logManager).isNotNull();
      assertThat(logManager.getExecutionDirectory().toFile().getAbsolutePath())
          .isEqualTo(Paths.get("./logs/test").normalize().toFile().getAbsolutePath());
    } finally {
      //noinspection ResultOfMethodCallIgnored
      Files.walk(logManager.getExecutionDirectory().getParent())
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  @Test
  void should_create_log_manager_when_output_directory_path_provided() throws Exception {
    Path dir = Files.createTempDirectory("test");
    String logDir = ConfigUtils.maybeEscapeBackslash(dir.toString());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + logDir + "\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "test");
    settings.init(false);
    LogManager logManager = settings.newLogManager(WorkflowType.LOAD, cluster);
    logManager.init();
    assertThat(logManager).isNotNull();
    assertThat(logManager.getExecutionDirectory().toFile()).isEqualTo(dir.resolve("test").toFile());
  }

  @Test
  void should_create_log_file_when_in_production() throws Exception {
    Path dir = Files.createTempDirectory("test");
    String logDir = ConfigUtils.maybeEscapeBackslash(dir.toString());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + logDir + "\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    lc.putProperty(PRODUCTION_KEY, "true");
    Level oldLevel = root.getLevel();
    try {
      LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
      settings.init(false);
      root.setLevel(Level.ERROR);
      LOGGER.error("this is a test");
      Path logFile = dir.resolve("TEST_EXECUTION_ID").resolve("operation.log");
      assertThat(logFile).exists();
      String contents = Files.readAllLines(logFile).stream().collect(Collectors.joining());
      assertThat(contents).contains("this is a test");
    } finally {
      lc.putProperty(PRODUCTION_KEY, "false");
      root.setLevel(oldLevel);
    }
  }

  @Test
  public void should_not_create_log_file_when_not_in_production() throws Exception {
    Path dir = Files.createTempDirectory("test");
    String logDir = ConfigUtils.maybeEscapeBackslash(dir.toString());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + logDir + "\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    settings.init(false);
    LOGGER.error("this is a test");
    Path logFile = dir.resolve("TEST_EXECUTION_ID").resolve("operation.log");
    assertThat(logFile).doesNotExist();
  }

  @Test
  public void should_redirect_standard_output_when_in_production() throws Exception {
    Path dir = Files.createTempDirectory("test");
    String logDir = ConfigUtils.maybeEscapeBackslash(dir.toString());
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + logDir + "\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    lc.putProperty(PRODUCTION_KEY, "true");
    PrintStream originalStdout = System.out;
    PrintStream originalStderr = System.err;
    ByteArrayOutputStream baosOut = new ByteArrayOutputStream();
    ByteArrayOutputStream baosErr = new ByteArrayOutputStream();
    Level oldLevel = root.getLevel();
    try {
      System.setOut(new PrintStream(baosOut));
      System.setErr(new PrintStream(baosErr));
      settings.init(true);
      // info level would normally be printed to stdout, but has been redirected to
      // stderr
      root.setLevel(Level.INFO);
      LOGGER.info("你好");
      assertThat(baosOut.size()).isZero();
      assertThat(new String(baosErr.toByteArray(), "UTF-8")).contains("你好");
      Path logFile = dir.resolve("TEST_EXECUTION_ID").resolve("operation.log");
      assertThat(logFile).exists();
      String contents =
          Files.readAllLines(logFile, Charsets.UTF_8).stream().collect(Collectors.joining());
      assertThat(contents).contains("你好");
    } finally {
      System.setOut(originalStdout);
      System.setErr(originalStderr);
      lc.putProperty(PRODUCTION_KEY, "false");
      root.setLevel(oldLevel);
    }
  }
}
