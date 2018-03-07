/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDOUT;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static com.datastax.dsbulk.engine.internal.settings.LogSettings.MAIN_LOG_FILE_APPENDER;
import static com.datastax.dsbulk.engine.internal.settings.LogSettings.PRODUCTION_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumingThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(StreamInterceptingExtension.class)
class LogSettingsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettingsTest.class);

  private Cluster cluster;

  private Path tempFolder;

  @SuppressWarnings("Duplicates")
  @BeforeEach
  void setUp() {
    cluster = mock(Cluster.class);
    Configuration configuration = mock(Configuration.class);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
    when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
  }

  @BeforeEach
  void createTempFolder() throws IOException {
    tempFolder = Files.createTempDirectory("test");
    Files.createDirectories(tempFolder);
  }

  @AfterEach
  void deleteTempFolder() {
    deleteDirectory(tempFolder);
    deleteDirectory(Paths.get("./logs"));
  }

  @AfterEach
  void removeMainLogFileAppender() {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    FileAppender<ILoggingEvent> appender =
        (FileAppender<ILoggingEvent>) root.getAppender(MAIN_LOG_FILE_APPENDER);
    if (appender != null) {
      appender.stop();
      root.detachAppender(appender);
    }
  }

  @Test
  void should_create_log_manager_with_default_output_directory() throws Exception {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.log"));
    LogSettings settings = new LogSettings(config, "test");
    settings.init(false);
    try (LogManager logManager = settings.newLogManager(WorkflowType.LOAD, cluster)) {
      logManager.init();
      assertThat(logManager).isNotNull();
      assertThat(logManager.getExecutionDirectory().toFile().getAbsolutePath())
          .isEqualTo(Paths.get("./logs/test").normalize().toFile().getAbsolutePath());
    }
  }

  @Test()
  void should_error_when_percentage_is_out_of_bounds() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxErrors = 112 %")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "test");
    assertThatThrownBy(() -> settings.init(false))
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");

    config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxErrors = -1%")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));

    LogSettings settings2 = new LogSettings(config, "test");
    assertThatThrownBy(() -> settings2.init(false))
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");
  }

  @Test
  void should_create_log_manager_when_output_directory_path_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + escapeUserInput(tempFolder) + "\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "test");
    settings.init(false);
    try (LogManager logManager = settings.newLogManager(WorkflowType.LOAD, cluster)) {
      logManager.init();
      assertThat(logManager).isNotNull();
      assertThat(logManager.getExecutionDirectory().toFile())
          .isEqualTo(tempFolder.resolve("test").toFile());
    }
  }

  @Test
  void should_create_log_file_when_in_production() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + escapeUserInput(tempFolder) + "\"")
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
      Path logFile = tempFolder.resolve("TEST_EXECUTION_ID").resolve("operation.log");
      assertThat(logFile).exists();
      String contents = Files.readAllLines(logFile).stream().collect(Collectors.joining());
      assertThat(contents).contains("this is a test");
    } finally {
      lc.putProperty(PRODUCTION_KEY, "false");
      root.setLevel(oldLevel);
    }
  }

  @Test
  void should_not_create_log_file_when_not_in_production() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + escapeUserInput(tempFolder) + "\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    settings.init(false);
    LOGGER.error("this is a test");
    Path logFile = tempFolder.resolve("TEST_EXECUTION_ID").resolve("operation.log");
    assertThat(logFile).doesNotExist();
  }

  @Test
  void should_redirect_standard_output_when_in_production(
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr)
      throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + escapeUserInput(tempFolder) + "\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    lc.putProperty(PRODUCTION_KEY, "true");
    Level oldLevel = root.getLevel();
    try {
      settings.init(true);
      // info level would normally be printed to stdout, but has been redirected to
      // stderr
      root.setLevel(Level.INFO);
      LOGGER.info("你好");
      assertThat(stdOut.getStreamAsString()).isEmpty();
      assertThat(stdErr.getStreamAsString()).contains("你好");
      Path logFile = tempFolder.resolve("TEST_EXECUTION_ID").resolve("operation.log");
      assertThat(logFile).exists();
      String contents = FileUtils.readFile(logFile, UTF_8);
      assertThat(contents).contains("你好");
    } finally {
      lc.putProperty(PRODUCTION_KEY, "false");
      root.setLevel(oldLevel);
    }
  }

  @Test
  void should_throw_IAE_when_execution_directory_not_empty() throws Exception {
    Path executionDir = tempFolder.resolve("TEST_EXECUTION_ID");
    Path foo = executionDir.resolve("foo");
    Files.createDirectories(foo);
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + escapeUserInput(tempFolder) + "\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(() -> settings.init(false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Execution directory exists but is not empty: " + executionDir);
  }

  @Test
  void should_throw_IAE_when_execution_directory_not_writable() {
    assumingThat(
        !PlatformUtils.isWindows(),
        () -> {
          Path executionDir = tempFolder.resolve("TEST_EXECUTION_ID");
          Files.createDirectories(executionDir);
          assertThat(executionDir.toFile().setWritable(false, false)).isTrue();
          LoaderConfig config =
              new DefaultLoaderConfig(
                  ConfigFactory.parseString("directory = \"" + escapeUserInput(tempFolder) + "\"")
                      .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
          LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
          assertThatThrownBy(() -> settings.init(false))
              .isInstanceOf(IllegalArgumentException.class)
              .hasMessage("Execution directory exists but is not writable: " + executionDir);
        });
  }

  @Test
  void should_throw_IAE_when_execution_directory_not_directory() throws Exception {
    Path executionDir = tempFolder.resolve("TEST_EXECUTION_ID");
    Files.createFile(executionDir);
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = \"" + escapeUserInput(tempFolder) + "\"")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(() -> settings.init(false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Execution directory exists but is not a directory: " + executionDir);
  }

  @Test
  void should_throw_IAE_when_execution_directory_contains_forbidden_chars() {
    assumingThat(
        !PlatformUtils.isWindows(),
        () -> {
          LoaderConfig config =
              new DefaultLoaderConfig(
                  ConfigFactory.parseString("directory = \"" + escapeUserInput(tempFolder) + "\"")
                      .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
          char forbidden = '/';
          LogSettings settings = new LogSettings(config, forbidden + " IS FORBIDDEN");
          assertThatThrownBy(() -> settings.init(false))
              .isInstanceOf(IOException.class)
              .hasMessageContaining(forbidden + " IS FORBIDDEN");
        });
  }
}
