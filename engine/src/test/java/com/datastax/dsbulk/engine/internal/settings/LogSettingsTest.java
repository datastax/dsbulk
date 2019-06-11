/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumingThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.tests.utils.LogUtils;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    deleteDirectory(Paths.get("./target/logs"));
  }

  @BeforeEach
  @AfterEach
  void resetLogbackConfiguration() throws JoranException {
    LogUtils.resetLogbackConfiguration();
  }

  @Test
  void should_create_log_manager_with_default_output_directory() throws Exception {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.log"));
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    try (LogManager logManager = settings.newLogManager(WorkflowType.LOAD, cluster)) {
      logManager.init();
      assertThat(logManager).isNotNull();
      assertThat(logManager.getExecutionDirectory().toFile().getAbsolutePath())
          .isEqualTo(Paths.get("./target/logs/test").normalize().toFile().getAbsolutePath());
    }
  }

  @Test()
  void should_accept_maxErrors_as_absolute_number() throws IOException {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxErrors = 20")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    assertThat(settings.getMaxErrors()).isEqualTo(20);
    assertThat(settings.getMaxErrorsRatio()).isEqualTo(-1);
  }

  @Test()
  void should_accept_maxErrors_as_percentage() throws IOException {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxErrors = 20%")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    assertThat(settings.getMaxErrors()).isEqualTo(-1);
    assertThat(settings.getMaxErrorsRatio()).isEqualTo(0.2f);
  }

  @Test()
  void should_disable_maxErrors() throws IOException {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxErrors = -42")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    assertThat(settings.getMaxErrors()).isEqualTo(-42);
    assertThat(settings.getMaxErrorsRatio()).isEqualTo(-1);
  }

  @Test()
  void should_error_when_percentage_is_out_of_bounds() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxErrors = 112 %")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "test");
    assertThatThrownBy(settings::init)
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");

    config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxErrors = 0%")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));

    LogSettings settings2 = new LogSettings(config, "test");
    assertThatThrownBy(settings2::init)
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");

    config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxErrors = -1%")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));

    LogSettings settings3 = new LogSettings(config, "test");
    assertThatThrownBy(settings3::init)
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");
  }

  @Test
  void should_create_log_manager_when_output_directory_path_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = " + quoteJson(tempFolder))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    try (LogManager logManager = settings.newLogManager(WorkflowType.LOAD, cluster)) {
      logManager.init();
      assertThat(logManager).isNotNull();
      assertThat(logManager.getExecutionDirectory().toFile())
          .isEqualTo(tempFolder.resolve("test").toFile());
    }
  }

  @Test
  void should_log_to_main_log_file_in_normal_mode() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = " + quoteJson(tempFolder))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    Level oldLevel = root.getLevel();
    try {
      LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
      settings.init();
      assertThat(root.getLevel()).isEqualTo(Level.INFO);
      root.info("this is a test 1");
      root.debug("this should not appear");
      LOGGER.info("this is a test 2");
      LOGGER.debug("this should not appear");
      // driver log level should be WARN
      LoggerFactory.getLogger("com.datastax.driver").warn("this is a test 3");
      LoggerFactory.getLogger("com.datastax.driver").info("this should not appear");
      Path logFile = tempFolder.resolve("TEST_EXECUTION_ID").resolve("operation.log");
      assertThat(logFile).exists();
      String contents = String.join("", Files.readAllLines(logFile));
      assertThat(contents)
          .contains("this is a test 1", "this is a test 2", "this is a test 3")
          .doesNotContain("this should not appear");
    } finally {
      root.setLevel(oldLevel);
    }
  }

  @Test
  void should_log_to_main_log_file_in_quiet_mode() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = " + quoteJson(tempFolder) + ", verbosity = 0")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    settings.init();
    ch.qos.logback.classic.Logger dsbulkLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.datastax.dsbulk");
    dsbulkLogger.warn("this is a test 1");
    dsbulkLogger.info("this should not appear");
    LOGGER.warn("this is a test 2");
    LOGGER.info("this should not appear");
    Logger driverLogger = LoggerFactory.getLogger("com.datastax.driver");
    driverLogger.warn("this is a test 3");
    driverLogger.info("this should not appear");
    Path logFile = tempFolder.resolve("TEST_EXECUTION_ID").resolve("operation.log");
    assertThat(logFile).exists();
    String contents = String.join("", Files.readAllLines(logFile));
    assertThat(contents)
        .contains("this is a test 1", "this is a test 2", "this is a test 3")
        .doesNotContain("this should not appear");
  }

  @Test
  void should_log_to_main_log_file_in_verbose_mode() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = " + quoteJson(tempFolder) + ", verbosity = 2")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    settings.init();
    ch.qos.logback.classic.Logger dsbulkLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.datastax.dsbulk");
    assertThat(dsbulkLogger.getLevel()).isEqualTo(Level.DEBUG);
    dsbulkLogger.debug("this is a test 1");
    LOGGER.debug("this is a test 2");
    // driver log level should now be INFO
    LoggerFactory.getLogger("com.datastax.driver").info("this is a test 3");
    LoggerFactory.getLogger("com.datastax.driver").debug("this should not appear");
    Path logFile = tempFolder.resolve("TEST_EXECUTION_ID").resolve("operation.log");
    assertThat(logFile).exists();
    String contents = String.join("", Files.readAllLines(logFile));
    assertThat(contents)
        .contains("this is a test 1", "this is a test 2", "this is a test 3")
        .doesNotContain("this should not appear");
  }

  @Test
  void should_throw_IAE_when_execution_directory_not_empty() throws Exception {
    Path executionDir = tempFolder.resolve("TEST_EXECUTION_ID");
    Path foo = executionDir.resolve("foo");
    Files.createDirectories(foo);
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("directory = " + quoteJson(tempFolder))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
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
                  ConfigFactory.parseString("directory = " + quoteJson(tempFolder))
                      .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
          LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
          assertThatThrownBy(settings::init)
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
            ConfigFactory.parseString("directory = " + quoteJson(tempFolder))
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
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
                  ConfigFactory.parseString("directory = " + quoteJson(tempFolder))
                      .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
          char forbidden = '/';
          LogSettings settings = new LogSettings(config, forbidden + " IS FORBIDDEN");
          assertThatThrownBy(settings::init)
              .isInstanceOf(IOException.class)
              .hasMessageContaining(forbidden + " IS FORBIDDEN");
        });
  }

  @Test
  void should_throw_exception_when_maxQueryStringLength_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("stmt.maxQueryStringLength = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for log.stmt.maxQueryStringLength: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxBoundValueLength_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("stmt.maxBoundValueLength = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for log.stmt.maxBoundValueLength: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxBoundValues_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("stmt.maxBoundValues = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for log.stmt.maxBoundValues: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxResultSetValues_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("row.maxResultSetValues = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for log.row.maxResultSetValues: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxResultSetValueLength_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("row.maxResultSetValueLength = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for log.row.maxResultSetValueLength: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxInnerStatements_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("stmt.maxInnerStatements = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for log.stmt.maxInnerStatements: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_level_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("stmt.level = NotALevel")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.log")));
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'stmt.level': Expecting one of ABRIDGED, NORMAL, EXTENDED, got 'NotALevel'");
  }
}
