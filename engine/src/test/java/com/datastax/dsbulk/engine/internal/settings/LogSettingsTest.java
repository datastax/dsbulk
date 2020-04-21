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
import static com.datastax.dsbulk.commons.tests.utils.TestConfigUtils.createTestConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.tests.driver.DriverUtils;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.log.threshold.AbsoluteErrorThreshold;
import com.datastax.dsbulk.engine.internal.log.threshold.ErrorThreshold;
import com.datastax.dsbulk.engine.internal.log.threshold.RatioErrorThreshold;
import com.datastax.dsbulk.engine.internal.log.threshold.UnlimitedErrorThreshold;
import com.datastax.dsbulk.engine.tests.utils.LogUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.typesafe.config.Config;
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

  private CqlSession session;

  private Path tempFolder;

  @BeforeEach
  void setUp() {
    session = DriverUtils.mockSession();
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
    Config config = createTestConfig("dsbulk.log");
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    try (LogManager logManager = settings.newLogManager(WorkflowType.LOAD, session)) {
      logManager.init();
      assertThat(logManager).isNotNull();
      assertThat(logManager.getOperationDirectory().toFile().getAbsolutePath())
          .isEqualTo(Paths.get("./target/logs/test").normalize().toFile().getAbsolutePath());
    }
  }

  @Test()
  void should_accept_maxErrors_as_absolute_number() throws IOException {
    Config config = createTestConfig("dsbulk.log", "maxErrors", 20);
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    ErrorThreshold threshold = settings.errorThreshold;
    assertThat(threshold).isInstanceOf(AbsoluteErrorThreshold.class);
    assertThat(((AbsoluteErrorThreshold) threshold).getMaxErrors()).isEqualTo(20);
  }

  @Test()
  void should_accept_maxErrors_as_percentage() throws IOException {
    Config config = createTestConfig("dsbulk.log", "maxErrors", "20%");
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    ErrorThreshold threshold = settings.errorThreshold;
    assertThat(threshold).isInstanceOf(RatioErrorThreshold.class);
    assertThat(((RatioErrorThreshold) threshold).getMaxErrorRatio()).isEqualTo(0.2f);
    // min sample is fixed and cannot be changed by the user currently
    assertThat(((RatioErrorThreshold) threshold).getMinSample()).isEqualTo(100);
  }

  @Test()
  void should_disable_maxErrors() throws IOException {
    Config config = createTestConfig("dsbulk.log", "maxErrors", -42);
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    ErrorThreshold threshold = settings.errorThreshold;
    assertThat(threshold).isInstanceOf(UnlimitedErrorThreshold.class);
  }

  @Test()
  void should_error_when_percentage_is_out_of_bounds() {
    Config config = createTestConfig("dsbulk.log", "maxErrors", "112 %");
    LogSettings settings = new LogSettings(config, "test");
    assertThatThrownBy(settings::init)
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");

    config = createTestConfig("dsbulk.log", "maxErrors", "0%");

    LogSettings settings2 = new LogSettings(config, "test");
    assertThatThrownBy(settings2::init)
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");

    config = createTestConfig("dsbulk.log", "maxErrors", "-1%");

    LogSettings settings3 = new LogSettings(config, "test");
    assertThatThrownBy(settings3::init)
        .hasMessage(
            "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");
  }

  @Test
  void should_create_log_manager_when_output_directory_path_provided() throws Exception {
    Config config = createTestConfig("dsbulk.log", "directory", quoteJson(tempFolder));
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    try (LogManager logManager = settings.newLogManager(WorkflowType.LOAD, session)) {
      logManager.init();
      assertThat(logManager).isNotNull();
      assertThat(logManager.getOperationDirectory().toFile())
          .isEqualTo(tempFolder.resolve("test").toFile());
    }
  }

  @Test
  void should_log_to_main_log_file_in_normal_mode() throws Exception {
    Config config = createTestConfig("dsbulk.log", "directory", quoteJson(tempFolder));
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
      LoggerFactory.getLogger("com.datastax.oss.driver").warn("this is a test 3");
      LoggerFactory.getLogger("com.datastax.oss.driver").info("this should not appear");
      LoggerFactory.getLogger("com.datastax.dse.driver").info("this should not appear");
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
    Config config =
        createTestConfig("dsbulk.log", "directory", quoteJson(tempFolder), "verbosity", 0);
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
    Config config =
        createTestConfig("dsbulk.log", "directory", quoteJson(tempFolder), "verbosity", 2);
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
  void should_throw_exception_when_maxQueryStringLength_not_a_number() {
    Config config = createTestConfig("dsbulk.log", "stmt.maxQueryStringLength", "NotANumber");
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.log.stmt.maxQueryStringLength, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxBoundValueLength_not_a_number() {
    Config config = createTestConfig("dsbulk.log", "stmt.maxBoundValueLength", "NotANumber");
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.log.stmt.maxBoundValueLength, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxBoundValues_not_a_number() {
    Config config = createTestConfig("dsbulk.log", "stmt.maxBoundValues", "NotANumber");
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.log.stmt.maxBoundValues, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxResultSetValues_not_a_number() {
    Config config = createTestConfig("dsbulk.log", "row.maxResultSetValues", "NotANumber");
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.log.row.maxResultSetValues, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxResultSetValueLength_not_a_number() {
    Config config = createTestConfig("dsbulk.log", "row.maxResultSetValueLength", "NotANumber");
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.log.row.maxResultSetValueLength, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_maxInnerStatements_not_a_number() {
    Config config = createTestConfig("dsbulk.log", "stmt.maxInnerStatements", "NotANumber");
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.log.stmt.maxInnerStatements, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_level_invalid() {
    Config config = createTestConfig("dsbulk.log", "stmt.level", "NotALevel");
    LogSettings settings = new LogSettings(config, "TEST_EXECUTION_ID");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.log.stmt.level, expecting one of ABRIDGED, NORMAL, EXTENDED, got: 'NotALevel'");
  }

  @Test()
  void should_accept_maxQueryWarnings_as_absolute_number() throws IOException {
    Config config = createTestConfig("dsbulk.log", "maxQueryWarnings", 20);
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    ErrorThreshold threshold = settings.queryWarningsThreshold;
    assertThat(threshold).isInstanceOf(AbsoluteErrorThreshold.class);
    assertThat(((AbsoluteErrorThreshold) threshold).getMaxErrors()).isEqualTo(20);
  }

  @Test()
  void should_not_accept_maxQueryWarnings_as_percentage() {
    Config config = createTestConfig("dsbulk.log", "maxQueryWarnings", "20%");
    LogSettings settings = new LogSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.log.maxQueryWarnings, expecting NUMBER, got STRING");
  }

  @Test()
  void should_disable_maxQueryWarnings() throws IOException {
    Config config = createTestConfig("dsbulk.log", "maxQueryWarnings", -42);
    LogSettings settings = new LogSettings(config, "test");
    settings.init();
    ErrorThreshold threshold = settings.queryWarningsThreshold;
    assertThat(threshold).isInstanceOf(UnlimitedErrorThreshold.class);
  }
}
