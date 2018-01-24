/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.internal.utils.StringUtils.DELIMITER;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.FileAppender;
import com.datastax.driver.core.Cluster;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatter;
import com.typesafe.config.ConfigException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/** */
public class LogSettings {

  public static final String OPERATION_DIRECTORY_KEY = "com.datastax.dsbulk.OPERATION_DIRECTORY";
  public static final String MAIN_LOG_FILE_APPENDER = "MAIN_LOG_FILE_APPENDER";
  public static final String PRODUCTION_KEY = "com.datastax.dsbulk.PRODUCTION";

  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettings.class);

  // Path Constants
  private static final String STMT = "stmt";
  private static final String MAX_QUERY_STRING_LENGTH = STMT + DELIMITER + "maxQueryStringLength";
  private static final String MAX_BOUND_VALUE_LENGTH = STMT + DELIMITER + "maxBoundValueLength";
  private static final String MAX_BOUND_VALUES = STMT + DELIMITER + "maxBoundValues";
  private static final String MAX_INNER_STATEMENTS = STMT + DELIMITER + "maxInnerStatements";
  private static final String LEVEL = STMT + DELIMITER + "level";
  private static final String MAX_ERRORS = "maxErrors";

  private final LoaderConfig config;
  private final String executionId;

  private Path executionDirectory;
  private int maxQueryStringLength;
  private int maxBoundValueLength;
  private int maxBoundValues;
  private int maxInnerStatements;
  private StatementFormatVerbosity level;
  private int maxErrors;
  private float maxErrorsRatio;

  LogSettings(LoaderConfig config, String executionId) {
    this.config = config;
    this.executionId = executionId;
  }

  public void init(boolean writeToStandardOutput) throws IOException {
    try {
      executionDirectory = config.getPath("directory").resolve(executionId);
      checkExecutionDirectory();
      System.setProperty(OPERATION_DIRECTORY_KEY, executionDirectory.toFile().getAbsolutePath());
      maxQueryStringLength = config.getInt(MAX_QUERY_STRING_LENGTH);
      maxBoundValueLength = config.getInt(MAX_BOUND_VALUE_LENGTH);
      maxBoundValues = config.getInt(MAX_BOUND_VALUES);
      maxInnerStatements = config.getInt(MAX_INNER_STATEMENTS);
      level = config.getEnum(StatementFormatVerbosity.class, LEVEL);
      String maxErrorString = config.getString(MAX_ERRORS);
      if (isPercent(maxErrorString)) {
        maxErrorsRatio = Float.parseFloat(maxErrorString.replaceAll("\\s*%", "")) / 100f;
        validatePercentageRange(maxErrorsRatio);
        maxErrors = 0;
      } else {
        maxErrors = config.getInt(MAX_ERRORS);
        maxErrorsRatio = 0;
      }
      if (writeToStandardOutput) {
        redirectStandardOutputToStandardError();
      }
      maybeStartExecutionLogFileAppender();
      installJavaLoggingToSLF4JBridge();
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "log");
    }
  }

  public LogManager newLogManager(WorkflowType workflowType, Cluster cluster) {
    StatementFormatter formatter =
        StatementFormatter.builder()
            .withMaxQueryStringLength(maxQueryStringLength)
            .withMaxBoundValueLength(maxBoundValueLength)
            .withMaxBoundValues(maxBoundValues)
            .withMaxInnerStatements(maxInnerStatements)
            .build();
    return new LogManager(
        workflowType, cluster, executionDirectory, maxErrors, maxErrorsRatio, formatter, level);
  }

  Path getExecutionDirectory() {
    return executionDirectory;
  }

  private void checkExecutionDirectory() throws IOException {
    if (Files.exists(executionDirectory)) {
      if (Files.isDirectory(executionDirectory)) {
        if (Files.isWritable(executionDirectory)) {
          if (Files.list(executionDirectory).count() > 0) {
            throw new IllegalArgumentException(
                "Execution directory exists but is not empty: " + executionDirectory);
          }
        } else {
          throw new IllegalArgumentException(
              "Execution directory exists but is not writable: " + executionDirectory);
        }
      } else {
        throw new IllegalArgumentException(
            "Execution directory exists but is not a directory: " + executionDirectory);
      }
    } else {
      Files.createDirectories(executionDirectory);
    }
  }

  private void redirectStandardOutputToStandardError() {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    String production = lc.getProperty(PRODUCTION_KEY);
    if (production != null && production.equalsIgnoreCase("true")) {
      root.detachAppender("STDOUT");
      Appender<ILoggingEvent> stderr = root.getAppender("STDERR");
      stderr.clearAllFilters();
    }
    LOGGER.info("Standard output is reserved, log messages are redirected to standard error.");
  }

  private boolean isPercent(String maxErrors) {
    return maxErrors.contains("%");
  }

  private void maybeStartExecutionLogFileAppender() {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    String production = lc.getProperty(PRODUCTION_KEY);
    if (production != null && production.equalsIgnoreCase("true")) {
      PatternLayoutEncoder ple = new PatternLayoutEncoder();
      ple.setPattern("%date{yyyy-MM-dd HH:mm:ss,UTC} %-5level %msg%n");
      ple.setContext(lc);
      ple.setCharset(StandardCharsets.UTF_8);
      ple.start();
      FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
      fileAppender.setName(MAIN_LOG_FILE_APPENDER);
      fileAppender.setFile(executionDirectory.resolve("operation.log").toFile().getAbsolutePath());
      fileAppender.setEncoder(ple);
      fileAppender.setContext(lc);
      fileAppender.setAppend(false);
      fileAppender.start();
      root.addAppender(fileAppender);
    }
  }

  private void validatePercentageRange(float maxErrorRatio) {
    if (maxErrorRatio <= 0 || maxErrorRatio >= 1) {
      throw new BulkConfigurationException(
          "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");
    }
  }

  private void installJavaLoggingToSLF4JBridge() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }
}
