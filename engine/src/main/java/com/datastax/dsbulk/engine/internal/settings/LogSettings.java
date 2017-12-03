/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.internal.utils.StringUtils.DELIMITER;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.FileAppender;
import com.datastax.driver.core.Cluster;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatter;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.ConfigException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/** */
public class LogSettings {

  public static final String OPERATION_DIRECTORY_KEY = "com.datastax.dsbulk.OPERATION_DIRECTORY";
  @VisibleForTesting public static final String PRODUCTION_KEY = "com.datastax.dsbulk.PRODUCTION";
  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettings.class);

  // Path Constants
  private static final String STMT = "stmt";
  private static final String MAX_QUERY_STRING_LENGTH = STMT + DELIMITER + "maxQueryStringLength";
  private static final String MAX_BOUND_VALUE_LENGTH = STMT + DELIMITER + "maxBoundValueLength";
  private static final String MAX_BOUND_VALUES = STMT + DELIMITER + "maxBoundValues";
  private static final String MAX_INNER_STATEMENTS = STMT + DELIMITER + "maxInnerStatements";
  private static final String LEVEL = STMT + DELIMITER + "level";
  private static final String MAX_ERRORS = "maxErrors";

  private final Path executionDirectory;
  private final int maxQueryStringLength;
  private final int maxBoundValueLength;
  private final int maxBoundValues;
  private final int maxInnerStatements;
  private final StatementFormatVerbosity level;
  private final int maxErrors;

  LogSettings(LoaderConfig config, String executionId) {
    try {
      executionDirectory = config.getPath("directory").resolve(executionId);
      System.setProperty(OPERATION_DIRECTORY_KEY, executionDirectory.toFile().getAbsolutePath());
      maxQueryStringLength = config.getInt(MAX_QUERY_STRING_LENGTH);
      maxBoundValueLength = config.getInt(MAX_BOUND_VALUE_LENGTH);
      maxBoundValues = config.getInt(MAX_BOUND_VALUES);
      maxInnerStatements = config.getInt(MAX_INNER_STATEMENTS);
      level = config.getEnum(StatementFormatVerbosity.class, LEVEL);
      maxErrors = config.getInt(MAX_ERRORS);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "log");
    }
  }

  public Path getExecutionDirectory() {
    return executionDirectory;
  }

  public void init(boolean writeToStandardOutput) {
    if (writeToStandardOutput) {
      redirectStandardOutputToStandardError();
    }
    maybeStartExecutionLogFileAppender();
    installJavaLoggingToSLF4JBridge();
  }

  public LogManager newLogManager(WorkflowType workflowType, Cluster cluster) {
    StatementFormatter formatter =
        StatementFormatter.builder()
            .withMaxQueryStringLength(maxQueryStringLength)
            .withMaxBoundValueLength(maxBoundValueLength)
            .withMaxBoundValues(maxBoundValues)
            .withMaxInnerStatements(maxInnerStatements)
            .build();
    return new LogManager(workflowType, cluster, executionDirectory, maxErrors, formatter, level);
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

  private void maybeStartExecutionLogFileAppender() {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    String production = lc.getProperty(PRODUCTION_KEY);
    if (production != null && production.equalsIgnoreCase("true")) {
      PatternLayoutEncoder ple = new PatternLayoutEncoder();
      ple.setPattern("%date{yyyy-MM-dd HH:mm:ss,UTC} %-5level %msg%n");
      ple.setContext(lc);
      ple.start();
      FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
      fileAppender.setFile(executionDirectory.resolve("operation.log").toFile().getAbsolutePath());
      fileAppender.setEncoder(ple);
      fileAppender.setContext(lc);
      fileAppender.setAppend(false);
      fileAppender.start();
      root.addAppender(fileAppender);
    }
  }

  private void installJavaLoggingToSLF4JBridge() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }
}
