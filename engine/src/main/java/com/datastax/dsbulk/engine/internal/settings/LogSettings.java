/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import com.datastax.driver.core.Cluster;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.ConfigException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class LogSettings implements SettingsValidator {

  public static final String PRODUCTION_KEY = "com.datastax.dsbulk.PRODUCTION";
  public static final String OPERATION_DIRECTORY_KEY = "com.datastax.dsbulk.OPERATION_DIRECTORY";

  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettings.class);

  private final LoaderConfig config;
  private final Path executionDirectory;

  LogSettings(LoaderConfig config, String executionId)
      throws MalformedURLException, URISyntaxException {
    this.config = config;
    executionDirectory = config.getPath("directory").resolve(executionId);
    LOGGER.info("Operation output directory: {}", executionDirectory);
    System.setProperty(OPERATION_DIRECTORY_KEY, executionDirectory.toFile().getAbsolutePath());
    maybeStartExecutionLogFileAppender();
  }

  public void validateConfig(WorkflowType type) throws BulkConfigurationException {
    try {
      config.getInt("stmt.maxQueryStringLength");
      config.getInt("stmt.maxBoundValueLength");
      config.getInt("stmt.maxBoundValues");
      config.getInt("stmt.maxInnerStatements");
      config.getEnum(StatementFormatVerbosity.class, "stmt.level");
      config.getInt("maxErrors");
      config.getThreads("maxThreads");
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "log");
    }
  }

  public LogManager newLogManager(Cluster cluster) {
    StatementFormatter formatter =
        StatementFormatter.builder()
            .withMaxQueryStringLength(config.getInt("stmt.maxQueryStringLength"))
            .withMaxBoundValueLength(config.getInt("stmt.maxBoundValueLength"))
            .withMaxBoundValues(config.getInt("stmt.maxBoundValues"))
            .withMaxInnerStatements(config.getInt("stmt.maxInnerStatements"))
            .build();
    StatementFormatVerbosity verbosity =
        config.getEnum(StatementFormatVerbosity.class, "stmt.level");
    int threads = config.getThreads("maxThreads");
    ExecutorService executor =
        Executors.newFixedThreadPool(
            threads, new ThreadFactoryBuilder().setNameFormat("log-manager-%d").build());
    return new LogManager(
        cluster, executionDirectory, executor, config.getInt("maxErrors"), formatter, verbosity);
  }

  private void maybeStartExecutionLogFileAppender() {
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    String production = lc.getProperty(PRODUCTION_KEY);
    if (production != null && production.equalsIgnoreCase("true")) {
      PatternLayoutEncoder ple = new PatternLayoutEncoder();
      ple.setPattern("%date{ISO8601,UTC} %-5level %msg%n");
      ple.setContext(lc);
      ple.start();
      FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
      fileAppender.setFile(executionDirectory.resolve("operation.log").toFile().getAbsolutePath());
      fileAppender.setEncoder(ple);
      fileAppender.setContext(lc);
      fileAppender.setAppend(false);
      fileAppender.start();
      ch.qos.logback.classic.Logger logger =
          (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
      logger.addAppender(fileAppender);
      logger.setLevel(Level.INFO);
    }
  }
}
