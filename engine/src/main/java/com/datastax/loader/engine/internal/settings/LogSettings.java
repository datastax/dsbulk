/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.loader.engine.internal.log.LogManager;
import com.datastax.loader.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.loader.engine.internal.log.statement.StatementFormatter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class LogSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettings.class);
  private static final String OPERATION_DIRECTORY_MDC_KEY =
      "com.datastax.loader.OPERATION_DIRECTORY";

  private final Config config;
  private final Path operationDirectory;

  public LogSettings(Config config, String operationId)
      throws MalformedURLException, URISyntaxException {
    this.config = config;
    Path outputDirectory = SettingsUtils.parseAbsolutePath(config.getString("output-directory"));
    operationDirectory = outputDirectory.resolve(operationId);
    System.setProperty(OPERATION_DIRECTORY_MDC_KEY, operationDirectory.toFile().getAbsolutePath());
    LOGGER.info("Operation output directory: {}", operationDirectory);
  }

  public LogManager newLogManager() {
    StatementFormatter formatter =
        StatementFormatter.builder()
            .withMaxQueryStringLength(config.getInt("stmt.max-query-string-length"))
            .withMaxBoundValueLength(config.getInt("stmt.max-bound-value-length"))
            .withMaxBoundValues(config.getInt("stmt.max-bound-values"))
            .withMaxInnerStatements(config.getInt("stmt.max-inner-statements"))
            .build();
    StatementFormatVerbosity verbosity =
        config.getEnum(StatementFormatVerbosity.class, "stmt.verbosity");
    int threads = SettingsUtils.parseNumThreads(config.getString("max-threads"));
    ExecutorService executor =
        Executors.newFixedThreadPool(
            threads, new ThreadFactoryBuilder().setNameFormat("log-manager-%d").build());
    return new LogManager(
        operationDirectory, executor, config.getInt("max-errors"), formatter, verbosity);
  }
}
