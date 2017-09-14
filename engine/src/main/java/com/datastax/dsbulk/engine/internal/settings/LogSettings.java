/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.dsbulk.commons.config.ConfigUtils;
import com.datastax.dsbulk.commons.config.LoaderConfig;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettings.class);
  public static final String OPERATION_DIRECTORY_KEY = "com.datastax.dsbulk.OPERATION_DIRECTORY";

  private final LoaderConfig config;
  private final Path executionDirectory;

  LogSettings(LoaderConfig config, String executionId)
      throws MalformedURLException, URISyntaxException {
    this.config = config;
    Path directory = config.getPath("directory");
    executionDirectory = directory.resolve(executionId);
    System.setProperty(OPERATION_DIRECTORY_KEY, executionDirectory.toFile().getAbsolutePath());
    LOGGER.info("Operation output directory: {}", executionDirectory);
  }

  public void validateConfig(WorkflowType type) throws IllegalArgumentException {
    try {
      config.getInt("stmt.maxQueryStringLength");
      config.getInt("stmt.maxBoundValueLength");
      config.getInt("stmt.maxBoundValues");
      config.getInt("stmt.maxInnerStatements");
      config.getEnum(StatementFormatVerbosity.class, "stmt.level");
      config.getInt("maxErrors");
      config.getThreads("maxThreads");
    } catch (ConfigException e) {
      ConfigUtils.badConfigToIllegalArgument(e, "log");
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
}
