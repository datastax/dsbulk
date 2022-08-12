/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.workflow.commons.settings;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.filter.Filter;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.format.row.RowFormatter;
import com.datastax.oss.dsbulk.format.statement.StatementFormatVerbosity;
import com.datastax.oss.dsbulk.format.statement.StatementFormatter;
import com.datastax.oss.dsbulk.workflow.api.error.ErrorThreshold;
import com.datastax.oss.dsbulk.workflow.api.log.OperationDirectory;
import com.datastax.oss.dsbulk.workflow.api.log.OperationDirectoryResolver;
import com.datastax.oss.dsbulk.workflow.api.utils.WorkflowUtils;
import com.datastax.oss.dsbulk.workflow.commons.log.LogManager;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.CheckpointManager;
import com.datastax.oss.dsbulk.workflow.commons.log.checkpoint.ReplayStrategy;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedBoundStatementPrinter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class LogSettings {

  public enum Verbosity {
    quiet,
    normal,
    high,
    max
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettings.class);
  private static final String CONSOLE_APPENDER = "CONSOLE";

  private static final int MIN_SAMPLE = 100;

  private static final String MAIN_LOG_FILE_APPENDER = "FILE";
  private static final String MAIN_LOG_FILE_NAME = "operation.log";

  /** The options for stack trace printing. */
  public static final ImmutableList<String> STACK_TRACE_PRINTER_OPTIONS =
      ImmutableList.of(
          // number of stack elements to print
          "10",
          // packages to exclude from stack traces
          "reactor.core",
          "com.datastax.oss.driver.shaded",
          "io.netty",
          "java.util.concurrent");

  /**
   * The layout pattern to use for the main log file.
   *
   * <p>Example of formatted message:
   *
   * <pre>
   * 2018-03-09 14:41:02 ERROR Unload workflow engine execution UNLOAD_20180309-144101-093338 aborted: Invalid table
   * com.datastax.driver.core.exceptions.SyntaxError: Invalid table
   *     at com.datastax.driver.core.exceptions.SyntaxError.copy(SyntaxError.java:49)
   * 	   ...
   * </pre>
   */
  private static final String LAYOUT_PATTERN =
      "%date{yyyy-MM-dd HH:mm:ss,UTC} %-5level %msg%n%ex{"
          + Joiner.on(',').join(STACK_TRACE_PRINTER_OPTIONS)
          + "}";

  private static final String DEBUG_LAYOUT_PATTERN =
      "%date{yyyy-MM-dd HH:mm:ss,UTC} %-5level %-15thread %-45logger{45} %msg%n";

  private static final Comparator<Entry<String, ConfigValue>> BASIC_SETTINGS_FIRST =
      Comparator.comparing(
          entry -> entry.getKey().replaceFirst("basic\\.", "0.").replaceFirst("advanced\\.", "1."));

  private static final List<String> PATHS_TO_OBFUSCATE =
      ImmutableList.of(
          "advanced.auth-provider.password",
          "advanced.ssl-engine-factory.truststore-password",
          "advanced.ssl-engine-factory.keystore-password");

  // Path Constants
  private static final String STMT = "stmt";
  private static final String ROW = "row";
  private static final String MAX_QUERY_STRING_LENGTH = STMT + '.' + "maxQueryStringLength";
  private static final String MAX_BOUND_VALUE_LENGTH = STMT + '.' + "maxBoundValueLength";
  private static final String MAX_BOUND_VALUES = STMT + '.' + "maxBoundValues";
  private static final String MAX_INNER_STATEMENTS = STMT + '.' + "maxInnerStatements";
  private static final String MAX_RESULT_SET_VALUE_LENGTH = ROW + '.' + "maxResultSetValueLength";
  private static final String MAX_RESULT_SET_VALUES = ROW + '.' + "maxResultSetValues";
  private static final String LEVEL = STMT + '.' + "level";
  private static final String MAX_ERRORS = "maxErrors";
  private static final String MAX_QUERY_WARNINGS = "maxQueryWarnings";
  private static final String VERBOSITY = "verbosity";
  private static final String SOURCES = "sources";

  private final Config config;
  private final String executionId;

  private Path operationDirectory;
  private int maxQueryStringLength;
  private int maxBoundValueLength;
  private int maxBoundValues;
  private int maxResultSetValueLength;
  private int maxResultSetValues;
  private int maxInnerStatements;
  private StatementFormatVerbosity level;
  @VisibleForTesting ErrorThreshold errorThreshold;
  @VisibleForTesting ErrorThreshold queryWarningsThreshold;
  private Verbosity verbosity;
  private boolean sources;
  private boolean checkpointEnabled;
  private ReplayStrategy checkpointReplayStrategy;
  private CheckpointManager checkpointManager;

  public LogSettings(Config config, String executionId) {
    this.config = config;
    this.executionId = executionId;
  }

  public void init() throws IOException {
    try {
      // Note: log.ansiMode is handled upstream by the runner
      // com.datastax.oss.dsbulk.runner.cli.AnsiConfigurator
      operationDirectory =
          new OperationDirectoryResolver(ConfigUtils.getPath(config, "directory"), executionId)
              .resolve();
      OperationDirectory.setCurrentOperationDirectory(operationDirectory);
      maxQueryStringLength = config.getInt(MAX_QUERY_STRING_LENGTH);
      maxBoundValueLength = config.getInt(MAX_BOUND_VALUE_LENGTH);
      maxBoundValues = config.getInt(MAX_BOUND_VALUES);
      maxInnerStatements = config.getInt(MAX_INNER_STATEMENTS);
      level = config.getEnum(StatementFormatVerbosity.class, LEVEL);
      maxResultSetValueLength = config.getInt(MAX_RESULT_SET_VALUE_LENGTH);
      maxResultSetValues = config.getInt(MAX_RESULT_SET_VALUES);
      String maxErrorString = config.getString(MAX_ERRORS);
      if (isPercent(maxErrorString)) {
        float maxErrorsRatio = Float.parseFloat(maxErrorString.replaceAll("\\s*%", "")) / 100f;
        validatePercentageRange(maxErrorsRatio);
        errorThreshold = ErrorThreshold.forRatio(maxErrorsRatio, MIN_SAMPLE);
      } else {
        long maxErrors = config.getLong(MAX_ERRORS);
        if (maxErrors < 0) {
          errorThreshold = ErrorThreshold.unlimited();
        } else {
          errorThreshold = ErrorThreshold.forAbsoluteValue(maxErrors);
        }
      }
      long maxQueryWarnings = config.getLong(MAX_QUERY_WARNINGS);
      if (maxQueryWarnings < 0) {
        queryWarningsThreshold = ErrorThreshold.unlimited();
      } else {
        queryWarningsThreshold = ErrorThreshold.forAbsoluteValue(maxQueryWarnings);
      }
      Path mainLogFile =
          operationDirectory.resolve(MAIN_LOG_FILE_NAME).normalize().toAbsolutePath();
      createMainLogFileAppender(mainLogFile);
      installJavaLoggingToSLF4JBridge();
      verbosity = processVerbosityLevel();
      switch (verbosity) {
        case quiet:
          setVerbosityQuiet();
          break;
        case high:
          setVerbosityHigh();
          break;
        case max:
          setVerbosityMax();
          break;
        default:
          setVerbosityNormal();
          break;
      }
      sources = config.getBoolean(SOURCES);
      checkpointEnabled = config.getBoolean("checkpoint.enabled");
      checkpointReplayStrategy = config.getEnum(ReplayStrategy.class, "checkpoint.replayStrategy");
      if (config.hasPath("checkpoint.file")) {
        Path path = ConfigUtils.getPath(config, "checkpoint.file");
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
          checkpointManager = CheckpointManager.parse(reader);
        }
        if (checkpointManager.isComplete(checkpointReplayStrategy)) {
          throw new IllegalArgumentException(
              String.format(
                  "Nothing to replay using replay strategy %s in checkpoint file: %s.",
                  checkpointReplayStrategy, path));
        }
        LOGGER.warn("Replaying from checkpoint file: {}.", path);
        LOGGER.warn(
            "Record metrics will reflect totals from the previous operation; other metrics won't be affected.");
        LOGGER.warn(
            "Only errors and rejected records generated by the current operation will be reported in its log files.");
      } else {
        checkpointManager = new CheckpointManager();
      }
    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.log");
    }
  }

  public void logEffectiveSettings(Config dsbulkConfig, Config driverConfig) {
    LOGGER.debug("{} starting.", WorkflowUtils.getBulkLoaderNameAndVersion());
    // Initialize the following static fields: their initialization will print the driver
    // coordinates to the console at INFO level, which is enough.
    Objects.requireNonNull(Session.OSS_DRIVER_COORDINATES);
    LOGGER.debug("Available processors: {}.", Runtime.getRuntime().availableProcessors());
    LOGGER.info("Operation directory: {}", operationDirectory);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("DataStax Bulk Loader Effective settings:");
      dumpConfig(dsbulkConfig.withoutPath("driver"), Entry.comparingByKey());
      LOGGER.debug("DataStax Java Driver Effective settings:");
      dumpConfig(driverConfig, BASIC_SETTINGS_FIRST);
    }
  }

  private void dumpConfig(Config config, Comparator<Entry<String, ConfigValue>> comparator) {
    Set<Entry<String, ConfigValue>> entries = new TreeSet<>(comparator);
    entries.addAll(config.entrySet());
    for (Entry<String, ConfigValue> entry : entries) {
      String key = entry.getKey();
      String value;
      if (PATHS_TO_OBFUSCATE.contains(key)) {
        value = "***************";
      } else {
        value = entry.getValue().render(ConfigRenderOptions.concise());
      }
      LOGGER.debug(String.format("- %s = %s", key, value));
    }
  }

  public LogManager newLogManager(CqlSession session) {
    StatementFormatter statementFormatter =
        StatementFormatter.builder()
            .withMaxQueryStringLength(maxQueryStringLength)
            .withMaxBoundValueLength(maxBoundValueLength)
            .withMaxBoundValues(maxBoundValues)
            .withMaxInnerStatements(maxInnerStatements)
            .addStatementPrinters(new MappedBoundStatementPrinter())
            .build();
    RowFormatter rowFormatter = new RowFormatter(maxResultSetValueLength, maxResultSetValues);
    return new LogManager(
        session,
        operationDirectory,
        errorThreshold,
        queryWarningsThreshold,
        statementFormatter,
        level,
        rowFormatter,
        checkpointEnabled,
        checkpointManager,
        checkpointReplayStrategy);
  }

  @NonNull
  public Verbosity getVerbosity() {
    return verbosity;
  }

  /**
   * Whether {@linkplain Record#getSource() record sources} should be retained in memory. When
   * sources are retained, DSBulk is able to print record sources in debug files, for easier error
   * diagnostic. When loading, it also enables DSBulk to create bad files for records that failed to
   * be processed.
   */
  public boolean isSources() {
    return sources;
  }

  @VisibleForTesting
  public static void createMainLogFileAppender(Path mainLogFile) {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext lc = root.getLoggerContext();
    PatternLayoutEncoder ple = new PatternLayoutEncoder();
    ple.setPattern(LAYOUT_PATTERN);
    ple.setContext(lc);
    ple.setCharset(StandardCharsets.UTF_8);
    ple.start();
    FileAppender<ILoggingEvent> mainLogFileAppender = new FileAppender<>();
    mainLogFileAppender.setName(MAIN_LOG_FILE_APPENDER);
    mainLogFileAppender.setFile(mainLogFile.toFile().getAbsolutePath());
    mainLogFileAppender.setEncoder(ple);
    mainLogFileAppender.setContext(lc);
    mainLogFileAppender.setAppend(false);
    ThresholdFilter thresholdFilter = new ThresholdFilter();
    thresholdFilter.setLevel("INFO");
    thresholdFilter.start();
    mainLogFileAppender.addFilter(thresholdFilter);
    mainLogFileAppender.start();
    root.addAppender(mainLogFileAppender);
  }

  private static void installJavaLoggingToSLF4JBridge() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  private Verbosity processVerbosityLevel() {
    try {
      int verbosity = config.getInt(VERBOSITY);
      validateNumericVerbosity(verbosity);
      return Verbosity.values()[verbosity];
    } catch (ConfigException.WrongType e) {
      return config.getEnum(Verbosity.class, VERBOSITY);
    }
  }

  @VisibleForTesting
  public static void setVerbosityQuiet() {
    setAppenderThreshold(CONSOLE_APPENDER, "WARN");
    setAppenderThreshold(MAIN_LOG_FILE_APPENDER, "WARN");
    // raise log levels to WARN across the board
    seLoggerThreshold("com.datastax.oss.dsbulk", Level.WARN);
    seLoggerThreshold("com.datastax.oss.driver", Level.WARN);
    seLoggerThreshold("com.datastax.dse.driver", Level.WARN);
    seLoggerThreshold("io.netty", Level.WARN);
    seLoggerThreshold("reactor.core", Level.WARN);
  }

  @VisibleForTesting
  public static void setVerbosityHigh() {
    setAppenderThreshold(CONSOLE_APPENDER, "DEBUG");
    setAppenderThreshold(MAIN_LOG_FILE_APPENDER, "DEBUG");
    // downgrade log levels to DEBUG (dsbulk) and INFO (driver, Netty, Reactor)
    seLoggerThreshold("com.datastax.oss.dsbulk", Level.DEBUG);
    seLoggerThreshold("com.datastax.oss.driver", Level.INFO);
    seLoggerThreshold("com.datastax.dse.driver", Level.INFO);
    seLoggerThreshold("io.netty", Level.INFO);
    seLoggerThreshold("reactor.core", Level.INFO);
  }

  @VisibleForTesting
  public static void setVerbosityMax() {
    setAppenderThreshold(CONSOLE_APPENDER, "TRACE");
    setAppenderThreshold(MAIN_LOG_FILE_APPENDER, "TRACE");
    setAppenderEncoderPattern(CONSOLE_APPENDER, DEBUG_LAYOUT_PATTERN);
    setAppenderEncoderPattern(MAIN_LOG_FILE_APPENDER, DEBUG_LAYOUT_PATTERN);
    // downgrade log levels to TRACE (dsbulk, driver) and DEBUG (Netty, Reactor)
    seLoggerThreshold("com.datastax.oss.dsbulk", Level.TRACE);
    seLoggerThreshold("com.datastax.oss.driver", Level.TRACE);
    seLoggerThreshold("com.datastax.dse.driver", Level.TRACE);
    seLoggerThreshold("io.netty", Level.DEBUG);
    seLoggerThreshold("reactor.core", Level.DEBUG);
  }

  @VisibleForTesting
  public static void setVerbosityNormal() {
    setAppenderThreshold(CONSOLE_APPENDER, "INFO");
    setAppenderThreshold(MAIN_LOG_FILE_APPENDER, "INFO");
    // These levels correspond to the ones declared in logback.xml,
    // but it doesn't hurt to force them programmatically
    seLoggerThreshold("com.datastax.oss.dsbulk", Level.INFO);
    seLoggerThreshold("com.datastax.oss.driver", Level.WARN);
    seLoggerThreshold("com.datastax.dse.driver", Level.WARN);
    seLoggerThreshold("io.netty", Level.WARN);
    seLoggerThreshold("reactor.core", Level.WARN);
  }

  private static void seLoggerThreshold(String s, Level debug) {
    ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(s);
    logger.setLevel(debug);
  }

  private static void setAppenderThreshold(String appenderName, String level) {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    Appender<ILoggingEvent> appender = root.getAppender(appenderName);
    List<Filter<ILoggingEvent>> filters = appender.getCopyOfAttachedFiltersList();
    for (Filter<ILoggingEvent> filter : filters) {
      if (filter instanceof ThresholdFilter) {
        ThresholdFilter thresholdFilter = (ThresholdFilter) filter;
        thresholdFilter.setLevel(level);
      }
    }
  }

  private static void setAppenderEncoderPattern(String appenderName, String pattern) {
    ch.qos.logback.classic.Logger root =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    OutputStreamAppender<ILoggingEvent> appender =
        ((OutputStreamAppender<ILoggingEvent>) root.getAppender(appenderName));
    LoggerContext lc = root.getLoggerContext();
    PatternLayoutEncoder ple = new PatternLayoutEncoder();
    ple.setPattern(pattern);
    ple.setContext(lc);
    ple.setCharset(StandardCharsets.UTF_8);
    ple.start();
    appender.setEncoder(ple);
  }

  private static boolean isPercent(String maxErrors) {
    return maxErrors.contains("%");
  }

  private static void validatePercentageRange(float maxErrorRatio) {
    if (maxErrorRatio <= 0 || maxErrorRatio >= 1) {
      throw new IllegalArgumentException(
          "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");
    }
  }

  private static void validateNumericVerbosity(int verbosity) {
    if (verbosity < Verbosity.quiet.ordinal() || verbosity > Verbosity.max.ordinal()) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid numeric value for dsbulk.log.verbosity, expecting one of: 0 (quiet), 1 (normal), 2 (high) or 3 (max), got: %s.",
              verbosity));
    }
    LOGGER.warn(
        "Numeric verbosity levels are deprecated, use 'quiet' (0), 'normal' (1), 'high' (2) or 'max' (3) instead.");
  }
}
