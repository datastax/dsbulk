/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.filter.Filter;
import com.datastax.driver.core.Cluster;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.log.row.RowFormatter;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatter;
import com.datastax.dsbulk.engine.internal.log.threshold.ErrorThreshold;
import com.datastax.dsbulk.engine.internal.utils.HelpUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class LogSettings {

  public static final String OPERATION_DIRECTORY_KEY = "com.datastax.dsbulk.OPERATION_DIRECTORY";
  private static final String CONSOLE_APPENDER = "CONSOLE";
  private static final int MIN_SAMPLE = 100;

  static {
    // DAT-451: disable driver query warnings logging, we will handle them in LogManager
    System.setProperty("com.datastax.driver.DISABLE_QUERY_WARNING_LOGS", "true");
  }

  public enum Verbosity {
    quiet,
    normal,
    verbose
  }

  private static final String MAIN_LOG_FILE_APPENDER = "FILE";
  private static final String MAIN_LOG_FILE_NAME = "operation.log";

  /** The options for stack trace printing. */
  public static final ImmutableList<String> STACK_TRACE_PRINTER_OPTIONS =
      ImmutableList.of(
          // number of stack elements to print
          "5",
          // packages to exclude from stack traces
          "reactor.core",
          "com.google",
          "io.netty",
          "java.util.concurrent");

  /**
   * A filter to exclude some errors from the sanitized message printed to the console. See {@link
   * com.datastax.dsbulk.commons.internal.utils.ThrowableUtils}. Used in {@link
   * com.datastax.dsbulk.engine.DataStaxBulkLoader} but declared here since this class deals with
   * error handling.
   */
  public static final Predicate<Throwable> NO_REACTOR_ERRORS =
      t ->
          // filter out reactor exceptions as they are usually not relevant to users and
          // other more meaningful errors are generally present.
          !t.getClass().getName().startsWith("reactor.")
              && (t.getMessage() == null || !t.getMessage().startsWith("#block terminated"));

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

  private static final Logger LOGGER = LoggerFactory.getLogger(LogSettings.class);

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

  private final LoaderConfig config;
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

  LogSettings(LoaderConfig config, String executionId) {
    this.config = config;
    this.executionId = executionId;
  }

  public void init() throws IOException {
    try {
      // Note: log.ansiMode is handled upstream by com.datastax.dsbulk.engine.DataStaxBulkLoader
      resolveOperationDirectory();
      checkOperationDirectory();
      System.setProperty(OPERATION_DIRECTORY_KEY, operationDirectory.toFile().getAbsolutePath());
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
      int verbosity = config.getInt(VERBOSITY);
      validateVerbosity(verbosity);
      if (verbosity == 0) {
        setQuiet();
      } else if (verbosity == 2) {
        setVerbose();
      }
      this.verbosity = Verbosity.values()[verbosity];
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "log");
    }
  }

  public void logEffectiveSettings(Config global) {
    LOGGER.debug("{} starting.", HelpUtils.getVersionMessage());
    LOGGER.debug("Available processors: {}.", Runtime.getRuntime().availableProcessors());
    LOGGER.info("Operation directory: {}", operationDirectory);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Effective settings:");
      Set<Map.Entry<String, ConfigValue>> entries =
          new TreeSet<>(Comparator.comparing(Map.Entry::getKey));
      entries.addAll(global.entrySet());
      for (Map.Entry<String, ConfigValue> entry : entries) {
        // Skip all settings that have a `metaSettings` path element.
        if (entry.getKey().contains(".metaSettings.")) {
          continue;
        }
        LOGGER.debug(
            String.format(
                "%s = %s", entry.getKey(), entry.getValue().render(ConfigRenderOptions.concise())));
      }
    }
  }

  public LogManager newLogManager(WorkflowType workflowType, Cluster cluster) {
    StatementFormatter statementFormatter =
        StatementFormatter.builder()
            .withMaxQueryStringLength(maxQueryStringLength)
            .withMaxBoundValueLength(maxBoundValueLength)
            .withMaxBoundValues(maxBoundValues)
            .withMaxInnerStatements(maxInnerStatements)
            .build();
    RowFormatter rowFormatter = new RowFormatter(maxResultSetValueLength, maxResultSetValues);
    return new LogManager(
        workflowType,
        cluster,
        operationDirectory,
        errorThreshold,
        queryWarningsThreshold,
        statementFormatter,
        level,
        rowFormatter);
  }

  public Verbosity getVerbosity() {
    return verbosity;
  }

  private void resolveOperationDirectory() {
    Path logDirectory = config.getPath("directory");
    try {
      operationDirectory = logDirectory.resolve(executionId);
    } catch (InvalidPathException e) {
      throw new BulkConfigurationException(
          String.format(
              "Execution ID '%s' is not a valid path name on the local filesytem", executionId),
          e);
    }
  }

  private void checkOperationDirectory() throws IOException {
    if (Files.exists(operationDirectory)) {
      if (Files.isDirectory(operationDirectory)) {
        if (Files.isWritable(operationDirectory)) {
          @SuppressWarnings("StreamResourceLeak")
          long count = Files.list(operationDirectory).count();
          if (count > 0) {
            throw new IllegalArgumentException(
                "Execution directory exists but is not empty: " + operationDirectory);
          }
        } else {
          throw new IllegalArgumentException(
              "Execution directory exists but is not writable: " + operationDirectory);
        }
      } else {
        throw new IllegalArgumentException(
            "Execution directory exists but is not a directory: " + operationDirectory);
      }
    } else {
      Files.createDirectories(operationDirectory);
    }
  }

  @VisibleForTesting
  public static FileAppender<ILoggingEvent> createMainLogFileAppender(Path mainLogFile) {
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
    return mainLogFileAppender;
  }

  private static void installJavaLoggingToSLF4JBridge() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  @VisibleForTesting
  public static void setQuiet() {
    setThreshold(CONSOLE_APPENDER, "WARN");
    setThreshold(MAIN_LOG_FILE_APPENDER, "WARN");
  }

  @VisibleForTesting
  public static void setVerbose() {
    setThreshold(CONSOLE_APPENDER, "DEBUG");
    setThreshold(MAIN_LOG_FILE_APPENDER, "DEBUG");
    // downgrade log levels to DEBUG (dsbulk) and INFO (driver, Netty, Reactor)
    ch.qos.logback.classic.Logger dsbulkLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.datastax.dsbulk");
    dsbulkLogger.setLevel(Level.DEBUG);
    ch.qos.logback.classic.Logger driverLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.datastax.driver");
    driverLogger.setLevel(Level.INFO);
    ch.qos.logback.classic.Logger nettyLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("io.netty");
    nettyLogger.setLevel(Level.INFO);
    ch.qos.logback.classic.Logger reactorLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("reactor.core");
    reactorLogger.setLevel(Level.INFO);
  }

  private static void setThreshold(String appenderName, String level) {
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

  private static boolean isPercent(String maxErrors) {
    return maxErrors.contains("%");
  }

  private static void validatePercentageRange(float maxErrorRatio) {
    if (maxErrorRatio <= 0 || maxErrorRatio >= 1) {
      throw new BulkConfigurationException(
          "maxErrors must either be a number, or percentage between 0 and 100 exclusive.");
    }
  }

  private static void validateVerbosity(int verbosity) {
    if (verbosity < 0 || verbosity > 2) {
      throw new BulkConfigurationException(
          "verbosity must either be 0 (quiet), 1 (normal) or 2 (verbose).");
    }
  }
}
