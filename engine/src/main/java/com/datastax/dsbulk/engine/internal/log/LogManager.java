/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import static com.datastax.dsbulk.engine.internal.log.LogUtils.formatSource;
import static com.datastax.dsbulk.engine.internal.log.LogUtils.printAndMaybeAddNewLine;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.pattern.ThrowableProxyConverter;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.BusyConnectionException;
import com.datastax.driver.core.exceptions.BusyPoolException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.dsbulk.commons.internal.concurrent.ScalableThreadPoolExecutor;
import com.datastax.dsbulk.connectors.api.ErrorRecord;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.row.RowFormatter;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatter;
import com.datastax.dsbulk.engine.internal.log.threshold.ErrorThreshold;
import com.datastax.dsbulk.engine.internal.schema.InvalidMappingException;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.engine.internal.statement.BulkStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.Result;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Joiner;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Signal;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LogManager implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);

  private static final String MAPPING_ERRORS_FILE = "mapping-errors.log";
  private static final String CONNECTOR_ERRORS_FILE = "connector-errors.log";
  private static final String UNLOAD_ERRORS_FILE = "unload-errors.log";
  private static final String LOAD_ERRORS_FILE = "load-errors.log";
  private static final String CAS_ERRORS_FILE = "paxos-errors.log";

  private static final String CONNECTOR_BAD_FILE = "connector.bad";
  private static final String MAPPING_BAD_FILE = "mapping.bad";
  private static final String LOAD_BAD_FILE = "load.bad";
  private static final String CAS_BAD_FILE = "paxos.bad";

  private static final String POSITIONS_FILE = "positions.txt";

  private final WorkflowType workflowType;
  private final Cluster cluster;
  private final Path executionDirectory;
  private final ErrorThreshold errorThreshold;
  private final StatementFormatter statementFormatter;
  private final StatementFormatVerbosity statementFormatVerbosity;
  private final RowFormatter rowFormatter;

  private final AtomicInteger errors = new AtomicInteger(0);
  private final LongAdder totalItems = new LongAdder();

  private final LoadingCache<Path, PrintWriter> openFiles =
      Caffeine.newBuilder()
          .build(path -> new PrintWriter(Files.newBufferedWriter(path, UTF_8, CREATE_NEW, WRITE)));

  private ScalableThreadPoolExecutor executor;
  private Scheduler scheduler;

  private CodecRegistry codecRegistry;
  private ProtocolVersion protocolVersion;

  private StackTracePrinter stackTracePrinter;

  private PositionsTracker positionsTracker;
  private PrintWriter positionsPrinter;

  private FluxSink<ErrorRecord> failedRecordSink;
  private FluxSink<ErrorRecord> unmappableRecordSink;
  private FluxSink<UnmappableStatement> unmappableStatementSink;
  private FluxSink<WriteResult> failedWriteSink;
  private FluxSink<WriteResult> failedCASWriteSink;
  private FluxSink<ReadResult> failedReadSink;
  private FluxSink<Record> positionsSink;

  private UnicastProcessor<Void> uncaughtExceptionProcessor;
  private FluxSink<Void> uncaughtExceptionSink;

  private AtomicBoolean invalidMappingWarningDone;

  public LogManager(
      WorkflowType workflowType,
      Cluster cluster,
      Path executionDirectory,
      ErrorThreshold errorThreshold,
      StatementFormatter statementFormatter,
      StatementFormatVerbosity statementFormatVerbosity,
      RowFormatter rowFormatter) {
    this.workflowType = workflowType;
    this.cluster = cluster;
    this.executionDirectory = executionDirectory;
    this.errorThreshold = errorThreshold;
    this.statementFormatter = statementFormatter;
    this.statementFormatVerbosity = statementFormatVerbosity;
    this.rowFormatter = rowFormatter;
  }

  public void init() {
    executor =
        // Only spawn 1 thread initially, but stretch up to 8 in case lots of errors arrive
        new ScalableThreadPoolExecutor(1, 8, 60L, SECONDS);
    scheduler = Schedulers.fromExecutorService(executor);
    codecRegistry = cluster.getConfiguration().getCodecRegistry();
    protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    stackTracePrinter = new StackTracePrinter();
    stackTracePrinter.setOptionList(LogSettings.STACK_TRACE_PRINTER_OPTIONS);
    stackTracePrinter.start();
    positionsTracker = new PositionsTracker();
    failedRecordSink = newFailedRecordSink();
    unmappableRecordSink = newUnmappableRecordSink();
    unmappableStatementSink = newUnmappableStatementSink();
    failedWriteSink = newFailedWriteResultSink();
    failedCASWriteSink = newFailedCASWriteSink();
    failedReadSink = newFailedReadResultSink();
    positionsSink = newPositionsSink();
    uncaughtExceptionProcessor = UnicastProcessor.create();
    uncaughtExceptionSink = uncaughtExceptionProcessor.sink();
    invalidMappingWarningDone = new AtomicBoolean(false);
    // The hooks below allow to process uncaught exceptions in certain operators that "bubble up"
    // to the subscriber and/or the worker thread, in which case they are logged but are otherwise
    // ignored, causing the workflow to not stop properly. By setting these global hooks and
    // redirecting any unexpected error to a special sink for uncaught exceptions, we make sure the
    // workflow will receive these error signals and stop as expected.
    Hooks.onErrorDropped(t -> uncaughtExceptionSink.error(t));
    Thread.setDefaultUncaughtExceptionHandler((thread, t) -> uncaughtExceptionSink.error(t));
  }

  public Path getExecutionDirectory() {
    return executionDirectory;
  }

  public int getTotalErrors() {
    return errors.get();
  }

  @Override
  public void close() throws IOException {
    failedRecordSink.complete();
    unmappableRecordSink.complete();
    unmappableStatementSink.complete();
    failedWriteSink.complete();
    failedCASWriteSink.complete();
    failedReadSink.complete();
    uncaughtExceptionSink.complete();
    stackTracePrinter.stop();
    MoreExecutors.shutdownAndAwaitTermination(executor, 1, MINUTES);
    scheduler.dispose();
    // Forcibly close all open files on the thread that invokes close()
    // Using a cache removal listener is not an option because cache listeners
    // are invoked on the common ForkJoinPool, which uses daemon threads.
    openFiles
        .asMap()
        .values()
        .forEach(
            pw -> {
              pw.flush();
              pw.close();
            });
    positionsSink.complete();
    if (workflowType == WorkflowType.LOAD && !positionsTracker.isEmpty()) {
      positionsPrinter =
          new PrintWriter(
              Files.newBufferedWriter(
                  executionDirectory.resolve(POSITIONS_FILE),
                  Charset.forName("UTF-8"),
                  CREATE_NEW,
                  WRITE));
      // sort positions by URI
      new TreeMap<>(positionsTracker.getPositions())
          .forEach((resource, ranges) -> appendToPositionsFile(resource, ranges, positionsPrinter));
      positionsPrinter.flush();
      positionsPrinter.close();
    }
  }

  public void reportLastLocations() {
    PathMatcher badFileMatcher = FileSystems.getDefault().getPathMatcher("glob:*.bad");
    Set<Path> files = openFiles.asMap().keySet();
    List<Path> badFiles =
        files.stream().map(Path::getFileName).filter(badFileMatcher::matches).collect(toList());
    if (!badFiles.isEmpty()) {
      LOGGER.info(
          "Rejected records can be found in the following file(s): {}",
          Joiner.on(", ").join(badFiles));
    }
    List<Path> debugFiles =
        files.stream()
            .map(Path::getFileName)
            .filter(path -> !badFileMatcher.matches(path))
            .collect(toList());
    if (!debugFiles.isEmpty()) {
      LOGGER.info(
          "Errors are detailed in the following file(s): {}", Joiner.on(", ").join(debugFiles));
    }
    if (positionsTracker != null) {
      LOGGER.info("Last processed positions can be found in {}", POSITIONS_FILE);
    }
  }

  /**
   * Handler that is meant to be executed at the very end of the main workflow.
   *
   * <p>It detects when the main workflow emits a terminal signal and reacts by triggering the
   * completion of all processors managed by this component.
   *
   * @return a handler that is meant to be executed at the very end of the main workflow.
   */
  @NotNull
  public Function<Flux<Void>, Flux<Void>> newTerminationHandler() {
    return upstream ->
        upstream
            // when a terminal signal arrives,
            // complete all open processors
            .doOnTerminate(failedRecordSink::complete)
            .doOnTerminate(unmappableRecordSink::complete)
            .doOnTerminate(unmappableStatementSink::complete)
            .doOnTerminate(failedWriteSink::complete)
            .doOnTerminate(failedReadSink::complete)
            .doOnTerminate(failedCASWriteSink::complete)
            .doOnTerminate(uncaughtExceptionSink::complete)
            // By merging the main workflow with the uncaught exceptions
            // workflow, we make the main workflow fail when an uncaught
            // exception is triggered, even if the main workflow completes
            // normally.
            .mergeWith(uncaughtExceptionProcessor);
  }

  /**
   * Handler for unmappable statements produced by the {@linkplain
   * com.datastax.dsbulk.engine.internal.schema.RecordMapper record mapper}.
   *
   * <p>Used only in load workflows.
   *
   * <p>Increments the number of errors and forwards unmappable statements to the unmappable
   * statement processor for further processing.
   *
   * @return a handler for unmappable statements.
   */
  @NotNull
  public Function<Flux<Statement>, Flux<Statement>> newUnmappableStatementsHandler() {
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    Statement stmt = signal.get();
                    if (stmt instanceof UnmappableStatement) {
                      try {
                        unmappableStatementSink.next((UnmappableStatement) stmt);
                        signal = maybeTriggerOnError(signal, errors.incrementAndGet());
                      } catch (Exception e) {
                        signal = Signal.error(e);
                      }
                    }
                  }
                  return signal;
                })
            .<Statement>dematerialize()
            .filter(r -> !(r instanceof UnmappableStatement));
  }

  /**
   * Handler for failed records. A failed record is a record that the connector could not read or
   * write.
   *
   * <p>Used by both load and unload workflows.
   *
   * <p>Increments the number of errors and forwards failed records to the failed record processor
   * for further processing.
   *
   * @return a handler for failed records.
   */
  @NotNull
  public Function<Flux<Record>, Flux<Record>> newFailedRecordsHandler() {
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    Record r = signal.get();
                    if (r instanceof ErrorRecord) {
                      try {
                        failedRecordSink.next((ErrorRecord) r);
                        signal = maybeTriggerOnError(signal, errors.incrementAndGet());
                      } catch (Exception e) {
                        signal = Signal.error(e);
                      }
                    }
                  }
                  return signal;
                })
            .<Record>dematerialize()
            .filter(r -> !(r instanceof ErrorRecord));
  }

  /**
   * Handler for unmappable records produced by the {@linkplain
   * com.datastax.dsbulk.engine.internal.schema.ReadResultMapper result mapper}.
   *
   * <p>Used only in unload workflows.
   *
   * <p>Increments the number of errors and forwards unmappable records to the unmappable record
   * processor for further processing.
   *
   * @return a handler for unmappable records.
   */
  @NotNull
  public Function<Flux<Record>, Flux<Record>> newUnmappableRecordsHandler() {
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    Record r = signal.get();
                    if (r instanceof ErrorRecord) {
                      try {
                        unmappableRecordSink.next((ErrorRecord) r);
                        signal = maybeTriggerOnError(signal, errors.incrementAndGet());
                      } catch (Exception e) {
                        signal = Signal.error(e);
                      }
                    }
                  }
                  return signal;
                })
            .<Record>dematerialize()
            .filter(r -> !(r instanceof ErrorRecord));
  }

  /**
   * Handler for unsuccessful {@link WriteResult}s.
   *
   * <p>Used only by the load workflow.
   *
   * <p>Increments the number of errors and forwards unsuccessful write results to the write result
   * processor for further processing.
   *
   * @return a handler for unsuccessful write results.
   */
  @NotNull
  public Function<Flux<WriteResult>, Flux<WriteResult>> newFailedWritesHandler() {
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    WriteResult r = signal.get();
                    if (r != null) {
                      if (!r.isSuccess()) {
                        try {
                          failedWriteSink.next(r);
                          assert r.getError().isPresent();
                          Throwable cause = r.getError().get().getCause();
                          if (isUnrecoverable(cause)) {
                            signal = Signal.error(cause);
                          } else {
                            signal =
                                maybeTriggerOnError(signal, errors.addAndGet(r.getBatchSize()));
                          }
                        } catch (Exception e) {
                          signal = Signal.error(e);
                        }
                      } else if (!r.wasApplied()) {
                        try {
                          failedCASWriteSink.next(r);
                          signal = maybeTriggerOnError(signal, errors.addAndGet(r.getBatchSize()));
                        } catch (Exception e) {
                          signal = Signal.error(e);
                        }
                      }
                    }
                  }
                  return signal;
                })
            .<WriteResult>dematerialize()
            .filter(Result::isSuccess)
            .filter(WriteResult::wasApplied);
  }

  /**
   * Handler for unsuccessful {@link ReadResult}s.
   *
   * <p>Used only by the unload workflow.
   *
   * <p>Increments the number of errors and forwards unsuccessful read results to the read result
   * processor for further processing.
   *
   * @return a handler for unsuccessful read results.
   */
  @NotNull
  public Function<Flux<ReadResult>, Flux<ReadResult>> newFailedReadsHandler() {
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    ReadResult r = signal.get();
                    if (r != null && !r.isSuccess()) {
                      try {
                        failedReadSink.next(r);
                        assert r.getError().isPresent();
                        Throwable cause = r.getError().get().getCause();
                        if (isUnrecoverable(cause)) {
                          signal = Signal.error(cause);
                        } else {
                          signal = maybeTriggerOnError(signal, errors.incrementAndGet());
                        }
                      } catch (Exception e) {
                        signal = Signal.error(e);
                      }
                    }
                  }
                  return signal;
                })
            .<ReadResult>dematerialize()
            .filter(Result::isSuccess);
  }

  /**
   * Handler for result positions.
   *
   * <p>Used only by the load workflow.
   *
   * <p>Extracts the result's {@link Record} and updates the positions.
   *
   * @return A handler for result positions.
   */
  public Function<Flux<WriteResult>, Flux<Void>> newResultPositionsHandler() {
    return upstream ->
        upstream
            .map(Result::getStatement)
            .transform(newStatementToRecordMapper())
            .doOnNext(record -> positionsSink.next(record))
            .then()
            .flux();
  }

  public <T> Function<Flux<T>, Flux<T>> newTotalItemsCounter() {
    return upstream -> upstream.doOnNext(r -> totalItems.increment());
  }

  /**
   * Maps statements into records.
   *
   * <p>If the statement is a batch, then each of its children is mapped individually, otherwise the
   * statement is mapped to a record in a one-to-one fashion.
   *
   * <p>Note that all non-batch statements are required to be of type {@code BulkStatement<Record>}.
   *
   * @return a mapper from statements to records.
   */
  @NotNull
  private Function<Flux<? extends Statement>, Flux<Record>> newStatementToRecordMapper() {
    return upstream ->
        upstream
            .flatMap(
                statement -> {
                  if (statement instanceof BatchStatement) {
                    return Flux.fromIterable(((BatchStatement) statement).getStatements());
                  } else {
                    return Flux.just(statement);
                  }
                })
            .cast(BulkStatement.class)
            .map(BulkStatement::getSource)
            .cast(Record.class);
  }

  /**
   * A processor for failed records. A failed record is a record that the connector could not read
   * or write.
   *
   * <p>Used in both load and unload workflows.
   *
   * <p>Appends the record to the debug file, then (for load workflows only) to the bad file and
   * forwards the record's position to the position tracker.
   *
   * @return A processor for failed records.
   */
  @NotNull
  private FluxSink<ErrorRecord> newFailedRecordSink() {
    UnicastProcessor<ErrorRecord> processor = UnicastProcessor.create();
    Flux<ErrorRecord> flux =
        processor.publishOn(scheduler).doOnNext(this::appendFailedRecordToDebugFile);
    if (workflowType == WorkflowType.LOAD) {
      flux.doOnNext(record -> appendToBadFile(record, CONNECTOR_BAD_FILE))
          .doOnNext(record -> positionsSink.next(record))
          .subscribe();
    } else {
      flux.subscribe();
    }
    return processor.sink();
  }

  /**
   * A processor for unmappable records produced by the {@linkplain
   * com.datastax.dsbulk.engine.internal.schema.ReadResultMapper result mapper}.
   *
   * <p>Used only in unload workflows.
   *
   * <p>Appends the record to the debug file.
   *
   * @return A processor for unmappable records.
   */
  @NotNull
  private FluxSink<ErrorRecord> newUnmappableRecordSink() {
    UnicastProcessor<ErrorRecord> processor = UnicastProcessor.create();
    processor
        .publishOn(scheduler)
        .doOnNext(this::appendUnmappableReadResultToDebugFile)
        .subscribe();
    return processor.sink();
  }

  /**
   * A processor for unmappable statements produced by the {@linkplain
   * com.datastax.dsbulk.engine.internal.schema.RecordMapper record mapper}.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the statement to the debug file, then extracts its record, appends it to the bad
   * file, then forwards the record's position to the position tracker.
   *
   * @return A processor for unmappable statements.
   */
  @NotNull
  private FluxSink<UnmappableStatement> newUnmappableStatementSink() {
    UnicastProcessor<UnmappableStatement> processor = UnicastProcessor.create();
    processor
        .publishOn(scheduler)
        .doOnNext(this::maybeWarnInvalidMapping)
        .doOnNext(this::appendUnmappableStatementToDebugFile)
        .transform(newStatementToRecordMapper())
        .doOnNext(record -> appendToBadFile(record, MAPPING_BAD_FILE))
        .doOnNext(record -> positionsSink.next(record))
        .subscribe();
    return processor.sink();
  }

  /**
   * A processor for failed write results.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the failed result to the debug file, then extracts its statement, then extracts its
   * record, then appends it to the bad file, then forwards the record's position to the position
   * tracker.
   *
   * @return A processor for failed write results.
   */
  @NotNull
  private FluxSink<WriteResult> newFailedWriteResultSink() {
    UnicastProcessor<WriteResult> processor = UnicastProcessor.create();
    processor
        .publishOn(scheduler)
        .doOnNext(this::appendFailedWriteResultToDebugFile)
        .map(Result::getStatement)
        .transform(newStatementToRecordMapper())
        .doOnNext(record -> appendToBadFile(record, LOAD_BAD_FILE))
        .doOnNext(record -> positionsSink.next(record))
        .subscribe();
    return processor.sink();
  }

  /**
   * A processor for failed CAS write results.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the failed result to the debug file, then extracts its statement, then extracts its
   * record, then appends it to the bad file, then forwards the record's position to the position
   * tracker.
   *
   * @return A processor for failed CAS write results.
   */
  @NotNull
  private FluxSink<WriteResult> newFailedCASWriteSink() {
    UnicastProcessor<WriteResult> processor = UnicastProcessor.create();
    processor
        .publishOn(scheduler)
        .doOnNext(this::appendFailedCASWriteResultToDebugFile)
        .map(Result::getStatement)
        .transform(newStatementToRecordMapper())
        .doOnNext(record -> appendToBadFile(record, CAS_BAD_FILE))
        .doOnNext(record -> positionsSink.next(record))
        .subscribe();
    return processor.sink();
  }

  /**
   * A processor for failed read results.
   *
   * <p>Used only in the unload workflow.
   *
   * <p>Extracts the statement, then appends it to the debug file.
   *
   * @return A processor for failed read results.
   */
  @NotNull
  private FluxSink<ReadResult> newFailedReadResultSink() {
    UnicastProcessor<ReadResult> processor = UnicastProcessor.create();
    processor.publishOn(scheduler).doOnNext(this::appendFailedReadResultToDebugFile).subscribe();
    return processor.sink();
  }

  /**
   * A processor for record positions.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Updates the position tracker for every record received.
   *
   * @return A processor for record positions.
   */
  @NotNull
  private FluxSink<Record> newPositionsSink() {
    UnicastProcessor<Record> processor = UnicastProcessor.create();
    processor
        // do not need to be published on the dedicated log scheduler, the computation is fairly
        // cheap
        .doOnNext(record -> positionsTracker.update(record.getResource(), record.getPosition()))
        .subscribe();
    return processor.sink();
  }

  // Bad file management

  private void appendToBadFile(Record record, String file) {
    Path logFile = executionDirectory.resolve(file);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    Object source = record.getSource();
    printAndMaybeAddNewLine(source.toString(), writer);
    writer.flush();
  }

  // Executor errors (read/write failures)

  // write query failed
  private void appendFailedWriteResultToDebugFile(WriteResult result) {
    appendStatement(result, LOAD_ERRORS_FILE, true);
  }

  // CAS write query failed
  private void appendFailedCASWriteResultToDebugFile(WriteResult result) {
    appendStatement(result, CAS_ERRORS_FILE, true);
  }

  // read query failed
  private void appendFailedReadResultToDebugFile(ReadResult result) {
    appendStatement(result, UNLOAD_ERRORS_FILE, true);
  }

  private void appendStatement(Result result, String logFileName, boolean appendNewLine) {
    Path logFile = executionDirectory.resolve(logFileName);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    writer.print("Statement: ");
    String format =
        statementFormatter.format(
            result.getStatement(), statementFormatVerbosity, protocolVersion, codecRegistry);
    printAndMaybeAddNewLine(format, writer);
    if (result instanceof WriteResult) {
      WriteResult writeResult = (WriteResult) result;
      if (!writeResult.wasApplied()) {
        writer.println("Failed writes: ");
        writeResult
            .getFailedWrites()
            .forEach(
                row -> {
                  String failed = rowFormatter.format(row, protocolVersion, codecRegistry);
                  printAndMaybeAddNewLine(failed, writer);
                });
      }
    }
    if (result.getError().isPresent()) {
      stackTracePrinter.printStackTrace(result.getError().get(), writer);
    }
    if (appendNewLine) {
      writer.println();
    }
  }

  // Mapping errors (failed record -> statement or row -> record mappings)

  // record -> statement failed (load workflow)
  private void appendUnmappableStatementToDebugFile(UnmappableStatement statement) {
    Path logFile = executionDirectory.resolve(MAPPING_ERRORS_FILE);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    Record record = statement.getSource();
    writer.println("Resource: " + record.getResource());
    writer.println("Position: " + record.getPosition());
    writer.println("Source: " + formatSource(record));
    stackTracePrinter.printStackTrace(statement.getError(), writer);
    writer.println();
  }

  // row -> record failed (unload workflow)
  private void appendUnmappableReadResultToDebugFile(ErrorRecord record) {
    Path logFile = executionDirectory.resolve(MAPPING_ERRORS_FILE);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    writer.println("Resource: " + record.getResource());
    if (record.getSource() instanceof ReadResult) {
      ReadResult source = (ReadResult) record.getSource();
      appendStatement(source, MAPPING_ERRORS_FILE, false);
      source
          .getRow()
          .ifPresent(
              row -> {
                writer.print("Row: ");
                String format = rowFormatter.format(row, protocolVersion, codecRegistry);
                printAndMaybeAddNewLine(format, writer);
              });
    }
    stackTracePrinter.printStackTrace(record.getError(), writer);
    writer.println();
  }

  // Connector errors

  private void appendFailedRecordToDebugFile(ErrorRecord record) {
    Path logFile = executionDirectory.resolve(CONNECTOR_ERRORS_FILE);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    writer.println("Resource: " + record.getResource());
    writer.println("Position: " + record.getPosition());
    writer.println("Source: " + formatSource(record));
    stackTracePrinter.printStackTrace(record.getError(), writer);
    writer.println();
  }

  // Utility methods

  private static void appendToPositionsFile(
      URI resource, List<Range<Long>> positions, PrintWriter positionsPrinter) {
    positionsPrinter.print(resource);
    positionsPrinter.print(':');
    positions.stream().findFirst().ifPresent(pos -> positionsPrinter.print(pos.upperEndpoint()));
    positionsPrinter.println();
  }

  private <T> Signal<T> maybeTriggerOnError(Signal<T> signal, int errorCount) {
    if (errorThreshold.checkThresholdExceeded(errorCount, totalItems)) {
      return Signal.error(new TooManyErrorsException(errorThreshold));
    }
    return signal;
  }

  private void maybeWarnInvalidMapping(UnmappableStatement stmt) {
    if (stmt.getError() instanceof InvalidMappingException) {
      if (invalidMappingWarningDone.compareAndSet(false, true)) {
        LOGGER.warn(
            "At least 1 record does not match the provided schema.mapping or schema.query. "
                + "Please check that the connector configuration and the schema configuration are correct.");
      }
    }
  }

  private static boolean isUnrecoverable(Throwable error) {
    return !(error instanceof QueryExecutionException
        || error instanceof InvalidQueryException
        || error instanceof OperationTimedOutException
        || error instanceof BusyPoolException
        || error instanceof BusyConnectionException);
  }

  /** A small utility to print stack traces, leveraging Logback's filtering capabilities. */
  private static class StackTracePrinter extends ThrowableProxyConverter {

    @Override
    public void start() {
      setContext(new LoggerContext());
      super.start();
    }

    private void printStackTrace(Throwable t, PrintWriter writer) {
      // throwableProxyToString already appends a line break at the end
      writer.print(throwableProxyToString(new ThrowableProxy(t)));
      writer.flush();
    }
  }
}
