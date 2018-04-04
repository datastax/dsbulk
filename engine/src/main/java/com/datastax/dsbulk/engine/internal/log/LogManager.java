/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import static com.datastax.dsbulk.engine.internal.log.LogUtils.appendRecordInfo;
import static com.datastax.dsbulk.engine.internal.log.LogUtils.appendStatementInfo;
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
import com.datastax.dsbulk.connectors.api.ErrorRecord;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatter;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.engine.internal.statement.BulkStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.Result;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Range;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
import reactor.util.concurrent.Queues;

public class LogManager implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);
  private static final int MIN_SAMPLE = 100;

  private static final String MAPPING_ERRORS_FILE = "mapping-errors.log";
  private static final String CONNECTOR_ERRORS_FILE = "connector-errors.log";
  private static final String UNLOAD_ERRORS_FILE = "unload-errors.log";
  private static final String LOAD_ERRORS_FILE = "load-errors.log";

  private static final String CONNECTOR_BAD_FILE = "connector.bad";
  private static final String MAPPING_BAD_FILE = "mapping.bad";
  private static final String LOAD_BAD_FILE = "load.bad";

  private static final String POSITIONS_FILE = "positions.txt";

  private final WorkflowType workflowType;
  private final Cluster cluster;
  private final Path executionDirectory;
  private final int maxErrors;
  private final float maxErrorRatio;
  private final StatementFormatter formatter;
  private final StatementFormatVerbosity verbosity;

  private final AtomicInteger errors = new AtomicInteger(0);
  private final LongAdder totalItems = new LongAdder();

  private final LoadingCache<Path, PrintWriter> openFiles =
      Caffeine.newBuilder()
          .build(path -> new PrintWriter(Files.newBufferedWriter(path, UTF_8, CREATE_NEW, WRITE)));

  private final ConcurrentMap<URI, List<Range<Long>>> positions = new ConcurrentSkipListMap<>();

  private ExecutorService executor;
  private Scheduler scheduler;

  private CodecRegistry codecRegistry;
  private ProtocolVersion protocolVersion;

  private StackTracePrinter stackTracePrinter;
  private PrintWriter positionsPrinter;

  private FluxSink<ErrorRecord> failedRecordSink;
  private FluxSink<ErrorRecord> unmappableRecordSink;
  private FluxSink<UnmappableStatement> unmappableStatementSink;
  private FluxSink<WriteResult> writeResultSink;
  private FluxSink<ReadResult> readResultSink;

  private UnicastProcessor<Void> uncaughtExceptionProcessor;
  private FluxSink<Void> uncaughtExceptionSink;

  public LogManager(
      WorkflowType workflowType,
      Cluster cluster,
      Path executionDirectory,
      int maxErrors,
      float maxErrorRatio,
      StatementFormatter formatter,
      StatementFormatVerbosity verbosity) {
    this.workflowType = workflowType;
    this.cluster = cluster;
    this.executionDirectory = executionDirectory;
    this.maxErrors = maxErrors;
    this.maxErrorRatio = maxErrorRatio;
    this.formatter = formatter;
    this.verbosity = verbosity;
  }

  public void init() {
    executor =
        // Only spin 1 thread, but stretch up to 8 in case lots of errors arrive
        new ThreadPoolExecutor(
            1,
            8,
            60L,
            SECONDS,
            new LinkedBlockingQueue<>(),
            new DefaultThreadFactory("log-manager"));
    scheduler = Schedulers.fromExecutorService(executor);
    codecRegistry = cluster.getConfiguration().getCodecRegistry();
    protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    stackTracePrinter = new StackTracePrinter();
    stackTracePrinter.setOptionList(LogSettings.STACK_TRACE_PRINTER_OPTIONS);
    stackTracePrinter.start();
    failedRecordSink = newFailedRecordSink();
    unmappableRecordSink = newUnmappableRecordSink();
    unmappableStatementSink = newUnmappableStatementSink();
    writeResultSink = newWriteResultSink();
    readResultSink = newReadResultSink();
    uncaughtExceptionProcessor = UnicastProcessor.create();
    uncaughtExceptionSink = uncaughtExceptionProcessor.sink();
    // This is required because sometimes, when an error signal generated by this class
    // (e.g. when the maximum number of errors is reached) is propagated downstream,
    // it reaches the very last flatMap operator used by the position tracker *after* this
    // operator is completed. (It seems that these flatMap operators are completed as soon
    // as the enclosing groupBy operator completes.)
    // Because of that, the error signal is dropped by the Reactor framework and rethrown
    // wrapped in a bubbling exception (see FluxFlatMap line 388); this exception is then
    // caught by Guava's future listener, which logs it and then ignores it completely.
    // As a consequence, the workflow does not stop properly.
    // By setting a global hook for dropped errors, we prevent the bubbling exception from
    // being thrown and the workflow stops as expected.
    Hooks.onErrorDropped(t -> {});
    // The hook below allows to process uncaught exceptions in certain operators that "bubble up"
    // to the worker thread, in which case they are logged but are otherwise ignored,
    // and the workflow does not stop properly. By setting a global hook for errors
    // caught by a worker thread, we can make the workflow stop as expected.
    Thread.setDefaultUncaughtExceptionHandler((thread, t) -> uncaughtExceptionSink.error(t));
  }

  public Path getExecutionDirectory() {
    return executionDirectory;
  }

  public int getTotalErrors() {
    return errors.get();
  }

  @Override
  public void close() throws InterruptedException, IOException {
    failedRecordSink.complete();
    unmappableRecordSink.complete();
    unmappableStatementSink.complete();
    writeResultSink.complete();
    readResultSink.complete();
    uncaughtExceptionSink.complete();
    stackTracePrinter.stop();
    executor.shutdown();
    executor.awaitTermination(1, MINUTES);
    executor.shutdownNow();
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
    if (workflowType == WorkflowType.LOAD && !positions.isEmpty()) {
      positionsPrinter =
          new PrintWriter(
              Files.newBufferedWriter(
                  executionDirectory.resolve(POSITIONS_FILE),
                  Charset.forName("UTF-8"),
                  CREATE_NEW,
                  WRITE));
      positions.forEach(
          (resource, ranges) -> appendToPositionsFile(resource, ranges, positionsPrinter));
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
        files
            .stream()
            .map(Path::getFileName)
            .filter(path -> !badFileMatcher.matches(path))
            .collect(toList());
    if (!debugFiles.isEmpty()) {
      LOGGER.info(
          "Errors are detailed in the following file(s): {}", Joiner.on(", ").join(debugFiles));
    }
    if (positionsPrinter != null) {
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
            .doOnTerminate(writeResultSink::complete)
            .doOnTerminate(readResultSink::complete)
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
                    if (!r.isSuccess()) {
                      try {
                        writeResultSink.next(r);
                        assert r.getError().isPresent();
                        Throwable cause = r.getError().get().getCause();
                        if (isUnrecoverable(cause)) {
                          signal = Signal.error(cause);
                        } else {
                          signal =
                              maybeTriggerOnError(
                                  signal, errors.addAndGet(delta(r.getStatement())));
                        }
                      } catch (Exception e) {
                        signal = Signal.error(e);
                      }
                    }
                  }
                  return signal;
                })
            .<WriteResult>dematerialize()
            .filter(Result::isSuccess);
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
                    if (!r.isSuccess()) {
                      try {
                        readResultSink.next(r);
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
   * A tracker for result positions.
   *
   * <p>Used only by the load workflow.
   *
   * <p>Extracts the result's {@link Statement} and applies {@link #newStatementPositionTracker()
   * statementPositionTracker}.
   *
   * @return A tracker for result positions.
   */
  public Function<Flux<WriteResult>, Flux<Void>> newResultPositionTracker() {
    return upstream -> upstream.map(Result::getStatement).transform(newStatementPositionTracker());
  }

  public <T> Function<Flux<T>, Flux<T>> newTotalItemsCounter() {
    return upstream -> upstream.doOnNext(r -> totalItems.increment());
  }

  /**
   * A tracker for statement positions.
   *
   * <p>Used only by the load workflow.
   *
   * <p>Extracts the statements's {@link Record}s and applies {@link #newRecordPositionTracker()
   * recordPositionTracker}.
   *
   * @return A tracker for statement positions.
   */
  private Function<Flux<? extends Statement>, Flux<Void>> newStatementPositionTracker() {
    return upstream ->
        upstream.transform(newStatementToRecordMapper()).transform(newRecordPositionTracker());
  }

  /**
   * A tracker for record positions.
   *
   * <p>Used only by the load workflow.
   *
   * <p>Groups together records by {@linkplain Record#getResource() resource} then merges all their
   * {@linkplain Record#getPosition() positions} into continuous ranges.
   *
   * @return A tracker for statement positions.
   */
  private Function<Flux<? extends Record>, Flux<Void>> newRecordPositionTracker() {
    return upstream ->
        upstream
            .filter(record -> record.getPosition() > 0)
            .window(Queues.SMALL_BUFFER_SIZE)
            .flatMap(
                window ->
                    window
                        .groupBy(Record::getResource, Record::getPosition)
                        .flatMap(
                            group ->
                                group
                                    .reduceWith(ArrayList::new, LogManager::addPosition)
                                    .doOnNext(
                                        ranges ->
                                            positions.merge(
                                                group.key(), ranges, LogManager::mergePositions)),
                            Queues.SMALL_BUFFER_SIZE))
            .then()
            .flux();
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
   * forwards the record's position to the {@linkplain #newRecordPositionTracker() position
   * tracker}.
   *
   * @return A processor for failed records.
   */
  @NotNull
  private FluxSink<ErrorRecord> newFailedRecordSink() {
    UnicastProcessor<ErrorRecord> processor = UnicastProcessor.create();
    Flux<ErrorRecord> flux =
        processor
            .publishOn(scheduler)
            .doOnNext(record -> appendToDebugFile(record, CONNECTOR_ERRORS_FILE));
    if (workflowType == WorkflowType.LOAD) {
      flux.doOnNext(record -> appendToBadFile(record, CONNECTOR_BAD_FILE))
          .transform(newRecordPositionTracker())
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
   * <p>Appends the record to the debug file, then (for load workflows only) to the bad file and
   * forwards the record's position to the {@linkplain #newRecordPositionTracker() position
   * tracker}.
   *
   * @return A processor for unmappable records.
   */
  @NotNull
  private FluxSink<ErrorRecord> newUnmappableRecordSink() {
    UnicastProcessor<ErrorRecord> processor = UnicastProcessor.create();
    processor
        .publishOn(scheduler)
        .doOnNext(record -> appendToDebugFile(record, MAPPING_ERRORS_FILE))
        .subscribe();
    return processor.sink();
  }

  /**
   * A processor for unmappable statementsproduced by the {@linkplain
   * com.datastax.dsbulk.engine.internal.schema.RecordMapper} record mapper}.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the statement to the debug file, then extracts its record, appends it to the bad
   * file, then forwards the record's position to the {@linkplain #newRecordPositionTracker()
   * position tracker}.
   *
   * @return A processor for unmappable statements.
   */
  @NotNull
  private FluxSink<UnmappableStatement> newUnmappableStatementSink() {
    UnicastProcessor<UnmappableStatement> processor = UnicastProcessor.create();
    processor
        .publishOn(scheduler)
        .doOnNext(this::appendToDebugFile)
        .transform(newStatementToRecordMapper())
        .doOnNext(record -> appendToBadFile(record, MAPPING_BAD_FILE))
        .transform(newRecordPositionTracker())
        .subscribe();
    return processor.sink();
  }

  /**
   * A processor for failed write results.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the failed result to the debug file, then extracts its statement, then extracts its
   * record, then appends it to the bad file, then forwards the record's position to the {@linkplain
   * #newRecordPositionTracker() position tracker}.
   *
   * @return A processor for failed write results.
   */
  @NotNull
  private FluxSink<WriteResult> newWriteResultSink() {
    UnicastProcessor<WriteResult> processor = UnicastProcessor.create();
    processor
        .publishOn(scheduler)
        .doOnNext(this::appendToDebugFile)
        .map(Result::getStatement)
        .transform(newStatementToRecordMapper())
        .doOnNext(record -> appendToBadFile(record, LOAD_BAD_FILE))
        .transform(newRecordPositionTracker())
        .subscribe();
    return processor.sink();
  }

  /**
   * A processor for failed read results.
   *
   * <p>Used only in the unload workflow.
   *
   * <p>Extracts the statement, then appends it to the debug file, then extracts its record, appends
   * it to the bad file, then forwards the record's position to the {@linkplain
   * #newRecordPositionTracker() position tracker}.
   *
   * @return A processor for failed read results.
   */
  @NotNull
  private FluxSink<ReadResult> newReadResultSink() {
    UnicastProcessor<ReadResult> processor = UnicastProcessor.create();
    processor.publishOn(scheduler).doOnNext(this::appendToDebugFile).subscribe();
    return processor.sink();
  }

  private void appendToBadFile(Record record, String file) {
    Path logFile = executionDirectory.resolve(file);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    Object source = record.getSource();
    printAndMaybeAddNewLine(source == null ? null : source.toString(), writer);
    writer.flush();
  }

  private void appendToDebugFile(WriteResult result) {
    Path logFile = executionDirectory.resolve(LOAD_ERRORS_FILE);
    appendToDebugFile(result, logFile);
  }

  private void appendToDebugFile(ReadResult result) {
    Path logFile = executionDirectory.resolve(UNLOAD_ERRORS_FILE);
    appendToDebugFile(result, logFile);
  }

  private void appendToDebugFile(Result result, Path logFile) {
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    writer.print("Statement: ");
    String format =
        formatter.format(result.getStatement(), verbosity, protocolVersion, codecRegistry);
    printAndMaybeAddNewLine(format, writer);
    stackTracePrinter.printStackTrace(
        result.getError().orElseThrow(IllegalStateException::new), writer);
  }

  private void appendToDebugFile(UnmappableStatement statement) {
    Path logFile = executionDirectory.resolve(MAPPING_ERRORS_FILE);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    appendStatementInfo(statement, writer);
    stackTracePrinter.printStackTrace(statement.getError(), writer);
  }

  private void appendToDebugFile(ErrorRecord record, String file) {
    Path logFile = executionDirectory.resolve(file);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    appendRecordInfo(record, writer);
    stackTracePrinter.printStackTrace(record.getError(), writer);
  }

  private static void appendToPositionsFile(
      URI resource, List<Range<Long>> positions, PrintWriter positionsPrinter) {
    positionsPrinter.print(resource);
    positionsPrinter.print(':');
    positions.stream().findFirst().ifPresent(pos -> positionsPrinter.print(pos.upperEndpoint()));
    positionsPrinter.println();
  }

  @NotNull
  @VisibleForTesting
  static List<Range<Long>> addPosition(@NotNull List<Range<Long>> positions, long position) {
    ListIterator<Range<Long>> iterator = positions.listIterator();
    while (iterator.hasNext()) {
      Range<Long> range = iterator.next();
      if (range.contains(position)) {
        return positions;
      } else if (range.upperEndpoint() + 1L == position) {
        range = Range.closed(range.lowerEndpoint(), position);
        iterator.set(range);
        if (iterator.hasNext()) {
          Range<Long> next = iterator.next();
          if (range.upperEndpoint() == next.lowerEndpoint() - 1) {
            iterator.remove();
            iterator.previous();
            iterator.set(Range.closed(range.lowerEndpoint(), next.upperEndpoint()));
          }
        }
        return positions;
      } else if (range.lowerEndpoint() - 1L == position) {
        range = Range.closed(position, range.upperEndpoint());
        iterator.set(range);
        return positions;
      } else if (position < range.lowerEndpoint()) {
        iterator.previous();
        iterator.add(Range.singleton(position));
        return positions;
      }
    }
    iterator.add(Range.singleton(position));
    return positions;
  }

  @NotNull
  @VisibleForTesting
  static List<Range<Long>> mergePositions(
      @NotNull List<Range<Long>> positions1, @NotNull List<Range<Long>> positions2) {
    if (positions1.isEmpty()) {
      return positions2;
    }
    if (positions2.isEmpty()) {
      return positions1;
    }
    List<Range<Long>> merged = new ArrayList<>();
    ListIterator<Range<Long>> iterator1 = positions1.listIterator();
    ListIterator<Range<Long>> iterator2 = positions2.listIterator();
    Range<Long> previous = null;
    while (true) {
      Range<Long> current = nextRange(iterator1, iterator2);
      if (current == null) {
        merged.add(previous);
        break;
      }
      if (previous == null) {
        previous = current;
      } else if (isContiguous(previous, current)) {
        previous = previous.span(current);
      } else {
        merged.add(previous);
        previous = current;
      }
    }
    return merged;
  }

  private static boolean isContiguous(Range<Long> range1, Range<Long> range2) {
    return range1.lowerEndpoint() - range2.upperEndpoint() <= 1
        && range2.lowerEndpoint() - range1.upperEndpoint() <= 1;
  }

  private static Range<Long> nextRange(
      ListIterator<Range<Long>> iterator1, ListIterator<Range<Long>> iterator2) {
    Range<Long> range1 = null;
    Range<Long> range2 = null;
    if (iterator1.hasNext()) {
      range1 = iterator1.next();
    }
    if (iterator2.hasNext()) {
      range2 = iterator2.next();
    }
    if (range1 == null && range2 == null) {
      return null;
    }
    if (range1 == null) {
      return range2;
    } else if (range2 == null) {
      return range1;
    } else if (range1.lowerEndpoint() < range2.lowerEndpoint()) {
      iterator2.previous();
      return range1;
    } else {
      iterator1.previous();
      return range2;
    }
  }

  private static int delta(Statement statement) {
    if (statement instanceof BatchStatement) {
      return ((BatchStatement) statement).size();
    } else {
      return 1;
    }
  }

  private <T> Signal<T> maybeTriggerOnError(Signal<T> signal, int errorCount) {
    TooManyErrorsException exception;
    if (isPercentageBased()) {
      exception = maxPercentageExceeded(errorCount);
    } else {
      exception = maxErrorCountExceeded(errorCount);
    }
    if (exception != null) {
      return Signal.error(exception);
    }
    return signal;
  }

  private boolean isPercentageBased() {
    return maxErrorRatio != 0;
  }

  private TooManyErrorsException maxErrorCountExceeded(int errorCount) {
    if (maxErrors > 0 && errorCount > maxErrors) {
      return new TooManyErrorsException(maxErrors);
    }
    return null;
  }

  private TooManyErrorsException maxPercentageExceeded(int errorCount) {
    long attemptedTemp = totalItems.longValue();
    float currentRatio = (float) errorCount / attemptedTemp;
    if (attemptedTemp > MIN_SAMPLE && currentRatio > maxErrorRatio) {
      return new TooManyErrorsException(maxErrorRatio);
    }
    return null;
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
      writer.println(throwableProxyToString(new ThrowableProxy(t)));
      writer.flush();
    }
  }
}
