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
package com.datastax.oss.dsbulk.workflow.commons.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.stream.Collectors.toList;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.pattern.ThrowableProxyConverter;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.dsbulk.connectors.api.ErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.executor.api.result.Result;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.dsbulk.format.row.RowFormatter;
import com.datastax.oss.dsbulk.format.statement.StatementFormatVerbosity;
import com.datastax.oss.dsbulk.format.statement.StatementFormatter;
import com.datastax.oss.dsbulk.mapping.InvalidMappingException;
import com.datastax.oss.dsbulk.workflow.api.error.ErrorThreshold;
import com.datastax.oss.dsbulk.workflow.api.error.TooManyErrorsException;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultMapper;
import com.datastax.oss.dsbulk.workflow.commons.schema.RecordMapper;
import com.datastax.oss.dsbulk.workflow.commons.settings.LogSettings;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.UnmappableStatement;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
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
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

// FIXME UnicastProcessor and FluxSink were deprecated, but the Sinks.Many API does not offer
// support for multi-threaded access to the sink, see:
// https://stackoverflow.com/questions/65029619/how-to-call-sinks-manyt-tryemitnext-from-multiple-threads
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

  private final CqlSession session;
  private final Path operationDirectory;
  private final ErrorThreshold errorThreshold;
  private final ErrorThreshold queryWarningsThreshold;
  private final boolean trackPositions;
  private final StatementFormatter statementFormatter;
  private final StatementFormatVerbosity statementFormatVerbosity;
  private final RowFormatter rowFormatter;

  private final AtomicInteger errors = new AtomicInteger(0);
  private final LongAdder totalItems = new LongAdder();

  private final AtomicInteger queryWarnings = new AtomicInteger(0);
  private final AtomicBoolean queryWarningsEnabled = new AtomicBoolean(true);

  private final LoadingCache<Path, PrintWriter> openFiles =
      Caffeine.newBuilder()
          .build(path -> new PrintWriter(Files.newBufferedWriter(path, UTF_8, CREATE_NEW, WRITE)));

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
      CqlSession session,
      Path operationDirectory,
      ErrorThreshold errorThreshold,
      ErrorThreshold queryWarningsThreshold,
      boolean trackPositions,
      StatementFormatter statementFormatter,
      StatementFormatVerbosity statementFormatVerbosity,
      RowFormatter rowFormatter) {
    this.session = session;
    this.operationDirectory = operationDirectory;
    this.errorThreshold = errorThreshold;
    this.queryWarningsThreshold = queryWarningsThreshold;
    this.trackPositions = trackPositions;
    this.statementFormatter = statementFormatter;
    this.statementFormatVerbosity = statementFormatVerbosity;
    this.rowFormatter = rowFormatter;
  }

  public void init() {
    codecRegistry = session.getContext().getCodecRegistry();
    protocolVersion = session.getContext().getProtocolVersion();
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

  public Path getOperationDirectory() {
    return operationDirectory;
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
    if (trackPositions && !positionsTracker.isEmpty()) {
      positionsPrinter =
          new PrintWriter(
              Files.newBufferedWriter(
                  operationDirectory.resolve(POSITIONS_FILE), UTF_8, CREATE_NEW, WRITE));
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
  @NonNull
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
   * Handler for unmappable statements produced by the {@linkplain RecordMapper record mapper}.
   *
   * <p>Used only in load workflows.
   *
   * <p>Increments the number of errors and forwards unmappable statements to the unmappable
   * statement processor for further processing.
   *
   * @return a handler for unmappable statements.
   */
  @NonNull
  public Function<Flux<BatchableStatement<?>>, Flux<BatchableStatement<?>>>
      newUnmappableStatementsHandler() {
    return upstream ->
        upstream.flatMap(
            stmt -> {
              if (stmt instanceof UnmappableStatement) {
                try {
                  unmappableStatementSink.next((UnmappableStatement) stmt);
                  return maybeTriggerOnError(null, errors.incrementAndGet());
                } catch (Exception e) {
                  return Flux.error(e);
                }
              } else {
                return Flux.just(stmt);
              }
            },
            1,
            1);
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
  @NonNull
  public Function<Flux<Record>, Flux<Record>> newFailedRecordsHandler() {
    return upstream ->
        upstream.flatMap(
            r -> {
              if (r instanceof ErrorRecord) {
                try {
                  failedRecordSink.next((ErrorRecord) r);
                  return maybeTriggerOnError(null, errors.incrementAndGet());
                } catch (Exception e) {
                  return Flux.error(e);
                }
              } else {
                return Flux.just(r);
              }
            },
            1,
            1);
  }

  /**
   * Handler for unmappable records produced by the {@linkplain ReadResultMapper result mapper}.
   *
   * <p>Used only in unload workflows.
   *
   * <p>Increments the number of errors and forwards unmappable records to the unmappable record
   * processor for further processing.
   *
   * @return a handler for unmappable records.
   */
  @NonNull
  public Function<Flux<Record>, Flux<Record>> newUnmappableRecordsHandler() {
    return upstream ->
        upstream.flatMap(
            r -> {
              if (r instanceof ErrorRecord) {
                try {
                  unmappableRecordSink.next((ErrorRecord) r);
                  return maybeTriggerOnError(null, errors.incrementAndGet());
                } catch (Exception e) {
                  return Flux.error(e);
                }
              } else {
                return Flux.just(r);
              }
            },
            1,
            1);
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
  @NonNull
  public Function<Flux<WriteResult>, Flux<WriteResult>> newFailedWritesHandler() {
    return upstream ->
        upstream.flatMap(
            r -> {
              try {
                if (!r.isSuccess()) {
                  failedWriteSink.next(r);
                  assert r.getError().isPresent();
                  Throwable cause = r.getError().get().getCause();
                  return maybeTriggerOnError(cause, errors.addAndGet(r.getBatchSize()));
                } else if (!r.wasApplied()) {
                  failedCASWriteSink.next(r);
                  return maybeTriggerOnError(null, errors.addAndGet(r.getBatchSize()));
                } else {
                  return Flux.just(r);
                }
              } catch (Exception e) {
                return Flux.error(e);
              }
            },
            1,
            1);
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
  @NonNull
  public Function<Flux<ReadResult>, Flux<ReadResult>> newFailedReadsHandler() {
    return upstream ->
        upstream.flatMap(
            r -> {
              if (r.isSuccess()) {
                return Flux.just(r);
              } else {
                try {
                  failedReadSink.next(r);
                  assert r.getError().isPresent();
                  Throwable cause = r.getError().get().getCause();
                  return maybeTriggerOnError(cause, errors.incrementAndGet());
                } catch (Exception e) {
                  return Flux.error(e);
                }
              }
            },
            1,
            1);
  }
  /**
   * Handler for query warnings.
   *
   * <p>Used by all workflows.
   *
   * <p>Increments the number of query warnings; if the threshold is exceeded, logs one last warning
   * than mutes subsequent query warnings.
   *
   * @return a handler for for query warnings.
   */
  public <T extends Result> Function<Flux<T>, Flux<T>> newQueryWarningsHandler() {
    return upstream ->
        upstream.doOnNext(
            result -> {
              if (queryWarningsEnabled.get()) {
                result.getExecutionInfo().ifPresent(this::maybeLogQueryWarnings);
              }
            });
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
            .flatMap(this::sendToPositionsSink)
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
   * <p>Note that all non-batch statements are required to be of type {@link MappedStatement}.
   *
   * @return a mapper from statements to records.
   */
  @NonNull
  private Function<Flux<? extends Statement<?>>, Flux<Record>> newStatementToRecordMapper() {
    return upstream ->
        upstream
            .flatMap(
                statement -> {
                  if (statement instanceof BatchStatement) {
                    return Flux.fromIterable(((BatchStatement) statement));
                  } else {
                    return Flux.just(statement);
                  }
                })
            .cast(MappedStatement.class)
            .map(MappedStatement::getRecord);
  }

  /**
   * A sink for failed records. A failed record is a record that the connector could not read or
   * write.
   *
   * <p>Used in both load and unload workflows.
   *
   * <p>Appends the record to the debug file, then (for load workflows only) to the bad file and
   * forwards the record's position to the position tracker.
   *
   * @return A processor for failed records.
   */
  @NonNull
  private FluxSink<ErrorRecord> newFailedRecordSink() {
    UnicastProcessor<ErrorRecord> processor = UnicastProcessor.create();
    Flux<Record> flux = processor.flatMap(this::appendFailedRecordToDebugFile);
    if (trackPositions) {
      flux =
          flux.flatMap(record -> appendToBadFile(record, CONNECTOR_BAD_FILE))
              .flatMap(this::sendToPositionsSink);
    }
    flux.subscribe(v -> {}, this::onSinkError);
    return processor.sink(OverflowStrategy.BUFFER);
  }

  /**
   * A sink for unmappable records produced by the {@linkplain ReadResultMapper result mapper}.
   *
   * <p>Used only in unload workflows.
   *
   * <p>Appends the record to the debug file.
   *
   * @return A processor for unmappable records.
   */
  @NonNull
  private FluxSink<ErrorRecord> newUnmappableRecordSink() {
    UnicastProcessor<ErrorRecord> processor = UnicastProcessor.create();
    processor
        .flatMap(this::appendUnmappableReadResultToDebugFile)
        .subscribe(v -> {}, this::onSinkError);
    return processor.sink(OverflowStrategy.BUFFER);
  }

  /**
   * A sink for unmappable statements produced by the {@linkplain RecordMapper record mapper}.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the statement to the debug file, then extracts its record, appends it to the bad
   * file, then forwards the record's position to the position tracker.
   *
   * @return A processor for unmappable statements.
   */
  @NonNull
  private FluxSink<UnmappableStatement> newUnmappableStatementSink() {
    UnicastProcessor<UnmappableStatement> processor = UnicastProcessor.create();
    processor
        .doOnNext(this::maybeWarnInvalidMapping)
        .flatMap(this::appendUnmappableStatementToDebugFile)
        .transform(newStatementToRecordMapper())
        .flatMap(record -> appendToBadFile(record, MAPPING_BAD_FILE))
        .flatMap(this::sendToPositionsSink)
        .subscribe(v -> {}, this::onSinkError);
    return processor.sink(OverflowStrategy.BUFFER);
  }

  /**
   * A sink for failed write results.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the failed result to the debug file, then extracts its statement, then extracts its
   * record, then appends it to the bad file, then forwards the record's position to the position
   * tracker.
   *
   * @return A processor for failed write results.
   */
  @NonNull
  private FluxSink<WriteResult> newFailedWriteResultSink() {
    UnicastProcessor<WriteResult> processor = UnicastProcessor.create();
    processor
        .flatMap(this::appendFailedWriteResultToDebugFile)
        .map(Result::getStatement)
        .transform(newStatementToRecordMapper())
        .flatMap(record -> appendToBadFile(record, LOAD_BAD_FILE))
        .flatMap(this::sendToPositionsSink)
        .subscribe(v -> {}, this::onSinkError);
    return processor.sink(OverflowStrategy.BUFFER);
  }

  /**
   * A sink for failed CAS write results.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the failed result to the debug file, then extracts its statement, then extracts its
   * record, then appends it to the bad file, then forwards the record's position to the position
   * tracker.
   *
   * @return A processor for failed CAS write results.
   */
  @NonNull
  private FluxSink<WriteResult> newFailedCASWriteSink() {
    UnicastProcessor<WriteResult> processor = UnicastProcessor.create();
    processor
        .flatMap(this::appendFailedCASWriteResultToDebugFile)
        .map(Result::getStatement)
        .transform(newStatementToRecordMapper())
        .flatMap(record -> appendToBadFile(record, CAS_BAD_FILE))
        .flatMap(this::sendToPositionsSink)
        .subscribe(v -> {}, this::onSinkError);
    return processor.sink(OverflowStrategy.BUFFER);
  }

  /**
   * A sink for failed read results.
   *
   * <p>Used only in the unload workflow.
   *
   * <p>Extracts the statement, then appends it to the debug file.
   *
   * @return A processor for failed read results.
   */
  @NonNull
  private FluxSink<ReadResult> newFailedReadResultSink() {
    UnicastProcessor<ReadResult> processor = UnicastProcessor.create();
    processor
        .flatMap(this::appendFailedReadResultToDebugFile)
        .subscribe(v -> {}, this::onSinkError);
    return processor.sink(OverflowStrategy.BUFFER);
  }

  /**
   * A sink for record positions.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Updates the position tracker for every record received.
   *
   * @return A processor for record positions.
   */
  @NonNull
  private FluxSink<Record> newPositionsSink() {
    UnicastProcessor<Record> processor = UnicastProcessor.create();
    processor
        // do not need to be published on the dedicated log scheduler, the computation is fairly
        // cheap
        .doOnNext(record -> positionsTracker.update(record.getResource(), record.getPosition()))
        .subscribe(v -> {}, this::onSinkError);
    return processor.sink(OverflowStrategy.BUFFER);
  }

  // Bad file management
  @SuppressWarnings("BlockingMethodInNonBlockingContext")
  private Mono<Record> appendToBadFile(Record record, String file) {
    try {
      Object source = record.getSource();
      if (source != null) {
        Path logFile = operationDirectory.resolve(file);
        PrintWriter writer = openFiles.get(logFile);
        assert writer != null;
        LogManagerUtils.printAndMaybeAddNewLine(source.toString(), writer);
        writer.flush();
      }
      return Mono.just(record);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  // Executor errors (read/write failures)

  // write query failed
  private Mono<WriteResult> appendFailedWriteResultToDebugFile(WriteResult result) {
    return appendStatement(result, LOAD_ERRORS_FILE, true);
  }

  // CAS write query failed
  private Mono<WriteResult> appendFailedCASWriteResultToDebugFile(WriteResult result) {
    return appendStatement(result, CAS_ERRORS_FILE, true);
  }

  // read query failed
  private Mono<ReadResult> appendFailedReadResultToDebugFile(ReadResult result) {
    return appendStatement(result, UNLOAD_ERRORS_FILE, true);
  }

  @SuppressWarnings("BlockingMethodInNonBlockingContext")
  private <R extends Result> Mono<R> appendStatement(
      R result, String logFileName, boolean appendNewLine) {
    try {
      Path logFile = operationDirectory.resolve(logFileName);
      PrintWriter writer = openFiles.get(logFile);
      assert writer != null;
      writer.print("Statement: ");
      String format =
          statementFormatter.format(
              result.getStatement(), statementFormatVerbosity, protocolVersion, codecRegistry);
      LogManagerUtils.printAndMaybeAddNewLine(format, writer);
      if (result instanceof WriteResult) {
        WriteResult writeResult = (WriteResult) result;
        // If a conditional update could not be applied, print the failed mutations
        if (writeResult.isSuccess() && !writeResult.wasApplied()) {
          writer.println("Failed conditional updates: ");
          writeResult
              .getFailedWrites()
              .forEach(
                  row -> {
                    String failed = rowFormatter.format(row, protocolVersion, codecRegistry);
                    LogManagerUtils.printAndMaybeAddNewLine(failed, writer);
                  });
        }
      }
      if (result.getError().isPresent()) {
        stackTracePrinter.printStackTrace(result.getError().get(), writer);
      }
      if (appendNewLine) {
        writer.println();
      }
      writer.flush();
      return Mono.just(result);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  // Mapping errors (failed record -> statement or row -> record mappings)

  // record -> statement failed (load workflow)
  @SuppressWarnings("BlockingMethodInNonBlockingContext")
  private Mono<UnmappableStatement> appendUnmappableStatementToDebugFile(
      UnmappableStatement statement) {
    try {
      Path logFile = operationDirectory.resolve(MAPPING_ERRORS_FILE);
      PrintWriter writer = openFiles.get(logFile);
      assert writer != null;
      Record record = statement.getRecord();
      writer.println("Resource: " + record.getResource());
      writer.println("Position: " + record.getPosition());
      if (record.getSource() != null) {
        writer.println("Source: " + LogManagerUtils.formatSource(record));
      }
      stackTracePrinter.printStackTrace(statement.getError(), writer);
      writer.println();
      writer.flush();
      return Mono.just(statement);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  // row -> record failed (unload workflow)
  @SuppressWarnings("BlockingMethodInNonBlockingContext")
  private Mono<ErrorRecord> appendUnmappableReadResultToDebugFile(ErrorRecord record) {
    try {
      Path logFile = operationDirectory.resolve(MAPPING_ERRORS_FILE);
      PrintWriter writer = openFiles.get(logFile);
      assert writer != null;
      // Don't print the resource since it will be just cql://keyspace/table
      if (record.getSource() instanceof ReadResult) {
        ReadResult source = (ReadResult) record.getSource();
        appendStatement(source, MAPPING_ERRORS_FILE, false);
        source
            .getRow()
            .ifPresent(
                row -> {
                  writer.print("Row: ");
                  String format = rowFormatter.format(row, protocolVersion, codecRegistry);
                  LogManagerUtils.printAndMaybeAddNewLine(format, writer);
                });
      }
      stackTracePrinter.printStackTrace(record.getError(), writer);
      writer.println();
      writer.flush();
      return Mono.just(record);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  // Connector errors
  @SuppressWarnings("BlockingMethodInNonBlockingContext")
  private Mono<ErrorRecord> appendFailedRecordToDebugFile(ErrorRecord record) {
    try {
      Path logFile = operationDirectory.resolve(CONNECTOR_ERRORS_FILE);
      PrintWriter writer = openFiles.get(logFile);
      assert writer != null;
      writer.println("Resource: " + record.getResource());
      writer.println("Position: " + record.getPosition());
      if (record.getSource() != null) {
        writer.println("Source: " + LogManagerUtils.formatSource(record));
      }
      stackTracePrinter.printStackTrace(record.getError(), writer);
      writer.println();
      writer.flush();
      return Mono.just(record);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  @NonNull
  private Mono<Record> sendToPositionsSink(Record record) {
    try {
      positionsSink.next(record);
      return Mono.just(record);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  // Utility methods

  private static void appendToPositionsFile(
      URI resource, List<Range> positions, PrintWriter positionsPrinter) {
    positionsPrinter.print(resource);
    positionsPrinter.print(':');
    positions.stream().findFirst().ifPresent(pos -> positionsPrinter.print(pos.getUpper()));
    positionsPrinter.println();
  }

  private <T> Flux<T> maybeTriggerOnError(@Nullable Throwable error, int currentErrorCount) {
    if (error != null && isUnrecoverable(error)) {
      return Flux.error(error);
    } else if (errorThreshold.checkThresholdExceeded(currentErrorCount, totalItems)) {
      return Flux.error(new TooManyErrorsException(errorThreshold));
    } else {
      // filter out the failed element
      return Flux.empty();
    }
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

  private void maybeLogQueryWarnings(ExecutionInfo info) {
    for (String warning : info.getWarnings()) {
      if (queryWarningsThreshold.checkThresholdExceeded(
          queryWarnings.incrementAndGet(), totalItems)) {
        queryWarningsEnabled.set(false);
        LOGGER.warn(
            "The maximum number of logged query warnings has been exceeded ({}); "
                + "subsequent warnings will not be logged.",
            queryWarningsThreshold.thresholdAsString());
        break;
      } else {
        LOGGER.warn("Query generated server-side warning: " + warning);
      }
    }
  }

  private void onSinkError(Throwable error) {
    LOGGER.error("Error while writing to log files, aborting", error);
    uncaughtExceptionSink.error(error);
  }

  private static boolean isUnrecoverable(Throwable error) {
    if (error instanceof AllNodesFailedException) {
      for (Throwable child :
          ((AllNodesFailedException) error)
              .getAllErrors().values().stream()
                  .flatMap(List::stream)
                  .collect(Collectors.toList())) {
        if (isUnrecoverable(child)) {
          return true;
        }
      }
      return false;
    }
    return !(error instanceof ServerError
        || error instanceof QueryExecutionException
        || error instanceof InvalidQueryException
        || error instanceof DriverTimeoutException
        || error instanceof RequestThrottlingException
        || error instanceof FrameTooLongException
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
