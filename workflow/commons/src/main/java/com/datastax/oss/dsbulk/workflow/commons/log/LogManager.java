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
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.dsbulk.connectors.api.ErrorRecord;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.executor.api.result.Result;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.dsbulk.format.row.RowFormatter;
import com.datastax.oss.dsbulk.format.statement.StatementFormatVerbosity;
import com.datastax.oss.dsbulk.format.statement.StatementFormatter;
import com.datastax.oss.dsbulk.workflow.api.error.ErrorThreshold;
import com.datastax.oss.dsbulk.workflow.api.error.TooManyErrorsException;
import com.datastax.oss.dsbulk.workflow.commons.schema.InvalidMappingException;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultMapper;
import com.datastax.oss.dsbulk.workflow.commons.schema.RecordMapper;
import com.datastax.oss.dsbulk.workflow.commons.settings.LogSettings;
import com.datastax.oss.dsbulk.workflow.commons.statement.MappedStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadStatement;
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
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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

  private static final String SUMMARY_CSV = "summary.csv";

  private final CqlSession session;
  private final Path operationDirectory;
  private final ErrorThreshold errorThreshold;
  private final ErrorThreshold queryWarningsThreshold;
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

  private Scheduler positionsTrackerScheduler;

  private CodecRegistry codecRegistry;
  private ProtocolVersion protocolVersion;

  private StackTracePrinter stackTracePrinter;

  private final Deque<PositionTracker> positionTrackers = new ConcurrentLinkedDeque<>();
  private final PositionTracker failedPositionTracker = new PositionTracker();

  @VisibleForTesting final Map<URI, ResourceStats> resources = new ConcurrentHashMap<>();

  private FluxSink<ErrorRecord> failedRecordSink;
  private FluxSink<ErrorRecord> unmappableRecordSink;
  private FluxSink<UnmappableStatement> unmappableStatementSink;
  private FluxSink<WriteResult> failedWriteSink;
  private FluxSink<WriteResult> failedCASWriteSink;
  private FluxSink<ReadResult> failedReadSink;
  private FluxSink<Record> failedPositionsSink;

  private UnicastProcessor<Void> uncaughtExceptionProcessor;
  private FluxSink<Void> uncaughtExceptionSink;

  private AtomicBoolean invalidMappingWarningDone;

  public LogManager(
      CqlSession session,
      Path operationDirectory,
      ErrorThreshold errorThreshold,
      ErrorThreshold queryWarningsThreshold,
      StatementFormatter statementFormatter,
      StatementFormatVerbosity statementFormatVerbosity,
      RowFormatter rowFormatter) {
    this.session = session;
    this.operationDirectory = operationDirectory;
    this.errorThreshold = errorThreshold;
    this.queryWarningsThreshold = queryWarningsThreshold;
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
    failedRecordSink = newFailedRecordSink();
    unmappableRecordSink = newUnmappableRecordSink();
    unmappableStatementSink = newUnmappableStatementSink();
    failedWriteSink = newFailedWriteResultSink();
    failedCASWriteSink = newFailedCASWriteSink();
    failedReadSink = newFailedReadResultSink();
    failedPositionsSink = newFailedPositionsSink();
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
    int numCores = Runtime.getRuntime().availableProcessors();
    positionsTrackerScheduler =
        Schedulers.newParallel("positions-tracker", Math.max(2, numCores / 4));
    for (int i = 0; i < numCores * 2; i++) {
      positionTrackers.offer(new PositionTracker());
    }
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
    failedPositionsSink.complete();
    uncaughtExceptionSink.complete();
    stackTracePrinter.stop();
    positionsTrackerScheduler.dispose();
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
  }

  public void reportAvailableFiles() throws IOException {
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
    if (!resources.isEmpty()) {
      writeSummaryFile();
      LOGGER.info("A summary of the operation in CSV format can be found in {}.", SUMMARY_CSV);
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
   * Handler for successful write results.
   *
   * <p>Used only by the load workflow.
   *
   * <p>Extracts the result's {@link Record} and updates the positions.
   *
   * @return A handler for successful write results.
   */
  public Function<Flux<WriteResult>, Flux<Void>> newWriteResultPositionsHandler() {
    return upstream ->
        upstream
            .map(Result::getStatement)
            .transform(this::extractRecordFromMappedStatement)
            .transform(this::recordSuccessfulRecordPositions);
  }

  /**
   * Handler for successful read results.
   *
   * <p>Used only by the count workflow.
   *
   * <p>Extracts the result's {@link Record} and updates the positions.
   *
   * @return A handler for successful read results.
   */
  public Function<Flux<ReadResult>, Flux<Void>> newReadResultPositionsHandler() {
    return upstream -> upstream.transform(this::recordSuccessfulReadResultPositions);
  }

  /**
   * Handler for successful record positions.
   *
   * <p>Used only by the unload workflow.
   *
   * <p>Updates the positions.
   *
   * @return A handler for successful record positions.
   */
  public Function<Flux<Record>, Flux<Void>> newRecordPositionsHandler() {
    return upstream -> upstream.transform(this::recordSuccessfulRecordPositions);
  }

  public <T> Function<Flux<T>, Flux<T>> newTotalItemsCounter() {
    return upstream -> upstream.doOnNext(r -> totalItems.increment());
  }

  public BiFunction<URI, Publisher<Record>, Publisher<Record>> newConnectorResourceStatsHandler() {
    return (resource, upstream) ->
        Flux.from(upstream)
            .transformDeferredContextual(
                (original, ctx) -> {
                  ResourceStats stats = ctx.get(ResourceStats.class);
                  return original
                      .doOnSubscribe(s -> resources.put(resource, stats))
                      .doOnComplete(() -> stats.completed = true)
                      .doOnError(error -> stats.failed = true)
                      // increment even for failed records since they will be considered processed
                      // and will increment the position tracker.
                      .doOnNext(r -> stats.produced++);
                })
            .contextWrite(ctx -> ctx.put(ResourceStats.class, new ResourceStats()));
  }

  public BiFunction<URI, Publisher<ReadResult>, Publisher<ReadResult>>
      newCqlResourceStatsHandler() {
    return (resource, upstream) ->
        Flux.from(upstream)
            .transformDeferredContextual(
                (original, ctx) -> {
                  ResourceStats stats = ctx.get(ResourceStats.class);
                  return original
                      .doOnSubscribe(s -> resources.put(resource, stats))
                      .doOnComplete(() -> stats.completed = true)
                      // onError signals should not happen, since the executor is in failsafe mode
                      .doOnError(error -> stats.failed = true)
                      .doOnNext(
                          r -> {
                            if (r.isSuccess()) {
                              stats.produced++;
                            } else {
                              // read failures are global to the entire token range and don't
                              // increment the position tracker, so don't increment counter of rows,
                              // but instead signal that the entire resource failed.
                              stats.failed = true;
                            }
                          });
                })
            .contextWrite(ctx -> ctx.put(ResourceStats.class, new ResourceStats()));
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
  private Flux<Record> extractRecordFromMappedStatement(Flux<? extends Statement<?>> upstream) {
    return upstream
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
   * <p>Appends the record to the debug file, then to the connector bad file and forwards the
   * record's position to the position tracker.
   *
   * @return A processor for failed records.
   */
  @NonNull
  private FluxSink<ErrorRecord> newFailedRecordSink() {
    UnicastProcessor<ErrorRecord> processor = UnicastProcessor.create();
    processor
        .flatMap(this::appendFailedRecordToDebugFile)
        .flatMap(record -> appendToBadFile(record, CONNECTOR_BAD_FILE))
        .flatMap(this::sendToFailedPositionsSink)
        .subscribe(v -> {}, this::onSinkError);
    return processor.sink(OverflowStrategy.BUFFER);
  }

  /**
   * A sink for unmappable records produced by the {@linkplain ReadResultMapper result mapper}.
   *
   * <p>Used only in unload workflows.
   *
   * <p>Appends the record to the debug file, to the bad file, then send the record to the positions
   * sink.
   *
   * @return A processor for unmappable records.
   */
  @NonNull
  private FluxSink<ErrorRecord> newUnmappableRecordSink() {
    UnicastProcessor<ErrorRecord> processor = UnicastProcessor.create();
    processor
        .flatMap(this::appendUnmappableReadResultToDebugFile)
        .flatMap(record -> appendToBadFile(record, MAPPING_BAD_FILE))
        .flatMap(this::sendToFailedPositionsSink)
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
        .transform(this::extractRecordFromMappedStatement)
        .flatMap(record -> appendToBadFile(record, MAPPING_BAD_FILE))
        .flatMap(this::sendToFailedPositionsSink)
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
        .transform(this::extractRecordFromMappedStatement)
        .flatMap(record -> appendToBadFile(record, LOAD_BAD_FILE))
        .flatMap(this::sendToFailedPositionsSink)
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
        .transform(this::extractRecordFromMappedStatement)
        .flatMap(record -> appendToBadFile(record, CAS_BAD_FILE))
        .flatMap(this::sendToFailedPositionsSink)
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
        // no bad file nor positions tracker for failed reads
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
  private FluxSink<Record> newFailedPositionsSink() {
    UnicastProcessor<Record> processor = UnicastProcessor.create();
    processor
        // do not need to be published on the dedicated log scheduler, the computation is fairly
        // cheap, and we don't expect tons of failed records.
        .doOnNext(
            record -> failedPositionTracker.update(record.getResource(), record.getPosition()))
        .subscribe(v -> {}, this::onSinkError);
    return processor.sink(OverflowStrategy.BUFFER);
  }

  @NonNull
  private Flux<Void> recordSuccessfulRecordPositions(Flux<Record> upstream) {
    return upstream
        .window(1000)
        .flatMap(
            records ->
                records
                    .transformDeferredContextual(
                        (original, ctx) -> {
                          PositionTracker positionTracker = ctx.get(PositionTracker.class);
                          return original
                              .doOnNext(
                                  record ->
                                      positionTracker.update(
                                          record.getResource(), record.getPosition()))
                              .doOnTerminate(() -> positionTrackers.offer(positionTracker));
                        })
                    .contextWrite(ctx -> ctx.put(PositionTracker.class, positionTrackers.remove()))
                    .subscribeOn(positionsTrackerScheduler),
            Runtime.getRuntime().availableProcessors() * 2)
        .then()
        .flux();
  }

  private Flux<Void> recordSuccessfulReadResultPositions(Flux<ReadResult> upstream) {
    return upstream
        .window(1000)
        .flatMap(
            results ->
                results
                    .transformDeferredContextual(
                        (original, ctx) -> {
                          PositionTracker positionTracker = ctx.get(PositionTracker.class);
                          return original
                              .doOnNext(
                                  result -> {
                                    URI resource =
                                        ((RangeReadStatement) result.getStatement()).getResource();
                                    long position = result.getPosition();
                                    positionTracker.update(resource, position);
                                  })
                              .doOnTerminate(() -> positionTrackers.offer(positionTracker));
                        })
                    .contextWrite(ctx -> ctx.put(PositionTracker.class, positionTrackers.remove()))
                    .subscribeOn(positionsTrackerScheduler),
            Runtime.getRuntime().availableProcessors() * 2)
        .then()
        .flux();
  }

  // Bad file management
  private Mono<Record> appendToBadFile(Record record, String file) {
    return Mono.just(record)
        .handle(
            (r, sink) -> {
              try {
                doAppendToBadFile(r, file);
                sink.next(r);
              } catch (Exception e) {
                sink.error(e);
              }
            });
  }

  private void doAppendToBadFile(Record record, String file) {
    Object source = record.getSource();
    if (source != null) {
      Path logFile = operationDirectory.resolve(file);
      PrintWriter writer = openFiles.get(logFile);
      assert writer != null;
      if (source instanceof ReadResult) {
        ((ReadResult) source)
            .getRow()
            .ifPresent(
                row -> {
                  // In a bad file we must keep each element in one line, so use
                  // getFormattedContents instead of rowFormatter
                  LogManagerUtils.printAndMaybeAddNewLine(row.getFormattedContents(), writer);
                });
      } else {
        LogManagerUtils.printAndMaybeAddNewLine(source.toString(), writer);
      }
      writer.flush();
    }
  }

  // Executor errors (read/write failures)

  // write query failed
  private Mono<WriteResult> appendFailedWriteResultToDebugFile(WriteResult result) {
    return appendStatement(result, LOAD_ERRORS_FILE);
  }

  // CAS write query failed
  private Mono<WriteResult> appendFailedCASWriteResultToDebugFile(WriteResult result) {
    return appendStatement(result, CAS_ERRORS_FILE);
  }

  // read query failed
  private Mono<ReadResult> appendFailedReadResultToDebugFile(ReadResult result) {
    return appendStatement(result, UNLOAD_ERRORS_FILE);
  }

  private <R extends Result> Mono<R> appendStatement(R result, String logFileName) {
    return Mono.just(result)
        .handle(
            (r, sink) -> {
              try {
                doAppendStatement(r, logFileName, true);
                sink.next(r);
              } catch (Exception e) {
                sink.error(e);
              }
            });
  }

  private <R extends Result> void doAppendStatement(
      R result, String logFileName, boolean appendNewLine) {
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
  }

  // Mapping errors (failed record -> statement or row -> record mappings)

  // record -> statement failed (load workflow)
  private Mono<UnmappableStatement> appendUnmappableStatementToDebugFile(
      UnmappableStatement statement) {
    return Mono.just(statement)
        .handle(
            (s, sink) -> {
              try {
                doAppendUnmappableStatementToDebugFile(s);
                sink.next(s);
              } catch (Exception e) {
                sink.error(e);
              }
            });
  }

  private void doAppendUnmappableStatementToDebugFile(UnmappableStatement statement) {
    Path logFile = operationDirectory.resolve(MAPPING_ERRORS_FILE);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    Record record = statement.getRecord();
    appendResourceAndPosition(writer, record);
    if (record.getSource() != null) {
      writer.println("Source: " + LogManagerUtils.formatSource(record));
    }
    stackTracePrinter.printStackTrace(statement.getError(), writer);
    writer.println();
    writer.flush();
  }

  // row -> record failed (unload workflow)
  private Mono<ErrorRecord> appendUnmappableReadResultToDebugFile(ErrorRecord record) {
    return Mono.just(record)
        .handle(
            (r, sink) -> {
              try {
                doAppendUnmappableReadResultToDebugFile(r);
                sink.next(r);
              } catch (Exception e) {
                sink.error(e);
              }
            });
  }

  private void doAppendUnmappableReadResultToDebugFile(ErrorRecord record) {
    Path logFile = operationDirectory.resolve(MAPPING_ERRORS_FILE);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    appendResourceAndPosition(writer, record);
    if (record.getSource() instanceof ReadResult) {
      appendReadResult((ReadResult) record.getSource(), MAPPING_ERRORS_FILE, writer);
    }
    stackTracePrinter.printStackTrace(record.getError(), writer);
    writer.println();
    writer.flush();
  }

  // Connector errors
  // record cannot be read or written (load and unload workflows)
  private Mono<ErrorRecord> appendFailedRecordToDebugFile(ErrorRecord record) {
    return Mono.just(record)
        .handle(
            (r, sink) -> {
              try {
                doAppendFailedRecordToDebugFile(record);
                sink.next(r);
              } catch (Exception e) {
                sink.error(e);
              }
            });
  }

  private void doAppendFailedRecordToDebugFile(ErrorRecord record) {
    Path logFile = operationDirectory.resolve(CONNECTOR_ERRORS_FILE);
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    appendResourceAndPosition(writer, record);
    if (record.getSource() instanceof ReadResult) {
      appendReadResult((ReadResult) record.getSource(), CONNECTOR_ERRORS_FILE, writer);
    } else if (record.getSource() != null) {
      writer.println("Source: " + LogManagerUtils.formatSource(record));
    }
    stackTracePrinter.printStackTrace(record.getError(), writer);
    writer.println();
    writer.flush();
  }

  private void appendReadResult(ReadResult source, String logFileName, PrintWriter writer) {
    doAppendStatement(source, logFileName, false);
    source
        .getRow()
        .ifPresent(
            row -> {
              writer.print("Row: ");
              String format = rowFormatter.format(row, protocolVersion, codecRegistry);
              LogManagerUtils.printAndMaybeAddNewLine(format, writer);
            });
  }

  private void appendResourceAndPosition(PrintWriter writer, Record record) {
    writer.println("Resource: " + record.getResource());
    writer.println("Position: " + record.getPosition());
  }

  @NonNull
  private Mono<Record> sendToFailedPositionsSink(Record record) {
    try {
      failedPositionsSink.next(record);
      return Mono.just(record);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }

  // Utility methods

  @VisibleForTesting
  void writeSummaryFile() throws IOException {
    PositionTracker tracker = mergePositionTrackers();
    try (PrintWriter writer =
        new PrintWriter(
            Files.newBufferedWriter(
                operationDirectory.resolve(SUMMARY_CSV), UTF_8, CREATE_NEW, WRITE))) {
      // sort resources by URI
      TreeMap<URI, ResourceStats> stats = new TreeMap<>(resources);
      for (Entry<URI, ResourceStats> entry : stats.entrySet()) {
        List<Range> ranges =
            tracker
                .getPositions()
                .getOrDefault(entry.getKey(), Collections.singletonList(new Range(0)));
        appendToSummaryFile(entry.getKey(), entry.getValue(), ranges, writer);
      }
      writer.flush();
    }
  }

  private void appendToSummaryFile(
      URI resource, ResourceStats stats, List<Range> positions, PrintWriter writer) {
    writer.print(resource);
    writer.print(';');
    for (Range range : positions) {
      writer.print(range);
    }
    writer.print(';');
    if (positions.size() != 1 || !stats.completed || stats.failed) {
      writer.print('0');
    } else {
      Range range = positions.get(0);
      if (stats.produced == 0 && range.getLower() == 0 && range.getUpper() == 0) {
        // empty file or token range
        writer.print('1');
      } else if (stats.produced == range.getUpper() - range.getLower() + 1) {
        // range.getLower is not necessarily 1 when records were skipped
        writer.print('1');
      } else {
        writer.print('0');
      }
    }
    writer.println();
  }

  @NonNull
  @VisibleForTesting
  PositionTracker mergePositionTrackers() {
    PositionTracker merged = failedPositionTracker;
    for (PositionTracker child : positionTrackers) {
      merged.merge(child);
    }
    return merged;
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

  static class ResourceStats {
    long produced;
    boolean completed;
    boolean failed;
  }
}
