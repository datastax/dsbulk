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
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

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
import com.datastax.dsbulk.engine.internal.statement.BulkStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.Result;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
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

/** */
public class LogManager implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);
  private static final int MIN_SAMPLE = 100;

  private final WorkflowType workflowType;
  private final Cluster cluster;
  private final Path executionDirectory;
  private final Scheduler scheduler;
  private final int maxErrors;
  private final float maxErrorRatio;
  private final StatementFormatter formatter;
  private final StatementFormatVerbosity verbosity;

  private final AtomicInteger errors = new AtomicInteger(0);
  private final LongAdder totalItems = new LongAdder();

  private final LoadingCache<Path, PrintWriter> openFiles =
      Caffeine.newBuilder()
          .removalListener(
              (Path path, PrintWriter writer, RemovalCause cause) -> {
                if (writer != null) {
                  writer.flush();
                  writer.close();
                }
              })
          .build(
              path ->
                  new PrintWriter(
                      Files.newBufferedWriter(
                          path, Charset.forName("UTF-8"), CREATE, WRITE, APPEND)));

  private final ConcurrentMap<URI, List<Range<Long>>> positions = new ConcurrentSkipListMap<>();

  private CodecRegistry codecRegistry;
  private ProtocolVersion protocolVersion;
  private PrintWriter positionsPrinter;

  public LogManager(
      WorkflowType workflowType,
      Cluster cluster,
      Path executionDirectory,
      int maxErrors,
      float maxErrorRatio,
      StatementFormatter formatter,
      StatementFormatVerbosity verbosity) {
    this(
        workflowType,
        cluster,
        Schedulers.newParallel(4, new DefaultThreadFactory("log-manager")),
        executionDirectory,
        maxErrors,
        maxErrorRatio,
        formatter,
        verbosity);
  }

  @VisibleForTesting
  LogManager(
      WorkflowType workflowType,
      Cluster cluster,
      Scheduler scheduler,
      Path executionDirectory,
      int maxErrors,
      float maxErrorRatio,
      StatementFormatter formatter,
      StatementFormatVerbosity verbosity) {
    this.workflowType = workflowType;
    this.cluster = cluster;
    this.executionDirectory = executionDirectory;
    this.scheduler = scheduler;
    this.maxErrors = maxErrors;
    this.maxErrorRatio = maxErrorRatio;
    this.formatter = formatter;
    this.verbosity = verbosity;
  }

  public void init() throws IOException {
    codecRegistry = cluster.getConfiguration().getCodecRegistry();
    protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    if (workflowType == WorkflowType.LOAD) {
      positionsPrinter =
          new PrintWriter(
              Files.newBufferedWriter(
                  executionDirectory.resolve("positions.txt"),
                  Charset.forName("UTF-8"),
                  CREATE_NEW,
                  WRITE));
    }
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
    // being be thrown and the workflow stops as expected.
    Hooks.onErrorDropped(t -> {});
  }

  public Path getExecutionDirectory() {
    return executionDirectory;
  }

  public int getTotalErrors() {
    return errors.get();
  }

  @Override
  public void close() {
    openFiles.invalidateAll();
    openFiles.cleanUp();
    if (workflowType == WorkflowType.LOAD && positionsPrinter != null) {
      positions.forEach(this::appendToPositionsFile);
      positionsPrinter.flush();
      positionsPrinter.close();
    }
    scheduler.dispose();
  }

  public void reportLastLocations() {
    if (workflowType == WorkflowType.LOAD && positionsPrinter != null) {
      LOGGER.info(
          "Last processed positions can be found in {}",
          executionDirectory.resolve("positions.txt"));
    }
  }

  /**
   * Handler for unmappable statements produced by the {@link
   * com.datastax.dsbulk.engine.internal.schema.RecordMapper record mapper}.
   *
   * <p>Used only in load workflows.
   *
   * <p>Increments the number of errors and forwards unmappable statements to the {@link
   * #newUnmappableStatementProcessor() unmappable statement processor} for further processing.
   *
   * @return a handler for unmappable statements.
   */
  @NotNull
  public Function<Flux<Statement>, Flux<Statement>> newFailedStatementsHandler() {
    FluxSink<UnmappableStatement> sink = newUnmappableStatementProcessor();
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    Statement stmt = signal.get();
                    if (stmt instanceof UnmappableStatement) {
                      sink.next((UnmappableStatement) stmt);
                      signal = maybeTriggerOnError(signal, errors.incrementAndGet());
                    }
                  }
                  return signal;
                })
            .<Statement>dematerialize()
            .doOnTerminate(sink::complete)
            .filter(r -> !(r instanceof UnmappableStatement));
  }

  /**
   * Handler for unmappable records produced either by the {@link
   * com.datastax.dsbulk.connectors.api.Connector connector} (load workflows) or by the {@link
   * com.datastax.dsbulk.engine.internal.schema.ReadResultMapper read result mapper} (unload
   * workflows).
   *
   * <p>Used by both load and unload workflows.
   *
   * <p>Increments the number of errors and forwards unmappable records to the {@link
   * #newUnmappableRecordProcessor() unmappable record processor} for further processing.
   *
   * @return a handler for unmappable records.
   */
  @NotNull
  public Function<Flux<Record>, Flux<Record>> newFailedRecordsHandler() {
    FluxSink<ErrorRecord> sink = newUnmappableRecordProcessor();
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    Record r = signal.get();
                    if (r instanceof ErrorRecord) {
                      sink.next((ErrorRecord) r);
                      signal = maybeTriggerOnError(signal, errors.incrementAndGet());
                    }
                  }
                  return signal;
                })
            .<Record>dematerialize()
            .doOnTerminate(sink::complete)
            .filter(r -> !(r instanceof ErrorRecord));
  }

  /**
   * Handler for unsuccessful {@link WriteResult}s.
   *
   * <p>Used only by the load workflow.
   *
   * <p>Increments the number of errors and forwards unsuccessful write results to the {@link
   * #newWriteResultProcessor() write result processor} for further processing.
   *
   * @return a handler for unsuccessful write results.
   */
  @NotNull
  public Function<Flux<WriteResult>, Flux<WriteResult>> newFailedWritesHandler() {
    FluxSink<WriteResult> sink = newWriteResultProcessor();
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    WriteResult r = signal.get();
                    if (!r.isSuccess()) {
                      sink.next(r);
                      assert r.getError().isPresent();
                      Throwable cause = r.getError().get().getCause();
                      if (isUnrecoverable(cause)) {
                        return Signal.error(cause);
                      } else {
                        signal =
                            maybeTriggerOnError(signal, errors.addAndGet(delta(r.getStatement())));
                      }
                    }
                  }
                  return signal;
                })
            .<WriteResult>dematerialize()
            .doOnTerminate(sink::complete)
            .filter(Result::isSuccess);
  }

  /**
   * Handler for unsuccessful {@link ReadResult}s.
   *
   * <p>Used only by the unload workflow.
   *
   * <p>Increments the number of errors and forwards unsuccessful read results to the {@link
   * #newReadResultProcessor() read result processor} for further processing.
   *
   * @return a handler for unsuccessful read results.
   */
  @NotNull
  public Function<Flux<ReadResult>, Flux<ReadResult>> newFailedReadsHandler() {
    FluxSink<ReadResult> sink = newReadResultProcessor();
    return upstream ->
        upstream
            .materialize()
            .map(
                signal -> {
                  if (signal.isOnNext()) {
                    ReadResult r = signal.get();
                    if (!r.isSuccess()) {
                      sink.next(r);
                      assert r.getError().isPresent();
                      Throwable cause = r.getError().get().getCause();
                      if (isUnrecoverable(cause)) {
                        return Signal.error(cause);
                      } else {
                        signal = maybeTriggerOnError(signal, errors.incrementAndGet());
                      }
                    }
                  }
                  return signal;
                })
            .<ReadResult>dematerialize()
            .doOnTerminate(sink::complete)
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
   * <p>Groups together records by {@link Record#getResource() resource} then merges all their
   * {@link Record#getPosition() positions} into continuous ranges.
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
   * A processor for unmappable records.
   *
   * <p>Used in both load and unload workflows.
   *
   * <p>Appends the record to the debug file, then (for load workflows only) to the bad file and
   * forwards the record's position to the {@link #newRecordPositionTracker() position tracker}.
   *
   * @return A processor for unmappable records.
   */
  @NotNull
  private FluxSink<ErrorRecord> newUnmappableRecordProcessor() {
    UnicastProcessor<ErrorRecord> processor = UnicastProcessor.create();
    Flux<ErrorRecord> flux = processor.doOnNext(this::appendToDebugFile);
    if (workflowType == WorkflowType.LOAD) {
      flux.doOnNext(this::appendToBadFile)
          .transform(newRecordPositionTracker())
          .subscribeOn(scheduler)
          .subscribe();
    } else {
      flux.subscribeOn(scheduler).subscribe();
    }
    return processor.sink();
  }

  public <T> Function<Flux<T>, Flux<T>> newTotalItemsCounter() {
    return upstream -> upstream.doOnNext(r -> totalItems.increment());
  }

  /**
   * A processor for unmappable statements.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the statement to the debug file, then extracts its record, appends it to the bad
   * file, then forwards the record's position to the {@link #newRecordPositionTracker() position
   * tracker}.
   *
   * @return A processor for unmappable statements.
   */
  @NotNull
  private FluxSink<UnmappableStatement> newUnmappableStatementProcessor() {
    UnicastProcessor<UnmappableStatement> processor = UnicastProcessor.create();
    processor
        .doOnNext(this::appendToDebugFile)
        .transform(newStatementToRecordMapper())
        .doOnNext(this::appendToBadFile)
        .transform(newRecordPositionTracker())
        .subscribeOn(scheduler)
        .subscribe();
    return processor.sink();
  }

  /**
   * A processor for failed write results.
   *
   * <p>Used only in the load workflow.
   *
   * <p>Appends the failed result to the debug file, then extracts its statement, then extracts its
   * record, then appends it to the bad file, then forwards the record's position to the {@link
   * #newRecordPositionTracker() position tracker}.
   *
   * @return A processor for failed write results.
   */
  @NotNull
  private FluxSink<WriteResult> newWriteResultProcessor() {
    UnicastProcessor<WriteResult> processor = UnicastProcessor.create();
    processor
        .doOnNext(this::appendToDebugFile)
        .map(Result::getStatement)
        .transform(newStatementToRecordMapper())
        .doOnNext(this::appendToBadFile)
        .transform(newRecordPositionTracker())
        .subscribeOn(scheduler)
        .subscribe();
    return processor.sink();
  }

  /**
   * A processor for failed read results.
   *
   * <p>Used only in the unload workflow.
   *
   * <p>Extracts the statement, then appends it to the debug file, then extracts its record, appends
   * it to the bad file, then forwards the record's position to the {@link
   * #newRecordPositionTracker() position tracker}.
   *
   * @return A processor for failed read results.
   */
  @NotNull
  private FluxSink<ReadResult> newReadResultProcessor() {
    UnicastProcessor<ReadResult> processor = UnicastProcessor.create();
    processor.doOnNext(this::appendToDebugFile).subscribeOn(scheduler).subscribe();
    return processor.sink();
  }

  private void appendToBadFile(Record record) {
    Path logFile = executionDirectory.resolve("operation.bad");
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    Object source = record.getSource();
    printAndMaybeAddNewLine(source == null ? null : source.toString(), writer);
    writer.flush();
  }

  private void appendToDebugFile(WriteResult result) {
    Path logFile = executionDirectory.resolve("load-errors.log");
    appendToDebugFile(result, logFile);
  }

  private void appendToDebugFile(ReadResult result) {
    Path logFile = executionDirectory.resolve("unload-errors.log");
    appendToDebugFile(result, logFile);
  }

  private void appendToDebugFile(Result result, Path logFile) {
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    writer.print("Statement: ");
    String format =
        formatter.format(result.getStatement(), verbosity, protocolVersion, codecRegistry);
    printAndMaybeAddNewLine(format, writer);
    result.getError().orElseThrow(IllegalStateException::new).printStackTrace(writer);
    writer.println();
    writer.flush();
  }

  private void appendToDebugFile(UnmappableStatement statement) {
    Path logFile = executionDirectory.resolve("mapping-errors.log");
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    appendStatementInfo(statement, writer);
    statement.getError().printStackTrace(writer);
    writer.println();
    writer.flush();
  }

  private void appendToDebugFile(ErrorRecord record) {
    Path logFile = executionDirectory.resolve("mapping-errors.log");
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    appendRecordInfo(record, writer);
    record.getError().printStackTrace(writer);
    writer.println();
    writer.flush();
  }

  private void appendToPositionsFile(URI resource, List<Range<Long>> positions) {
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
}
