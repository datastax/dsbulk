/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.listener;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.histogram.HdrHistogramReservoir;
import java.nio.ByteBuffer;

/** A {@link ExecutionListener} that records useful metrics about the ongoing bulk operations. */
public class MetricsCollectingExecutionListener implements ExecutionListener {

  private final MetricRegistry registry;

  private final Timer totalStatementsTimer;
  private final Counter successfulStatementsCounter;
  private final Counter failedStatementsCounter;

  private final Timer totalReadsTimer;
  private final Counter successfulReadsCounter;
  private final Counter failedReadsCounter;

  private final Timer totalWritesTimer;
  private final Counter successfulWritesCounter;
  private final Counter failedWritesCounter;

  private final Timer totalReadsWritesTimer;
  private final Counter successfulReadsWritesCounter;
  private final Counter failedReadsWritesCounter;

  private final Counter inFlightRequestsCounter;

  private final Meter bytesReceivedMeter;
  private final Meter bytesSentMeter;

  private final ProtocolVersion protocolVersion;
  private final CodecRegistry codecRegistry;

  /** Creates a new instance using a newly-allocated {@link MetricRegistry}. */
  public MetricsCollectingExecutionListener() {
    this(new MetricRegistry(), ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE);
  }

  /**
   * Creates a new instance using the given {@link MetricRegistry}.
   *
   * @param registry The {@link MetricRegistry} to use.
   * @param protocolVersion the {@link ProtocolVersion} to use.
   * @param codecRegistry the {@link CodecRegistry} to use.
   */
  public MetricsCollectingExecutionListener(
      MetricRegistry registry, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    this.registry = registry;
    this.protocolVersion = protocolVersion;
    this.codecRegistry = codecRegistry;

    totalStatementsTimer =
        registry.timer("executor/statements/total", () -> new Timer(new HdrHistogramReservoir()));
    successfulStatementsCounter = registry.counter("executor/statements/successful");
    failedStatementsCounter = registry.counter("executor/statements/failed");

    totalReadsTimer =
        registry.timer("executor/reads/total", () -> new Timer(new HdrHistogramReservoir()));
    successfulReadsCounter = registry.counter("executor/reads/successful");
    failedReadsCounter = registry.counter("executor/reads/failed");

    totalWritesTimer =
        registry.timer("executor/writes/total", () -> new Timer(new HdrHistogramReservoir()));
    successfulWritesCounter = registry.counter("executor/writes/successful");
    failedWritesCounter = registry.counter("executor/writes/failed");

    totalReadsWritesTimer =
        registry.timer("executor/reads-writes/total", () -> new Timer(new HdrHistogramReservoir()));
    successfulReadsWritesCounter = registry.counter("executor/reads-writes/successful");
    failedReadsWritesCounter = registry.counter("executor/reads-writes/failed");

    inFlightRequestsCounter = registry.counter("executor/in-flight");

    bytesSentMeter = registry.meter("executor/bytes/sent");
    bytesReceivedMeter = registry.meter("executor/bytes/received");
  }

  /**
   * Returns the {@link MetricRegistry} used to aggregate metrics for this listener.
   *
   * @return the {@link MetricRegistry} used to aggregate metrics for this listener.
   */
  public MetricRegistry getRegistry() {
    return registry;
  }

  /**
   * Returns a {@link Timer} for total statement executions (successful and failed).
   *
   * <p>A batch statement is counted as one single statement. If that's not what you want, you are
   * probably looking for {@link #getTotalWritesTimer()}.
   *
   * @return a {@link Timer} for total statement executions (successful and failed).
   */
  public Timer getTotalStatementsTimer() {
    return totalStatementsTimer;
  }

  /**
   * Returns a {@link Counter} for successful statement executions.
   *
   * <p>A batch statement is counted as one single statement. If that's not what you want, you are
   * probably looking for {@link #getSuccessfulWritesCounter()}.
   *
   * @return a {@link Counter} for successful statement executions.
   */
  public Counter getSuccessfulStatementsCounter() {
    return successfulStatementsCounter;
  }

  /**
   * Returns a {@link Counter} for failed statement executions.
   *
   * <p>A batch statement is counted as one single statement. If that's not what you want, you are
   * probably looking for {@link #getFailedWritesCounter()}.
   *
   * @return a {@link Counter} for failed statement executions.
   */
  public Counter getFailedStatementsCounter() {
    return failedStatementsCounter;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of reads, both successful and
   * failed.
   *
   * @return a {@link Timer} that evaluates the duration of execution of reads, both successful and
   *     failed.
   */
  public Timer getTotalReadsTimer() {
    return totalReadsTimer;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of successful reads.
   *
   * @return a {@link Counter} that evaluates the duration of execution of successful reads.
   */
  public Counter getSuccessfulReadsCounter() {
    return successfulReadsCounter;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of failed reads.
   *
   * @return a {@link Counter} that evaluates the duration of execution of failed reads.
   */
  public Counter getFailedReadsCounter() {
    return failedReadsCounter;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of writes, both successful and
   * failed.
   *
   * <p>A batch statement is counted as many times as the number of child statements it contains. If
   * that's not what you want, you are probably looking for {@link #getTotalStatementsTimer()}.
   *
   * @return a {@link Timer} that evaluates the duration of execution of writes, both successful and
   *     failed.
   */
  public Timer getTotalWritesTimer() {
    return totalWritesTimer;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of successful writes.
   *
   * <p>A batch statement is counted as many times as the number of child statements it contains. If
   * that's not what you want, you are probably looking for {@link
   * #getSuccessfulStatementsCounter()}.
   *
   * @return a {@link Counter} that evaluates the duration of execution of successful writes.
   */
  public Counter getSuccessfulWritesCounter() {
    return successfulWritesCounter;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of failed writes.
   *
   * <p>A batch statement is counted as many times as the number of child statements it contains. If
   * that's not what you want, you are probably looking for {@link #getFailedStatementsCounter()}.
   *
   * @return a {@link Counter} that evaluates the duration of execution of failed writes.
   */
  public Counter getFailedWritesCounter() {
    return failedWritesCounter;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of all operations, including
   * reads and writes, both successful and failed.
   *
   * @return a {@link Timer} that evaluates the duration of execution of all operations.
   */
  public Timer getTotalReadsWritesTimer() {
    return totalReadsWritesTimer;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of all successful
   * operations, including reads and writes.
   *
   * @return a {@link Counter} that evaluates the duration of execution of all successful
   *     operations.
   */
  public Counter getSuccessfulReadsWritesCounter() {
    return successfulReadsWritesCounter;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of all failed operations,
   * including reads and writes.
   *
   * @return a {@link Counter} that evaluates the duration of execution of all failed operations.
   */
  public Counter getFailedReadsWritesCounter() {
    return failedReadsWritesCounter;
  }

  /**
   * Returns a {@link Counter} that evaluates the number of current in-flight requests, i.e. the
   * number of uncompleted futures waiting for a response from the server.
   *
   * @return a {@link Counter} that evaluates the number of current in-flight requests.
   */
  public Counter getInFlightRequestsCounter() {
    return inFlightRequestsCounter;
  }

  /**
   * Returns a {@link Meter} that evaluates the total number of bytes sent so far.
   *
   * <p>Note that this counter's value is an estimate of the actual amount of data sent; it might be
   * inaccurate or even zero, if the data size cannot be calculated.
   *
   * @return a {@link Meter} that evaluates the total number of bytes sent so far.
   */
  public Meter getBytesSentMeter() {
    return bytesSentMeter;
  }

  /**
   * Returns a {@link Meter} that evaluates the total number of bytes received so far.
   *
   * <p>Note that this counter's value is an estimate of the actual amount of data received; it
   * might be inaccurate or even zero, if the data size cannot be calculated.
   *
   * @return a {@link Meter} that evaluates the total number of bytes received so far.
   */
  public Meter getBytesReceivedMeter() {
    return bytesReceivedMeter;
  }

  @Override
  public void onWriteRequestStarted(Statement statement, ExecutionContext context) {
    long size = size(statement);
    bytesSentMeter.mark(size);
    inFlightRequestsCounter.inc();
  }

  @Override
  public void onReadRequestStarted(Statement statement, ExecutionContext context) {
    inFlightRequestsCounter.inc();
  }

  @Override
  public void onWriteRequestSuccessful(Statement statement, ExecutionContext context) {
    int delta = delta(statement);
    stop(context, totalWritesTimer, delta);
    stop(context, totalReadsWritesTimer, delta);
    successfulWritesCounter.inc(delta);
    successfulReadsWritesCounter.inc(delta);
    inFlightRequestsCounter.dec();
  }

  @Override
  public void onWriteRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    int delta = delta(statement);
    stop(context, totalWritesTimer, delta);
    stop(context, totalReadsWritesTimer, delta);
    failedWritesCounter.inc(delta);
    failedReadsWritesCounter.inc(delta);
    inFlightRequestsCounter.dec();
  }

  @Override
  public void onReadRequestSuccessful(Statement statement, ExecutionContext context) {
    inFlightRequestsCounter.dec();
  }

  @Override
  public void onRowReceived(Row row, ExecutionContext context) {
    stop(context, totalReadsTimer, 1);
    stop(context, totalReadsWritesTimer, 1);
    successfulReadsCounter.inc(1);
    successfulReadsWritesCounter.inc(1);
    long size = size(row);
    bytesReceivedMeter.mark(size);
  }

  @Override
  public void onReadRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    stop(context, totalReadsTimer, 1);
    stop(context, totalReadsWritesTimer, 1);
    failedReadsCounter.inc();
    failedReadsWritesCounter.inc();
    inFlightRequestsCounter.dec();
  }

  @Override
  public void onExecutionSuccessful(Statement statement, ExecutionContext context) {
    stop(context, totalStatementsTimer, 1);
    successfulStatementsCounter.inc();
  }

  @Override
  public void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {
    stop(context, totalStatementsTimer, 1);
    failedStatementsCounter.inc();
  }

  private static void stop(ExecutionContext context, Timer timer, int delta) {
    long elapsed = context.elapsedTimeNanos();
    for (int i = 0; i < delta; i++) {
      timer.update(elapsed, NANOSECONDS);
    }
  }

  private static int delta(Statement statement) {
    if (statement instanceof BatchStatement) {
      return ((BatchStatement) statement).size();
    } else {
      return 1;
    }
  }

  private long size(Statement statement) {
    long size = 0L;
    if (statement instanceof BoundStatement) {
      BoundStatement bs = (BoundStatement) statement;
      ColumnDefinitions metadata = bs.preparedStatement().getVariables();
      size += size(bs, metadata);
    } else if (statement instanceof BatchStatement) {
      BatchStatement batch = (BatchStatement) statement;
      for (Statement child : batch.getStatements()) {
        size += size(child);
      }
    } else if (statement instanceof SimpleStatement) {
      SimpleStatement stmt = (SimpleStatement) statement;
      try {
        ByteBuffer[] bbs = stmt.getValues(protocolVersion, codecRegistry);
        if (bbs != null) {
          for (ByteBuffer bb : bbs) {
            if (bb != null) {
              size += bb.remaining();
            }
          }
        }
      } catch (InvalidTypeException ignored) {
      }
    }
    return size;
  }

  private long size(Row row) {
    return size(row, row.getColumnDefinitions());
  }

  private long size(GettableData data, ColumnDefinitions metadata) {
    long size = 0L;
    if (metadata.size() > 0) {
      for (int i = 0; i < metadata.size(); i++) {
        ByteBuffer bb = data.getBytesUnsafe(i);
        if (bb != null) {
          size += bb.remaining();
        }
      }
    }
    return size;
  }
}
