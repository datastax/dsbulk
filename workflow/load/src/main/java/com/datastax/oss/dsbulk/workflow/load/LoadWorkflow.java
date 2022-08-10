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
package com.datastax.oss.dsbulk.workflow.load;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.executor.api.result.EmptyWriteResult;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.dsbulk.executor.api.writer.BulkWriter;
import com.datastax.oss.dsbulk.sampler.DataSizeSampler;
import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.utils.DurationUtils;
import com.datastax.oss.dsbulk.workflow.api.utils.ThrowableUtils;
import com.datastax.oss.dsbulk.workflow.commons.log.LogManager;
import com.datastax.oss.dsbulk.workflow.commons.metrics.MetricsManager;
import com.datastax.oss.dsbulk.workflow.commons.schema.NestedBatchException;
import com.datastax.oss.dsbulk.workflow.commons.schema.RecordMapper;
import com.datastax.oss.dsbulk.workflow.commons.settings.BatchSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.CodecSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.ConnectorSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.DriverSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.EngineSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.ExecutorSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.LogSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.MonitoringSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.SchemaGenerationStrategy;
import com.datastax.oss.dsbulk.workflow.commons.settings.SchemaSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.SettingsManager;
import com.datastax.oss.dsbulk.workflow.commons.utils.CloseableUtils;
import com.datastax.oss.dsbulk.workflow.commons.utils.ClusterInformationUtils;
import com.typesafe.config.Config;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/** The main class for load workflows. */
public class LoadWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadWorkflow.class);

  private static final int _1_KB = 1024;
  private static final int _10_KB = 10 * _1_KB;

  private final SettingsManager settingsManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private String executionId;
  private Connector connector;
  private MetricsManager metricsManager;
  private LogManager logManager;
  private EngineSettings engineSettings;
  private CqlSession session;
  private BulkWriter executor;
  private boolean batchingEnabled;
  private boolean dryRun;
  private int batchBufferSize;
  private Scheduler scheduler;
  private int numCores;
  private int readConcurrency;
  private int writeConcurrency;
  private boolean hasManyReaders;

  private Function<Record, Flux<BatchableStatement<?>>> mapper;
  private Function<Publisher<BatchableStatement<?>>, Publisher<Statement<?>>> batcher;
  private Function<Flux<Record>, Flux<Record>> totalItemsMonitor;
  private Function<Flux<Record>, Flux<Record>> totalItemsCounter;
  private Function<Flux<Record>, Flux<Record>> failedRecordsMonitor;
  private Function<Flux<BatchableStatement<?>>, Flux<BatchableStatement<?>>>
      unmappableStatementsMonitor;
  private Function<Flux<WriteResult>, Flux<WriteResult>>
      failedWritesMonitor;
  private Function<Flux<Record>, Flux<Record>> failedRecordsHandler;
  private Function<Flux<BatchableStatement<?>>, Flux<BatchableStatement<?>>>
      unmappableStatementsHandler;
  private Function<Flux<Statement<?>>, Flux<Statement<?>>> batcherMonitor;
  private Function<Flux<Void>, Flux<Void>> terminationHandler;
  private Function<Flux<WriteResult>, Flux<WriteResult>> failedWritesHandler;
  private Function<Flux<WriteResult>, Flux<Void>> resultPositionsHandler;
  private Function<Flux<WriteResult>, Flux<WriteResult>> queryWarningsHandler;
  private BiFunction<URI, Publisher<Record>, Publisher<Record>> resourceStatsHandler;

  LoadWorkflow(Config config) {
    settingsManager = new SettingsManager(config);
  }

  @Override
  public void init() throws Exception {
    settingsManager.init("LOAD", true, SchemaGenerationStrategy.MAP_AND_WRITE);
    executionId = settingsManager.getExecutionId();
    LogSettings logSettings = settingsManager.getLogSettings();
    logSettings.init();
    ConnectorSettings connectorSettings = settingsManager.getConnectorSettings();
    connectorSettings.init(logSettings.isSources());
    connector = connectorSettings.getConnector();
    connector.init();
    DriverSettings driverSettings = settingsManager.getDriverSettings();
    SchemaSettings schemaSettings = settingsManager.getSchemaSettings();
    BatchSettings batchSettings = settingsManager.getBatchSettings();
    ExecutorSettings executorSettings = settingsManager.getExecutorSettings();
    CodecSettings codecSettings = settingsManager.getCodecSettings();
    MonitoringSettings monitoringSettings = settingsManager.getMonitoringSettings();
    engineSettings = settingsManager.getEngineSettings();
    driverSettings.init(true);
    logSettings.logEffectiveSettings(
        settingsManager.getEffectiveBulkLoaderConfig(), driverSettings.getDriverConfig());
    monitoringSettings.init();
    codecSettings.init();
    executorSettings.init();
    engineSettings.init();
    ConvertingCodecFactory codecFactory =
        codecSettings.createCodecFactory(
            schemaSettings.isAllowExtraFields(), schemaSettings.isAllowMissingFields());
    session =
        driverSettings.newSession(
            executionId, codecFactory.getCodecRegistry(), monitoringSettings.getRegistry());
    ClusterInformationUtils.printDebugInfoAboutCluster(session);
    schemaSettings.init(
        session,
        codecFactory,
        connector.supports(CommonConnectorFeature.INDEXED_RECORDS),
        connector.supports(CommonConnectorFeature.MAPPED_RECORDS));
    logManager = logSettings.newLogManager(session);
    logManager.init();
    batchSettings.init();
    batchingEnabled = batchSettings.isBatchingEnabled();
    batchBufferSize = batchSettings.getBufferSize();
    RecordMapper recordMapper;
    try {
      recordMapper =
          schemaSettings.createRecordMapper(
              session, connector.getRecordMetadata(), batchingEnabled);
    } catch (NestedBatchException e) {
      LOGGER.warn(e.getMessage());
      batchingEnabled = false;
      recordMapper =
          schemaSettings.createRecordMapper(session, connector.getRecordMetadata(), false);
    }
    mapper = recordMapper::map;
    if (batchingEnabled) {
      batcher = batchSettings.newStatementBatcher(session)::batchByGroupingKey;
    }
    if (executorSettings.isTrackingBytes()) {
      monitoringSettings.forceTrackBytes();
    }
    metricsManager =
        monitoringSettings.newMetricsManager(
            true,
            batchingEnabled,
            logManager.getOperationDirectory(),
            logSettings.getVerbosity(),
            session.getContext().getProtocolVersion(),
            session.getContext().getCodecRegistry(),
            schemaSettings.getRowType());
    metricsManager.init();
    if (driverSettings.isCloud()) {
      executorSettings.enforceCloudRateLimit(session.getMetadata().getNodes().size());
    }
    executor = executorSettings.newWriteExecutor(session, metricsManager.getExecutionListener());
    dryRun = engineSettings.isDryRun();
    if (dryRun) {
      LOGGER.info("Dry-run mode enabled.");
    }
    closed.set(false);
    totalItemsMonitor = metricsManager.newTotalItemsMonitor();
    failedRecordsMonitor = metricsManager.newFailedRecordsMonitor();
    unmappableStatementsMonitor = metricsManager.newUnmappableStatementsMonitor();
    failedWritesMonitor = metricsManager.newFailedResultsMonitor();
    batcherMonitor = metricsManager.newBatcherMonitor();
    totalItemsCounter = logManager.newTotalItemsCounter();
    failedRecordsHandler = logManager.newFailedRecordsHandler();
    unmappableStatementsHandler = logManager.newUnmappableStatementsHandler();
    queryWarningsHandler = logManager.newQueryWarningsHandler();
    failedWritesHandler = logManager.newFailedWritesHandler();
    resultPositionsHandler = logManager.newWriteResultPositionsHandler();
    terminationHandler = logManager.newTerminationHandler();
    resourceStatsHandler = logManager.newConnectorResourceStatsHandler();
    numCores = Runtime.getRuntime().availableProcessors();
    if (connector.readConcurrency() < 1) {
      throw new IllegalArgumentException(
          "Invalid read concurrency: " + connector.readConcurrency());
    }
    readConcurrency = connector.readConcurrency();
    hasManyReaders = readConcurrency >= Math.max(4, numCores / 4);
    LOGGER.debug("Using read concurrency: {}", readConcurrency);
    writeConcurrency =
        engineSettings.getMaxConcurrentQueries().orElseGet(this::determineWriteConcurrency);
    LOGGER.debug(
        "Using write concurrency: {} (user-supplied: {})",
        writeConcurrency,
        engineSettings.getMaxConcurrentQueries().isPresent());
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    metricsManager.start();
    Stopwatch timer = Stopwatch.createStarted();
    Flux<Statement<?>> statements;
    if (hasManyReaders) {
      statements = manyReaders();
    } else {
      statements = fewReaders();
    }
    statements
        .transform(this::executeStatements)
        .transform(queryWarningsHandler)
        .transform(failedWritesMonitor)
        .transform(failedWritesHandler)
        .transform(resultPositionsHandler)
        .transform(terminationHandler)
        .blockLast();
    timer.stop();
    int totalErrors = logManager.getTotalErrors();
    metricsManager.stop(timer.elapsed(), totalErrors == 0);
    Duration elapsed = DurationUtils.round(timer.elapsed(), TimeUnit.SECONDS);
    String elapsedStr =
        elapsed.isZero() ? "less than one second" : DurationUtils.formatDuration(elapsed);
    if (totalErrors == 0) {
      LOGGER.info("{} completed successfully in {}.", this, elapsedStr);
    } else {
      LOGGER.warn("{} completed with {} errors in {}.", this, totalErrors, elapsedStr);
    }
    return totalErrors == 0;
  }

  /**
   * Reads the resources in parallel with {@code readConcurrency} parallelism.
   *
   * <p>Each thread in the workflow thread pool is responsible for reading one file and processing
   * its records.
   */
  private Flux<Statement<?>> manyReaders() {
    int numThreads = Math.min(readConcurrency, numCores);
    scheduler = Schedulers.newParallel(numThreads, new DefaultThreadFactory("workflow"));
    return Flux.defer(() -> connector.read(resourceStatsHandler))
        .flatMap(
            records ->
                Flux.from(records)
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedRecordsMonitor)
                    .transform(failedRecordsHandler)
                    .flatMap(mapper)
                    .transform(unmappableStatementsMonitor)
                    .transform(unmappableStatementsHandler)
                    .transform(this::bufferAndBatch)
                    .subscribeOn(scheduler),
            readConcurrency);
  }

  /**
   * Reads the resources one by one.
   *
   * <p>Even if resources are subscribed with {@code readConcurrency} parallelism, they are read,
   * one by one, by the main workflow thread. The chunks of parsed records are then dispatched to
   * the workflow thread pool. Each thread in the workflow thread pool is responsible for processing
   * a chunk of records, with {@code numCores} parallelism.
   */
  private Flux<Statement<?>> fewReaders() {
    scheduler = Schedulers.newParallel(numCores, new DefaultThreadFactory("workflow"));
    return Flux.defer(() -> connector.read(resourceStatsHandler))
        .flatMap(
            records ->
                Flux.from(records)
                    .window(batchingEnabled ? batchBufferSize : Queues.SMALL_BUFFER_SIZE),
            readConcurrency)
        .flatMap(
            records ->
                records
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedRecordsMonitor)
                    .transform(failedRecordsHandler)
                    .flatMap(mapper)
                    .transform(unmappableStatementsMonitor)
                    .transform(unmappableStatementsHandler)
                    .transform(this::batchBuffered)
                    .subscribeOn(scheduler),
            numCores);
  }

  /**
   * Batches the given statement flow, if batching is enabled; otherwise do nothing.
   *
   * <p>The flow is expected to be unbuffered, so this method first applies buffering by {@code
   * batchBufferSize} before batching the resulting chunks.
   */
  private Flux<? extends Statement<?>> bufferAndBatch(Flux<BatchableStatement<?>> stmts) {
    return batchingEnabled
        ? stmts.window(batchBufferSize).flatMap(batcher).transform(batcherMonitor)
        : stmts;
  }

  /**
   * Batches the given statement flow, if batching is enabled; otherwise do nothing.
   *
   * <p>The flow is expected to be already buffered by {@code batchBufferSize} so this method
   * applies batching immediately.
   */
  private Flux<? extends Statement<?>> batchBuffered(Flux<BatchableStatement<?>> stmts) {
    return batchingEnabled ? stmts.transform(batcher).transform(batcherMonitor) : stmts;
  }

  /**
   * Executes the given statement flow, unless we are running in dry-run mode, in which case a
   * successful write is emulated.
   */
  private Flux<WriteResult> executeStatements(Flux<? extends Statement<?>> stmts) {
    return dryRun
        ? stmts.map(EmptyWriteResult::new)
        : stmts.flatMap(executor::writeReactive, writeConcurrency);
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      LOGGER.debug("{} closing.", this);
      Exception e = CloseableUtils.closeQuietly(metricsManager, null);
      e = CloseableUtils.closeQuietly(logManager, e);
      e = CloseableUtils.closeQuietly(connector, e);
      e = CloseableUtils.closeQuietly(scheduler, e);
      e = CloseableUtils.closeQuietly(executor, e);
      e = CloseableUtils.closeQuietly(session, e);
      if (metricsManager != null) {
        metricsManager.reportFinalMetrics();
      }
      if (logManager != null) {
        logManager.reportAvailableFiles();
      }
      LOGGER.debug("{} closed.", this);
      if (e != null) {
        throw e;
      }
    }
  }

  @Override
  public String toString() {
    if (executionId == null) {
      return "Operation";
    } else {
      return "Operation " + executionId;
    }
  }

  private int determineWriteConcurrency() {
    if (dryRun) {
      return numCores;
    }
    double meanSize;
    if (engineSettings.isDataSizeSamplingEnabled()
        && connector.supports(CommonConnectorFeature.DATA_SIZE_SAMPLING)) {
      meanSize = getMeanRowSize();
    } else {
      // Can't sample data, so use a common value
      meanSize = _1_KB;
    }
    int writeConcurrency;
    if (meanSize <= 512) {
      if (hasManyReaders) {
        writeConcurrency = numCores * 32;
      } else {
        writeConcurrency = numCores * 16;
      }
    } else if (meanSize <= _1_KB) {
      if (hasManyReaders) {
        writeConcurrency = numCores * 16;
      } else {
        writeConcurrency = numCores * 8;
      }
    } else if (meanSize <= _10_KB) {
      writeConcurrency = numCores * 4;
    } else {
      writeConcurrency = numCores;
    }
    if (!batchingEnabled && meanSize <= _1_KB) {
      writeConcurrency *= 4;
    }
    return writeConcurrency;
  }

  private double getMeanRowSize() {
    double meanSize;
    try {
      LOGGER.debug("Sampling data...");
      Histogram sample =
          DataSizeSampler.sampleWrites(
              session.getContext(),
              Flux.merge(connector.read())
                  .take(1000)
                  .<Statement<?>>flatMap(mapper)
                  .filter(BoundStatement.class::isInstance)
                  .toIterable());
      if (sample.getCount() < 1000) {
        // sample too small, go with a common value
        LOGGER.debug("Data sample is too small: {}, discarding", sample.getCount());
        meanSize = _1_KB;
      } else {
        Snapshot snapshot = sample.getSnapshot();
        meanSize = snapshot.getMean();
        double standardDeviation = snapshot.getStdDev();
        double coefficientOfVariation = standardDeviation / meanSize;
        LOGGER.debug(
            "Average record size in bytes: {}, std dev: {}, coefficientOfVariation: {}",
            meanSize,
            standardDeviation,
            coefficientOfVariation);
        if (coefficientOfVariation >= 1) {
          LOGGER.debug("Data sample is too spread out, discarding");
          meanSize = _1_KB;
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Sampling failed: {}", ThrowableUtils.getSanitizedErrorMessage(e));
      meanSize = _1_KB;
    }
    return meanSize;
  }
}
