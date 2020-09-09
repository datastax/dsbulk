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
package com.datastax.oss.dsbulk.workflow.unload;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.executor.api.reader.BulkReader;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.utils.DurationUtils;
import com.datastax.oss.dsbulk.workflow.commons.log.LogManager;
import com.datastax.oss.dsbulk.workflow.commons.metrics.MetricsManager;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultMapper;
import com.datastax.oss.dsbulk.workflow.commons.settings.CodecSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.ConnectorSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.DriverSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.EngineSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.ExecutorSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.LogSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.MonitoringSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.SchemaGenerationType;
import com.datastax.oss.dsbulk.workflow.commons.settings.SchemaSettings;
import com.datastax.oss.dsbulk.workflow.commons.settings.SettingsManager;
import com.datastax.oss.dsbulk.workflow.commons.utils.CloseableUtils;
import com.datastax.oss.dsbulk.workflow.commons.utils.ClusterInformationUtils;
import com.typesafe.config.Config;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** The main class for unload workflows. */
public class UnloadWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnloadWorkflow.class);

  private final SettingsManager settingsManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private String executionId;
  private Connector connector;
  private Set<Scheduler> schedulers;
  private ReadResultMapper readResultMapper;
  private MetricsManager metricsManager;
  private LogManager logManager;
  private CqlSession session;
  private BulkReader executor;
  private List<Statement<?>> readStatements;
  private Function<Publisher<Record>, Publisher<Record>> writer;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsMonitor;
  private Function<Flux<Record>, Flux<Record>> failedRecordsMonitor;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedReadResultsMonitor;
  private Function<Flux<Record>, Flux<Record>> failedRecordsHandler;
  private Function<Flux<ReadResult>, Flux<ReadResult>> totalItemsCounter;
  private Function<Flux<ReadResult>, Flux<ReadResult>> failedReadsHandler;
  private Function<Flux<ReadResult>, Flux<ReadResult>> queryWarningsHandler;
  private Function<Flux<Record>, Flux<Record>> unmappableRecordsHandler;
  private Function<Flux<Void>, Flux<Void>> terminationHandler;
  private int readConcurrency;
  private int numCores;
  private int writeConcurrency;

  UnloadWorkflow(Config config) {
    settingsManager = new SettingsManager(config);
  }

  @Override
  public void init() throws Exception {
    settingsManager.init("UNLOAD", false);
    executionId = settingsManager.getExecutionId();
    LogSettings logSettings = settingsManager.getLogSettings();
    DriverSettings driverSettings = settingsManager.getDriverSettings();
    ConnectorSettings connectorSettings = settingsManager.getConnectorSettings();
    SchemaSettings schemaSettings = settingsManager.getSchemaSettings();
    ExecutorSettings executorSettings = settingsManager.getExecutorSettings();
    CodecSettings codecSettings = settingsManager.getCodecSettings();
    MonitoringSettings monitoringSettings = settingsManager.getMonitoringSettings();
    EngineSettings engineSettings = settingsManager.getEngineSettings();
    engineSettings.init();
    // First verify that dry-run is off; that's unsupported for unload.
    if (engineSettings.isDryRun()) {
      throw new IllegalArgumentException("Dry-run is not supported for unload");
    }
    // No logs should be produced until the following statement returns
    logSettings.init();
    connectorSettings.init(false);
    connector = connectorSettings.getConnector();
    connector.init();
    driverSettings.init(false);
    logSettings.logEffectiveSettings(
        settingsManager.getEffectiveBulkLoaderConfig(), driverSettings.getDriverConfig());
    codecSettings.init();
    monitoringSettings.init();
    executorSettings.init();
    session = driverSettings.newSession(executionId);
    ClusterInformationUtils.printDebugInfoAboutCluster(session);
    schemaSettings.init(
        SchemaGenerationType.READ_AND_MAP,
        session,
        connector.supports(CommonConnectorFeature.INDEXED_RECORDS),
        connector.supports(CommonConnectorFeature.MAPPED_RECORDS));
    logManager = logSettings.newLogManager(session, false);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            false,
            false,
            logManager.getOperationDirectory(),
            logSettings.getVerbosity(),
            session.getMetrics().map(Metrics::getRegistry).orElse(new MetricRegistry()),
            session.getContext().getProtocolVersion(),
            session.getContext().getCodecRegistry(),
            schemaSettings.getRowType());
    metricsManager.init();
    RecordMetadata recordMetadata = connector.getRecordMetadata();
    ConvertingCodecFactory codecFactory =
        codecSettings.createCodecFactory(
            schemaSettings.isAllowExtraFields(), schemaSettings.isAllowMissingFields());
    readResultMapper =
        schemaSettings.createReadResultMapper(
            session, recordMetadata, codecFactory, logSettings.isSources());
    readStatements = schemaSettings.createReadStatements(session);
    executor =
        executorSettings.newReadExecutor(
            session, metricsManager.getExecutionListener(), schemaSettings.isSearchQuery());
    closed.set(false);
    writer = connector.write();
    totalItemsMonitor = metricsManager.newTotalItemsMonitor();
    failedRecordsMonitor = metricsManager.newFailedItemsMonitor();
    failedReadResultsMonitor = metricsManager.newFailedItemsMonitor();
    failedRecordsHandler = logManager.newFailedRecordsHandler();
    totalItemsCounter = logManager.newTotalItemsCounter();
    failedReadsHandler = logManager.newFailedReadsHandler();
    queryWarningsHandler = logManager.newQueryWarningsHandler();
    unmappableRecordsHandler = logManager.newUnmappableRecordsHandler();
    terminationHandler = logManager.newTerminationHandler();
    numCores = Runtime.getRuntime().availableProcessors();
    if (connector.writeConcurrency() < 1) {
      throw new IllegalArgumentException("Invalid write concurrency: " + 1);
    }
    writeConcurrency = connector.writeConcurrency();
    LOGGER.debug("Using write concurrency: {}", writeConcurrency);
    readConcurrency =
        Math.min(
            readStatements.size(),
            // Most connectors have a default of numCores/2 for writeConcurrency;
            // a good readConcurrency is then numCores.
            engineSettings.getMaxConcurrentQueries().orElse(numCores));
    LOGGER.debug(
        "Using read concurrency: {} (user-supplied: {})",
        readConcurrency,
        engineSettings.getMaxConcurrentQueries().isPresent());
    schedulers = new HashSet<>();
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    metricsManager.start();
    Flux<Record> flux;
    if (writeConcurrency == 1) {
      flux = oneWriter();
    } else if (writeConcurrency < numCores / 2 || readConcurrency < numCores / 2) {
      flux = fewWriters();
    } else {
      flux = manyWriters();
    }
    Stopwatch timer = Stopwatch.createStarted();
    flux.then().flux().transform(terminationHandler).blockLast();
    timer.stop();
    metricsManager.stop();
    Duration elapsed = DurationUtils.round(timer.elapsed(), TimeUnit.SECONDS);
    String elapsedStr =
        elapsed.isZero() ? "less than one second" : DurationUtils.formatDuration(elapsed);
    int totalErrors = logManager.getTotalErrors();
    if (totalErrors == 0) {
      LOGGER.info("{} completed successfully in {}.", this, elapsedStr);
    } else {
      LOGGER.warn("{} completed with {} errors in {}.", this, totalErrors, elapsedStr);
    }
    return totalErrors == 0;
  }

  private Flux<Record> oneWriter() {
    int numThreads = Math.min(numCores * 2, readConcurrency);
    Scheduler scheduler =
        numThreads == 1
            ? Schedulers.immediate()
            : Schedulers.newParallel(numThreads, new DefaultThreadFactory("workflow"));
    schedulers.add(scheduler);
    return Flux.fromIterable(readStatements)
        .flatMap(
            results ->
                Flux.from(executor.readReactive(results))
                    .publishOn(scheduler, 500)
                    .transform(queryWarningsHandler)
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedReadResultsMonitor)
                    .transform(failedReadsHandler)
                    .map(readResultMapper::map)
                    .transform(failedRecordsMonitor)
                    .transform(unmappableRecordsHandler),
            readConcurrency,
            500)
        .transform(writer)
        .transform(failedRecordsMonitor)
        .transform(failedRecordsHandler);
  }

  private Flux<Record> fewWriters() {
    // writeConcurrency cannot be 1 here, but readConcurrency can
    int numThreadsForReads = Math.min(numCores, readConcurrency);
    Scheduler schedulerForReads =
        numThreadsForReads == 1
            ? Schedulers.immediate()
            : Schedulers.newParallel(numThreadsForReads, new DefaultThreadFactory("workflow-read"));
    int numThreadsForWrites = Math.min(numCores, writeConcurrency);
    Scheduler schedulerForWrites =
        Schedulers.newParallel(numThreadsForWrites, new DefaultThreadFactory("workflow-write"));
    schedulers.add(schedulerForReads);
    schedulers.add(schedulerForWrites);
    return Flux.fromIterable(readStatements)
        .flatMap(
            results ->
                Flux.from(executor.readReactive(results))
                    .publishOn(schedulerForReads, 500)
                    .transform(queryWarningsHandler)
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedReadResultsMonitor)
                    .transform(failedReadsHandler)
                    .map(readResultMapper::map)
                    .transform(failedRecordsMonitor)
                    .transform(unmappableRecordsHandler),
            readConcurrency,
            500)
        .parallel(writeConcurrency)
        .runOn(schedulerForWrites)
        .groups()
        .flatMap(
            records ->
                records
                    .transform(writer)
                    .transform(failedRecordsMonitor)
                    .transform(failedRecordsHandler),
            writeConcurrency,
            500);
  }

  private Flux<Record> manyWriters() {
    // writeConcurrency and readConcurrency are >= 0.5C here
    int actualConcurrency = Math.min(readConcurrency, writeConcurrency);
    int numThreads = Math.min(numCores * 2, actualConcurrency);
    Scheduler scheduler = Schedulers.newParallel(numThreads, new DefaultThreadFactory("workflow"));
    schedulers.add(scheduler);
    return Flux.fromIterable(readStatements)
        .flatMap(
            results -> {
              Flux<Record> records =
                  Flux.from(executor.readReactive(results))
                      .publishOn(scheduler, 500)
                      .transform(queryWarningsHandler)
                      .transform(totalItemsMonitor)
                      .transform(totalItemsCounter)
                      .transform(failedReadResultsMonitor)
                      .transform(failedReadsHandler)
                      .map(readResultMapper::map)
                      .transform(failedRecordsMonitor)
                      .transform(unmappableRecordsHandler);
              if (actualConcurrency == writeConcurrency) {
                records = records.transform(writer);
              } else {
                // If the actual concurrency is lesser than the connector's desired write
                // concurrency, we need to give the connector a chance to switch writers
                // frequently so that it can really redirect records to all the final destinations
                // (to that many files on disk for example). If the connector is correctly
                // implemented, each window will be redirected to a different destination
                // in a round-robin fashion.
                records = records.window(500).flatMap(window -> window.transform(writer), 1, 500);
              }
              return records.transform(failedRecordsMonitor).transform(failedRecordsHandler);
            },
            actualConcurrency,
            500);
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      LOGGER.debug("{} closing.", this);
      Exception e = CloseableUtils.closeQuietly(metricsManager, null);
      e = CloseableUtils.closeQuietly(logManager, e);
      e = CloseableUtils.closeQuietly(connector, e);
      if (schedulers != null) {
        for (Scheduler scheduler : schedulers) {
          e = CloseableUtils.closeQuietly(scheduler, e);
        }
      }
      e = CloseableUtils.closeQuietly(executor, e);
      e = CloseableUtils.closeQuietly(session, e);
      if (metricsManager != null) {
        metricsManager.reportFinalMetrics();
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
}
