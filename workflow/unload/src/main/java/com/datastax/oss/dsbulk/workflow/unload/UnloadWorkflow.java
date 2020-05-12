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

import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.commons.utils.StringUtils;
import com.datastax.oss.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.executor.api.reader.BulkReader;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.workflow.api.Workflow;
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
import com.datastax.oss.dsbulk.workflow.commons.utils.WorkflowUtils;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/** The main class for unload workflows. */
public class UnloadWorkflow implements Workflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnloadWorkflow.class);

  private final SettingsManager settingsManager;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private String executionId;
  private Connector connector;
  private Scheduler scheduler;
  private ReadResultMapper readResultMapper;
  private MetricsManager metricsManager;
  private LogManager logManager;
  private CqlSession session;
  private BulkReader executor;
  private List<? extends Statement<?>> readStatements;
  private Function<? super Publisher<Record>, ? extends Publisher<Record>> writer;
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
    connectorSettings.init();
    connector = connectorSettings.getConnector();
    connector.init();
    driverSettings.init(false);
    logSettings.logEffectiveSettings(
        settingsManager.getBulkLoaderConfig(), driverSettings.getDriverConfig());
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
    readResultMapper = schemaSettings.createReadResultMapper(session, recordMetadata, codecFactory);
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
    scheduler = Schedulers.newParallel(numCores, new DefaultThreadFactory("workflow"));
    readConcurrency =
        executorSettings
            .getMaxConcurrentQueries()
            .map(maxConcurrentQueries -> Math.min(maxConcurrentQueries, numCores))
            .orElse(numCores);
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    metricsManager.start();
    Stopwatch timer = Stopwatch.createStarted();
    (readStatements.size() >= WorkflowUtils.TPC_THRESHOLD ? threadPerCoreFlux() : parallelFlux())
        .transformDeferred(writer)
        .transform(failedRecordsMonitor)
        .transform(failedRecordsHandler)
        .then()
        .flux()
        .transform(terminationHandler)
        .blockLast();
    timer.stop();
    metricsManager.stop();
    long seconds = timer.elapsed(SECONDS);
    if (logManager.getTotalErrors() == 0) {
      LOGGER.info("{} completed successfully in {}.", this, StringUtils.formatElapsed(seconds));
    } else {
      LOGGER.warn(
          "{} completed with {} errors in {}.",
          this,
          logManager.getTotalErrors(),
          StringUtils.formatElapsed(seconds));
    }
    return logManager.getTotalErrors() == 0;
  }

  @NonNull
  private Flux<Record> threadPerCoreFlux() {
    return Flux.fromIterable(readStatements)
        .flatMap(
            statement ->
                Flux.from(executor.readReactive(statement))
                    .transform(queryWarningsHandler)
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedReadResultsMonitor)
                    .transform(failedReadsHandler)
                    .map(readResultMapper::map)
                    .transform(failedRecordsMonitor)
                    .transform(unmappableRecordsHandler)
                    .subscribeOn(scheduler),
            readConcurrency);
  }

  @NonNull
  private Flux<Record> parallelFlux() {
    return Flux.fromIterable(readStatements)
        .flatMap(executor::readReactive, readConcurrency)
        .window(Queues.SMALL_BUFFER_SIZE)
        .flatMap(
            results ->
                results
                    .transform(queryWarningsHandler)
                    .transform(totalItemsMonitor)
                    .transform(totalItemsCounter)
                    .transform(failedReadResultsMonitor)
                    .transform(failedReadsHandler)
                    .map(readResultMapper::map)
                    .transform(failedRecordsMonitor)
                    .transform(unmappableRecordsHandler)
                    .subscribeOn(scheduler),
            numCores);
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
