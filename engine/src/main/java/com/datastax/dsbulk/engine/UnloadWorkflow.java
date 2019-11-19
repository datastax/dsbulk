/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import static com.datastax.dsbulk.connectors.api.CommonConnectorFeature.INDEXED_RECORDS;
import static com.datastax.dsbulk.connectors.api.CommonConnectorFeature.MAPPED_RECORDS;
import static com.datastax.dsbulk.engine.internal.utils.ClusterInformationUtils.printDebugInfoAboutCluster;
import static com.datastax.dsbulk.engine.internal.utils.WorkflowUtils.TPC_THRESHOLD;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.MetricRegistry;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.log.LogManager;
import com.datastax.dsbulk.engine.internal.metrics.MetricsManager;
import com.datastax.dsbulk.engine.internal.schema.ReadResultMapper;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.dsbulk.engine.internal.settings.ConnectorSettings;
import com.datastax.dsbulk.engine.internal.settings.DriverSettings;
import com.datastax.dsbulk.engine.internal.settings.EngineSettings;
import com.datastax.dsbulk.engine.internal.settings.ExecutorSettings;
import com.datastax.dsbulk.engine.internal.settings.LogSettings;
import com.datastax.dsbulk.engine.internal.settings.MonitoringSettings;
import com.datastax.dsbulk.engine.internal.settings.SchemaSettings;
import com.datastax.dsbulk.engine.internal.settings.SettingsManager;
import com.datastax.dsbulk.engine.internal.utils.WorkflowUtils;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.reactor.reader.ReactorBulkReader;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.driver.shaded.guava.common.collect.BiMap;
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
  private DseSession session;
  private ReactorBulkReader executor;
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

  UnloadWorkflow(LoaderConfig config, BiMap<String, String> shortcuts) {
    settingsManager = new SettingsManager(config, shortcuts, WorkflowType.UNLOAD);
  }

  @Override
  public void init() throws Exception {
    settingsManager.init();
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
      throw new BulkConfigurationException("Dry-run is not supported for unload");
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
    printDebugInfoAboutCluster(session);
    schemaSettings.init(
        WorkflowType.UNLOAD,
        session,
        connector.supports(INDEXED_RECORDS),
        connector.supports(MAPPED_RECORDS));
    logManager = logSettings.newLogManager(WorkflowType.UNLOAD, session);
    logManager.init();
    metricsManager =
        monitoringSettings.newMetricsManager(
            WorkflowType.UNLOAD,
            false,
            logManager.getOperationDirectory(),
            logSettings.getVerbosity(),
            session.getMetrics().map(Metrics::getRegistry).orElse(new MetricRegistry()),
            session.getContext().getProtocolVersion(),
            session.getContext().getCodecRegistry());
    metricsManager.init();
    RecordMetadata recordMetadata = connector.getRecordMetadata();
    ExtendedCodecRegistry codecRegistry =
        codecSettings.createCodecRegistry(
            schemaSettings.isAllowExtraFields(), schemaSettings.isAllowMissingFields());
    readResultMapper =
        schemaSettings.createReadResultMapper(session, recordMetadata, codecRegistry);
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
    // Set the concurrency to the number of cores to maximize parallelism, unless a maximum number
    // of concurrent queries has been set, in which case, if that number is lesser than the number
    // of cores, cap the concurrency at that number to reduce lock contention around the semaphore
    // that controls it.
    readConcurrency =
        executorSettings.getMaxConcurrentQueries().isPresent()
            ? Math.min(numCores, executorSettings.getMaxConcurrentQueries().get())
            : numCores;
  }

  @Override
  public boolean execute() {
    LOGGER.debug("{} started.", this);
    metricsManager.start();
    Stopwatch timer = Stopwatch.createStarted();
    Flux<Record> flux;
    if (readStatements.size() >= TPC_THRESHOLD) {
      flux = threadPerCoreFlux();
    } else {
      flux = parallelFlux();
    }
    flux.transformDeferred(writer)
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
                executor
                    .readReactive(statement)
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
      Exception e = WorkflowUtils.closeQuietly(metricsManager, null);
      e = WorkflowUtils.closeQuietly(logManager, e);
      e = WorkflowUtils.closeQuietly(connector, e);
      e = WorkflowUtils.closeQuietly(scheduler, e);
      e = WorkflowUtils.closeQuietly(executor, e);
      e = WorkflowUtils.closeQuietly(session, e);
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
