/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.metrics.MetricsManager;
import com.datastax.dsbulk.engine.internal.settings.LogSettings.Verbosity;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.ConfigException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class MonitoringSettings {
  private static final String RATE_UNIT = "rateUnit";
  private static final String DURATION_UNIT = "durationUnit";
  private static final String REPORT_RATE = "reportRate";
  private static final String EXPECTED_WRITES = "expectedWrites";
  private static final String EXPECTED_READS = "expectedReads";
  private static final String JMX = "jmx";
  private static final String CSV = "csv";

  private final LoaderConfig config;
  private final String executionId;

  private TimeUnit rateUnit;
  private TimeUnit durationUnit;
  private Duration reportRate;
  private long expectedWrites;
  private long expectedReads;
  private boolean jmx;
  private boolean csv;

  MonitoringSettings(LoaderConfig config, String executionId) {
    this.config = config;
    this.executionId = executionId;
  }

  public void init() {
    try {
      rateUnit = config.getEnum(TimeUnit.class, RATE_UNIT);
      durationUnit = config.getEnum(TimeUnit.class, DURATION_UNIT);
      reportRate = config.getDuration(REPORT_RATE);
      expectedWrites = config.getLong(EXPECTED_WRITES);
      expectedReads = config.getLong(EXPECTED_READS);
      jmx = config.getBoolean(JMX);
      csv = config.getBoolean(CSV);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "monitoring");
    }
  }

  public MetricsManager newMetricsManager(
      WorkflowType workflowType,
      boolean batchingEnabled,
      Path executionDirectory,
      Verbosity verbosity,
      MetricRegistry registry,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry,
      RowType rowType) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("reporter-%d")
            .setPriority(Thread.MIN_PRIORITY)
            .build();
    ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, threadFactory);
    return new MetricsManager(
        registry,
        workflowType,
        executionId,
        scheduler,
        rateUnit,
        durationUnit,
        expectedWrites,
        expectedReads,
        jmx,
        csv,
        executionDirectory,
        verbosity,
        reportRate,
        batchingEnabled,
        protocolVersion,
        codecRegistry,
        rowType);
  }
}
