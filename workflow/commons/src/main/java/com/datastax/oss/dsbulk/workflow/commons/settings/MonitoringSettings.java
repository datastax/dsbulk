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
package com.datastax.oss.dsbulk.workflow.commons.settings;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.workflow.commons.metrics.MetricsManager;
import com.datastax.oss.dsbulk.workflow.commons.settings.LogSettings.Verbosity;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringSettings.class);

  private static final String RATE_UNIT = "rateUnit";
  private static final String DURATION_UNIT = "durationUnit";
  private static final String REPORT_RATE = "reportRate";
  private static final String EXPECTED_WRITES = "expectedWrites";
  private static final String EXPECTED_READS = "expectedReads";
  private static final String TRACK_BYTES = "trackBytes";
  private static final String JMX = "jmx";
  private static final String CSV = "csv";

  private final Config config;
  private final String executionId;

  private TimeUnit rateUnit;
  private TimeUnit durationUnit;
  private Duration reportRate;
  private long expectedWrites;
  private long expectedReads;
  private boolean trackBytes;
  private boolean jmx;
  private boolean csv;

  public MonitoringSettings(Config config, String executionId) {
    this.config = config;
    this.executionId = executionId;
  }

  public void init() {
    try {
      rateUnit = config.getEnum(TimeUnit.class, RATE_UNIT);
      durationUnit = config.getEnum(TimeUnit.class, DURATION_UNIT);
      reportRate = config.getDuration(REPORT_RATE);
      if (reportRate.getSeconds() == 0) {
        LOGGER.warn(
            "Invalid value for dsbulk.monitoring.{}: expecting duration >= 1 second, got '{}' – will use 1 second instead",
            REPORT_RATE,
            config.getString(REPORT_RATE));
        reportRate = Duration.ofSeconds(1);
      }
      expectedWrites = config.getLong(EXPECTED_WRITES);
      expectedReads = config.getLong(EXPECTED_READS);
      trackBytes = config.getBoolean(TRACK_BYTES);
      jmx = config.getBoolean(JMX);
      csv = config.getBoolean(CSV);
    } catch (ConfigException e) {
      throw ConfigUtils.convertConfigException(e, "dsbulk.monitoring");
    }
  }

  public MetricsManager newMetricsManager(
      boolean monitorWrites,
      boolean batchingEnabled,
      Path operationDirectory,
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
        monitorWrites,
        executionId,
        scheduler,
        rateUnit,
        durationUnit,
        expectedWrites,
        expectedReads,
        trackBytes,
        jmx,
        csv,
        operationDirectory,
        verbosity,
        reportRate,
        batchingEnabled,
        protocolVersion,
        codecRegistry,
        rowType);
  }
}
