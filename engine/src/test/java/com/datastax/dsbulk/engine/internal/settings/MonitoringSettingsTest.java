/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.metrics.MetricsManager;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

/** */
public class MonitoringSettingsTest {

  @Test
  public void should_create_metrics_manager_with_default_settings() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.monitoring"));
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    MetricsManager metricsManager = settings.newMetricsManager(WorkflowType.UNLOAD, true, null);
    assertThat(metricsManager).isNotNull();
    assertThat(Whitebox.getInternalState(metricsManager, "rateUnit")).isEqualTo(SECONDS);
    assertThat(Whitebox.getInternalState(metricsManager, "durationUnit")).isEqualTo(MILLISECONDS);
    assertThat(Whitebox.getInternalState(metricsManager, "reportInterval"))
        .isEqualTo(Duration.ofSeconds(5));
    assertThat(Whitebox.getInternalState(metricsManager, "expectedWrites")).isEqualTo(-1L);
    assertThat(Whitebox.getInternalState(metricsManager, "expectedReads")).isEqualTo(-1L);
    assertThat(Whitebox.getInternalState(metricsManager, "jmx")).isEqualTo(true);
    assertThat(Whitebox.getInternalState(metricsManager, "csv")).isEqualTo(false);
  }

  @Test
  public void should_create_metrics_manager_with_user_supplied_settings() throws Exception {
    Path tmpPath = new File(System.getProperty("java.io.tmpdir")).toPath();
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "rateUnit = MINUTES, "
                    + "durationUnit = SECONDS, "
                    + "reportRate = 30 minutes, "
                    + "expectedWrites = 1000, "
                    + "expectedReads = 50,"
                    + "jmx = false,"
                    + "csv = true"));
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    MetricsManager metricsManager = settings.newMetricsManager(WorkflowType.UNLOAD, true, tmpPath);
    assertThat(metricsManager).isNotNull();
    assertThat(Whitebox.getInternalState(metricsManager, "rateUnit")).isEqualTo(MINUTES);
    assertThat(Whitebox.getInternalState(metricsManager, "durationUnit")).isEqualTo(SECONDS);
    assertThat(Whitebox.getInternalState(metricsManager, "reportInterval"))
        .isEqualTo(Duration.ofMinutes(30));
    assertThat(Whitebox.getInternalState(metricsManager, "expectedWrites")).isEqualTo(1000L);
    assertThat(Whitebox.getInternalState(metricsManager, "expectedReads")).isEqualTo(50L);
    assertThat(Whitebox.getInternalState(metricsManager, "jmx")).isEqualTo(false);
    assertThat(Whitebox.getInternalState(metricsManager, "csv")).isEqualTo(true);
    assertThat(Whitebox.getInternalState(metricsManager, "executionDirectory")).isEqualTo(tmpPath);
  }
}
