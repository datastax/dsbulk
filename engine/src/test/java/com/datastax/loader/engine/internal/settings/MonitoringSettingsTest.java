/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.loader.commons.config.DefaultLoaderConfig;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.engine.WorkflowType;
import com.datastax.loader.engine.internal.metrics.MetricsManager;
import com.typesafe.config.ConfigFactory;
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
    MetricsManager metricsManager = settings.newMetricsManager(WorkflowType.UNLOAD);
    assertThat(metricsManager).isNotNull();
    assertThat(Whitebox.getInternalState(metricsManager, "rateUnit")).isEqualTo(SECONDS);
    assertThat(Whitebox.getInternalState(metricsManager, "durationUnit")).isEqualTo(MILLISECONDS);
    assertThat(Whitebox.getInternalState(metricsManager, "reportInterval"))
        .isEqualTo(Duration.ofSeconds(5));
    assertThat(Whitebox.getInternalState(metricsManager, "expectedWrites")).isEqualTo(-1L);
    assertThat(Whitebox.getInternalState(metricsManager, "expectedReads")).isEqualTo(-1L);
    assertThat(Whitebox.getInternalState(metricsManager, "jmx")).isEqualTo(true);
  }

  @Test
  public void should_create_metrics_manager_with_user_supplied_settings() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "rateUnit = MINUTES, "
                    + "durationUnit = SECONDS, "
                    + "reportRate = 30 minutes, "
                    + "expectedWrites = 1000, "
                    + "expectedReads = 50,"
                    + "jmx = false"));
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    MetricsManager metricsManager = settings.newMetricsManager(WorkflowType.UNLOAD);
    assertThat(metricsManager).isNotNull();
    assertThat(Whitebox.getInternalState(metricsManager, "rateUnit")).isEqualTo(MINUTES);
    assertThat(Whitebox.getInternalState(metricsManager, "durationUnit")).isEqualTo(SECONDS);
    assertThat(Whitebox.getInternalState(metricsManager, "reportInterval"))
        .isEqualTo(Duration.ofMinutes(30));
    assertThat(Whitebox.getInternalState(metricsManager, "expectedWrites")).isEqualTo(1000L);
    assertThat(Whitebox.getInternalState(metricsManager, "expectedReads")).isEqualTo(50L);
    assertThat(Whitebox.getInternalState(metricsManager, "jmx")).isEqualTo(false);
  }
}
