/*
 * Copyright DataStax Inc.
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
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.metrics.MetricsManager;
import com.typesafe.config.ConfigFactory;
import java.nio.file.Path;
import java.time.Duration;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;

/** */
class MonitoringSettingsTest {

  @Test
  void should_create_metrics_manager_with_default_settings() {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.monitoring"));
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    settings.init();
    MetricsManager metricsManager = settings.newMetricsManager(WorkflowType.UNLOAD, true, null);
    assertThat(metricsManager).isNotNull();
    assertThat(ReflectionUtils.getInternalState(metricsManager, "rateUnit")).isEqualTo(SECONDS);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "durationUnit"))
        .isEqualTo(MILLISECONDS);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "reportInterval"))
        .isEqualTo(Duration.ofSeconds(5));
    assertThat(ReflectionUtils.getInternalState(metricsManager, "expectedWrites")).isEqualTo(-1L);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "expectedReads")).isEqualTo(-1L);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "jmx")).isEqualTo(true);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "csv")).isEqualTo(false);
  }

  @Test
  void should_create_metrics_manager_with_user_supplied_settings() {
    Path tmpPath = Files.temporaryFolder().toPath();
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
    settings.init();
    MetricsManager metricsManager = settings.newMetricsManager(WorkflowType.UNLOAD, true, tmpPath);
    assertThat(metricsManager).isNotNull();
    assertThat(ReflectionUtils.getInternalState(metricsManager, "rateUnit")).isEqualTo(MINUTES);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "durationUnit")).isEqualTo(SECONDS);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "reportInterval"))
        .isEqualTo(Duration.ofMinutes(30));
    assertThat(ReflectionUtils.getInternalState(metricsManager, "expectedWrites")).isEqualTo(1000L);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "expectedReads")).isEqualTo(50L);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "jmx")).isEqualTo(false);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "csv")).isEqualTo(true);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "executionDirectory"))
        .isEqualTo(tmpPath);
  }
}
