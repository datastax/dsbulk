/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.tests.utils.TestConfigUtils.createTestConfig;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codahale.metrics.MetricRegistry;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.engine.internal.metrics.MetricsManager;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.file.Path;
import java.time.Duration;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;

class MonitoringSettingsTest {

  private ProtocolVersion protocolVersion = ProtocolVersion.DEFAULT;
  private CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

  @Test
  void should_create_metrics_manager_with_default_settings() {
    LoaderConfig config = createTestConfig("dsbulk.monitoring");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    settings.init();
    MetricsManager metricsManager =
        settings.newMetricsManager(
            WorkflowType.UNLOAD,
            true,
            null,
            LogSettings.Verbosity.normal,
            new MetricRegistry(),
            protocolVersion,
            codecRegistry);
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
        createTestConfig(
            "dsbulk.monitoring",
            "rateUnit",
            "MINUTES",
            "durationUnit",
            "SECONDS",
            "reportRate",
            "30 minutes",
            "expectedWrites",
            1000,
            "expectedReads",
            50,
            "trackBytes",
            true,
            "jmx",
            false,
            "csv",
            true);
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    settings.init();
    MetricsManager metricsManager =
        settings.newMetricsManager(
            WorkflowType.UNLOAD,
            true,
            tmpPath,
            LogSettings.Verbosity.normal,
            new MetricRegistry(),
            protocolVersion,
            codecRegistry);
    assertThat(metricsManager).isNotNull();
    assertThat(ReflectionUtils.getInternalState(metricsManager, "rateUnit")).isEqualTo(MINUTES);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "durationUnit")).isEqualTo(SECONDS);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "reportInterval"))
        .isEqualTo(Duration.ofMinutes(30));
    assertThat(ReflectionUtils.getInternalState(metricsManager, "expectedWrites")).isEqualTo(1000L);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "expectedReads")).isEqualTo(50L);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "jmx")).isEqualTo(false);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "csv")).isEqualTo(true);
    assertThat(ReflectionUtils.getInternalState(metricsManager, "operationDirectory"))
        .isEqualTo(tmpPath);
  }

  @Test
  void should_throw_exception_when_expectedWrites_not_a_number() {
    LoaderConfig config = createTestConfig("dsbulk.monitoring", "expectedWrites", "NotANumber");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.expectedWrites, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_expectedReads_not_a_number() {
    LoaderConfig config = createTestConfig("dsbulk.monitoring", "expectedReads", "NotANumber");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.expectedReads, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_trackBytes_not_a_boolean() {
    LoaderConfig config = createTestConfig("dsbulk.monitoring", "trackBytes", "NotABoolean");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.trackBytes, expecting BOOLEAN, got STRING");
  }

  @Test
  void should_throw_exception_when_jmx_not_a_boolean() {
    LoaderConfig config = createTestConfig("dsbulk.monitoring", "jmx", "NotABoolean");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.jmx, expecting BOOLEAN, got STRING");
  }

  @Test
  void should_throw_exception_when_rateUnit_not_a_boolean() {
    LoaderConfig config = createTestConfig("dsbulk.monitoring", "rateUnit", "NotAUnit");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.rateUnit, expecting one of NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS, got: 'NotAUnit'");
  }

  @Test
  void should_throw_exception_when_durationUnit_not_a_boolean() {
    LoaderConfig config = createTestConfig("dsbulk.monitoring", "durationUnit", "NotAUnit");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.durationUnit, expecting one of NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS, got: 'NotAUnit'");
  }

  @Test
  void should_throw_exception_when_reportRate_not_a_duration() {
    LoaderConfig config = createTestConfig("dsbulk.monitoring", "reportRate", "NotADuration");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.reportRate: No number in duration value 'NotADuration'");
  }
}
