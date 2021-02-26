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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.datastax.oss.dsbulk.workflow.commons.metrics.MetricsManager;
import com.typesafe.config.Config;
import java.nio.file.Path;
import java.time.Duration;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
class MonitoringSettingsTest {

  private final ProtocolVersion protocolVersion = ProtocolVersion.DEFAULT;
  private final CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

  @Test
  void should_create_metrics_manager_with_default_settings() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.monitoring");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    settings.init();
    MetricsManager metricsManager =
        settings.newMetricsManager(
            false,
            true,
            null,
            LogSettings.Verbosity.normal,
            new MetricRegistry(),
            protocolVersion,
            codecRegistry,
            RowType.REGULAR);
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
    Path tmpPath = Files.newTemporaryFolder().toPath();
    Config config =
        TestConfigUtils.createTestConfig(
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
            false,
            true,
            tmpPath,
            LogSettings.Verbosity.normal,
            new MetricRegistry(),
            protocolVersion,
            codecRegistry,
            RowType.REGULAR);
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
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.monitoring", "expectedWrites", "NotANumber");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.expectedWrites, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_expectedReads_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.monitoring", "expectedReads", "NotANumber");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.expectedReads, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_trackBytes_not_a_boolean() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.monitoring", "trackBytes", "NotABoolean");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.trackBytes, expecting BOOLEAN, got STRING");
  }

  @Test
  void should_throw_exception_when_jmx_not_a_boolean() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.monitoring", "jmx", "NotABoolean");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.jmx, expecting BOOLEAN, got STRING");
  }

  @Test
  void should_throw_exception_when_rateUnit_not_a_boolean() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.monitoring", "rateUnit", "NotAUnit");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.rateUnit, expecting one of NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS, got: 'NotAUnit'");
  }

  @Test
  void should_throw_exception_when_durationUnit_not_a_boolean() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.monitoring", "durationUnit", "NotAUnit");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.durationUnit, expecting one of NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS, got: 'NotAUnit'");
  }

  @Test
  void should_throw_exception_when_reportRate_not_a_duration() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.monitoring", "reportRate", "NotADuration");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.reportRate: No number in duration value 'NotADuration'");
  }

  @Test
  void should_log_warning_when_reportRate_lesser_than_one_second(LogInterceptor logs) {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.monitoring", "reportRate", "10 milliseconds");
    MonitoringSettings settings = new MonitoringSettings(config, "test");
    settings.init();
    assertThat(logs)
        .hasMessageContaining(
            "Invalid value for dsbulk.monitoring.reportRate: "
                + "expecting duration >= 1 second, got '10 milliseconds' – will use 1 second instead");
  }
}
