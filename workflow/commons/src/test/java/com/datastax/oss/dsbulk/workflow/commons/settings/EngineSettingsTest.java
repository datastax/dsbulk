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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.typesafe.config.Config;
import org.junit.jupiter.api.Test;

class EngineSettingsTest {

  @Test
  void should_report_default_dry_run() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.engine");
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.isDryRun()).isFalse();
  }

  @Test
  void should_create_custom_dry_run() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.engine", "dryRun", true);
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.isDryRun()).isTrue();
  }

  @Test
  void should_report_execution_id() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.engine", "executionId", "MyExecutionId");
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.getCustomExecutionIdTemplate()).contains("MyExecutionId");
  }

  @Test
  void should_report_max_concurrent_queries() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.engine", "maxConcurrentQueries", "10");
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.getMaxConcurrentQueries()).hasValue(10);
  }

  @Test
  void should_throw_when_max_concurrent_queries_invalid() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.engine", "maxConcurrentQueries", "-1");
    EngineSettings settings = new EngineSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.engine.maxConcurrentQueries, expecting positive integer or string in 'nC' syntax, got '-1'");
  }

  @Test
  void should_report_empty_max_concurrent_queries_when_AUTO() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.engine", "maxConcurrentQueries", "AUTO");
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.getMaxConcurrentQueries()).isEmpty();
  }

  @Test
  void should_create_data_size_sampling_enabled() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.engine", "dataSizeSamplingEnabled", false);
    EngineSettings settings = new EngineSettings(config);
    settings.init();
    assertThat(settings.isDataSizeSamplingEnabled()).isFalse();
  }
}
