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

import static com.datastax.oss.dsbulk.batcher.api.BatchMode.PARTITION_KEY;
import static com.datastax.oss.dsbulk.batcher.api.BatchMode.REPLICA_SET;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.batcher.api.ReactiveStatementBatcher;
import com.datastax.oss.dsbulk.batcher.reactor.ReactorStatementBatcher;
import com.datastax.oss.dsbulk.tests.driver.DriverUtils;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.utils.ReflectionUtils;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.typesafe.config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
class BatchSettingsTest {

  private CqlSession session;

  @BeforeEach
  void setUp() {
    session = DriverUtils.mockSession();
  }

  @Test
  void should_create_batcher_when_mode_is_default() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.batch");
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(128);
    ReactiveStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(batcher).isInstanceOf(ReactorStatementBatcher.class);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_for_deprecated_maxBatchSize_and_treat_it_as_maxBatchStatements() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch",
            "maxBatchSize",
            32,
            "bufferSize",
            32,
            "mode",
            "PARTITION_KEY",
            "maxBatchStatements",
            null);
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(32);
    ReactiveStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(batcher).isInstanceOf(ReactorStatementBatcher.class);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_mode_is_default_for_new_maxBatchStatements() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch", "maxBatchStatements", 32, "bufferSize", 32, "mode", "PARTITION_KEY");
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(32);
    ReactiveStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(batcher).isInstanceOf(ReactorStatementBatcher.class);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_throw_when_both_maxBatchStatements_and_maxBatchSize_is_specified() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch",
            "maxBatchStatements",
            32,
            "maxBatchSize",
            32,
            "bufferSize",
            32,
            "mode",
            "PARTITION_KEY");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Settings batch.maxBatchStatements and batch.maxBatchSize cannot be both defined; "
                + "consider using batch.maxBatchStatements exclusively, "
                + "because batch.maxBatchSize is deprecated.");
  }

  @Test
  void should_create_batcher_when_batch_mode_provided() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.batch", "mode", "REPLICA_SET");
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(128);
    ReactiveStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(batcher).isInstanceOf(ReactorStatementBatcher.class);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(REPLICA_SET);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_buffer_size_provided() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.batch", "bufferSize", 5000);
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(5000);
    ReactiveStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(batcher).isInstanceOf(ReactorStatementBatcher.class);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_max_batch_statements_mode_provided() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch", "maxBatchStatements", 10, "mode", "PARTITION_KEY", "bufferSize", -1);
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    // buffer size should implicitly be updated when max batch size is changed and it isn't
    // specified.
    assertThat(settings.getBufferSize()).isEqualTo(40);
    ReactiveStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(batcher).isInstanceOf(ReactorStatementBatcher.class);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(10);
  }

  @Test
  void should_throw_exception_when_buffer_size_less_than_max_batch_size() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch",
            "maxBatchSize",
            10,
            "bufferSize",
            5,
            "mode",
            "PARTITION_KEY",
            "maxBatchStatements",
            null);
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Value for batch.bufferSize (5) must be greater than or equal to "
                + "batch.maxBatchStatements OR batch.maxBatchSize (10). See settings.md for more information.");
  }

  @Test
  void should_throw_exception_when_buffer_size_less_than_max_batch_statements() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch", "maxBatchStatements", 10, "bufferSize", 5, "mode", "PARTITION_KEY");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Value for batch.bufferSize (5) must be greater than or equal to "
                + "batch.maxBatchStatements OR batch.maxBatchSize (10). See settings.md for more information.");
  }

  @Test
  void should_throw_exception_when_max_batch_size_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch", "maxBatchSize", "NotANumber", "maxBatchStatements", "null");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.maxBatchSize, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_max_batch_statements_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.batch", "maxBatchStatements", "NotANumber");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.maxBatchStatements, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_max_size_in_bytes_not_a_number() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.batch", "maxSizeInBytes", "NotANumber");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.maxSizeInBytes, expecting NUMBER or STRING in size-in-bytes format, got 'NotANumber'");
  }

  @Test
  void should_throw_exception_when_buffer_size_not_a_number() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.batch", "bufferSize", "NotANumber");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.bufferSize, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_batch_mode_invalid() {
    Config config = TestConfigUtils.createTestConfig("dsbulk.batch", "mode", "NotAMode");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.mode, expecting one of DISABLED, PARTITION_KEY, REPLICA_SET, got: 'NotAMode'");
  }

  @Test
  void should_throw_exception_when_max_batch_statements_and_max_size_in_bytes_are_non_positive() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch", "mode", "PARTITION_KEY", "maxBatchStatements", -1, "maxSizeInBytes", 0);
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "At least one of batch.maxSizeInBytes or batch.maxBatchStatements must be positive. See settings.md for more information.");
  }

  @Test
  void should_throw_exception_when_max_batch_statements_and_buffer_size_are_non_positive() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch",
            "mode",
            "PARTITION_KEY",
            "maxSizeInBytes",
            100,
            "bufferSize",
            -1,
            "maxBatchStatements",
            0);
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Value for batch.bufferSize (0) must be positive if batch.maxBatchStatements is "
                + "negative or zero. See settings.md for more information.");
  }

  @Test
  void should_load_config_when_only_max_size_in_bytes_specified() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch",
            "mode",
            "PARTITION_KEY",
            "maxSizeInBytes",
            1,
            "bufferSize",
            1,
            "maxBatchStatements",
            -1);
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(ReflectionUtils.getInternalState(settings, "maxSizeInBytes")).isEqualTo(1L);
    ReactiveStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(batcher).isInstanceOf(ReactorStatementBatcher.class);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxSizeInBytes")).isEqualTo(1L);
  }

  @Test
  void should_load_config_when_max_size_in_bytes_and_max_batch_sizes_specified() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch",
            "mode",
            "PARTITION_KEY",
            "maxSizeInBytes",
            "1K",
            "bufferSize",
            10,
            "maxBatchSize",
            10,
            "maxBatchStatements",
            null);
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(ReflectionUtils.getInternalState(settings, "maxSizeInBytes")).isEqualTo(1024L);
    assertThat(ReflectionUtils.getInternalState(settings, "maxBatchStatements")).isEqualTo(10);
    ReactiveStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(batcher).isInstanceOf(ReactorStatementBatcher.class);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxSizeInBytes")).isEqualTo(1024L);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(10);
  }

  @Test
  void should_load_config_when_max_size_in_bytes_and_max_batch_statements_specified() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.batch",
            "mode",
            "PARTITION_KEY",
            "maxSizeInBytes",
            1,
            "bufferSize",
            10,
            "maxBatchStatements",
            10);
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(ReflectionUtils.getInternalState(settings, "maxSizeInBytes")).isEqualTo(1L);
    assertThat(ReflectionUtils.getInternalState(settings, "maxBatchStatements")).isEqualTo(10);
    ReactiveStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(batcher).isInstanceOf(ReactorStatementBatcher.class);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxSizeInBytes")).isEqualTo(1L);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(10);
  }
}
