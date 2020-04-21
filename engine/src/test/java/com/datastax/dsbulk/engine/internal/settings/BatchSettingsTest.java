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
import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.PARTITION_KEY;
import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.REPLICA_SET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dsbulk.commons.tests.driver.DriverUtils;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.executor.reactor.batch.ReactorStatementBatcher;
import com.datastax.oss.driver.api.core.CqlSession;
import com.typesafe.config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BatchSettingsTest {

  private CqlSession session;

  @BeforeEach
  void setUp() {
    session = DriverUtils.mockSession();
  }

  @Test
  void should_create_batcher_when_mode_is_default() {
    Config config = createTestConfig("dsbulk.batch");
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(128);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_for_deprecated_maxBatchSize_and_treat_it_as_maxBatchStatements() {
    Config config =
        createTestConfig(
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
    ReactorStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_mode_is_default_for_new_maxBatchStatements() {
    Config config =
        createTestConfig(
            "dsbulk.batch", "maxBatchStatements", 32, "bufferSize", 32, "mode", "PARTITION_KEY");
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(32);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_throw_when_both_maxBatchStatements_and_maxBatchSize_is_specified() {
    Config config =
        createTestConfig(
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
    Config config = createTestConfig("dsbulk.batch", "mode", "REPLICA_SET");
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(128);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(REPLICA_SET);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_buffer_size_provided() {
    Config config = createTestConfig("dsbulk.batch", "bufferSize", 5000);
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(5000);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_max_batch_statements_mode_provided() {
    Config config =
        createTestConfig(
            "dsbulk.batch", "maxBatchStatements", 10, "mode", "PARTITION_KEY", "bufferSize", -1);
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    // buffer size should implicitly be updated when max batch size is changed and it isn't
    // specified.
    assertThat(settings.getBufferSize()).isEqualTo(40);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(10);
  }

  @Test
  void should_throw_exception_when_buffer_size_less_than_max_batch_size() {
    Config config =
        createTestConfig(
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
        createTestConfig(
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
        createTestConfig(
            "dsbulk.batch", "maxBatchSize", "NotANumber", "maxBatchStatements", "null");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.maxBatchSize, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_max_batch_statements_not_a_number() {
    Config config = createTestConfig("dsbulk.batch", "maxBatchStatements", "NotANumber");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.maxBatchStatements, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_max_size_in_bytes_not_a_number() {
    Config config = createTestConfig("dsbulk.batch", "maxSizeInBytes", "NotANumber");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.maxSizeInBytes, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_buffer_size_not_a_number() {
    Config config = createTestConfig("dsbulk.batch", "bufferSize", "NotANumber");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.bufferSize, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_batch_mode_invalid() {
    Config config = createTestConfig("dsbulk.batch", "mode", "NotAMode");
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.batch.mode, expecting one of DISABLED, PARTITION_KEY, REPLICA_SET, got: 'NotAMode'");
  }

  @Test
  void should_throw_exception_when_max_batch_statements_and_max_size_in_bytes_are_non_positive() {
    Config config =
        createTestConfig(
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
        createTestConfig(
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
        createTestConfig(
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
    ReactorStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxSizeInBytes")).isEqualTo(1L);
  }

  @Test
  void should_load_config_when_max_size_in_bytes_and_max_batch_sizes_specified() {
    Config config =
        createTestConfig(
            "dsbulk.batch",
            "mode",
            "PARTITION_KEY",
            "maxSizeInBytes",
            1,
            "bufferSize",
            10,
            "maxBatchSize",
            10,
            "maxBatchStatements",
            null);
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(ReflectionUtils.getInternalState(settings, "maxSizeInBytes")).isEqualTo(1L);
    assertThat(ReflectionUtils.getInternalState(settings, "maxBatchStatements")).isEqualTo(10);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxSizeInBytes")).isEqualTo(1L);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(10);
  }

  @Test
  void should_load_config_when_max_size_in_bytes_and_max_batch_statements_specified() {
    Config config =
        createTestConfig(
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
    ReactorStatementBatcher batcher = settings.newStatementBatcher(session);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxSizeInBytes")).isEqualTo(1L);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(10);
  }
}
