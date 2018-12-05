/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.PARTITION_KEY;
import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.REPLICA_SET;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.dsbulk.executor.reactor.batch.ReactorStatementBatcher;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BatchSettingsTest {

  private Cluster cluster;

  @SuppressWarnings("Duplicates")
  @BeforeEach
  void setUp() {
    cluster = mock(Cluster.class);
    Configuration configuration = mock(Configuration.class);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
    when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Test
  void should_create_batcher_when_mode_is_default() {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.batch"));
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(32);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_for_deprecated_maxBatchSize_and_treat_it_as_maxBatchStatements() {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString(
                    "maxBatchSize = 32, bufferSize = 32, mode = PARTITION_KEY, maxBatchStatements = null"))
            .withFallback(ConfigFactory.load().getConfig("dsbulk.batch"));

    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(32);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_mode_is_default_for_new_maxBatchStatements() {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString(
                    "maxBatchStatements = 32, bufferSize = 32, mode = PARTITION_KEY"))
            .withFallback(ConfigFactory.load().getConfig("dsbulk.batch"));
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(32);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_throw_when_both_maxBatchStatements_and_maxBatchSize_is_specified() {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString(
                    "maxBatchStatements = 32, maxBatchSize = 32, bufferSize = 32, mode = PARTITION_KEY"))
            .withFallback(ConfigFactory.load().getConfig("dsbulk.batch"));
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Settings batch.maxBatchStatements and batch.maxBatchSize cannot be both defined; consider using batch.maxBatchStatements exclusively, because batch.maxBatchSize is deprecated.");
  }

  @Test
  void should_create_batcher_when_batch_mode_provided() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("mode = REPLICA_SET")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(32);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(REPLICA_SET);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_buffer_size_provided() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("bufferSize = 5000")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(settings.getBufferSize()).isEqualTo(5000);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_max_batch_statements_mode_provided() {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString(
                    "maxBatchStatements = 10, mode = PARTITION_KEY, bufferSize = -1"))
            .withFallback(ConfigFactory.load().getConfig("dsbulk.batch"));
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    // buffer size should implicitly be updated when max batch size is changed and it isn't
    // specified.
    assertThat(settings.getBufferSize()).isEqualTo(10);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(10);
  }

  @Test
  void should_throw_exception_when_buffer_size_less_than_max_batch_size() {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString(
                    "maxBatchSize = 10, bufferSize = 5,mode = PARTITION_KEY, maxBatchStatements = null"))
            .withFallback(ConfigFactory.load().getConfig("dsbulk.batch"));
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Value for batch.bufferSize (5) must be greater than or equal to buffer.maxBatchStatements OR buffer.maxBatchSize (10). See settings.md for more information.");
  }

  @Test
  void should_throw_exception_when_buffer_size_less_than_max_batch_statements() {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString(
                    "maxBatchStatements = 10, bufferSize = 5,mode = PARTITION_KEY"))
            .withFallback(ConfigFactory.load().getConfig("dsbulk.batch"));
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Value for batch.bufferSize (5) must be greater than or equal to buffer.maxBatchStatements OR buffer.maxBatchSize (10). See settings.md for more information.");
  }

  @Test
  void should_throw_exception_when_max_batch_size_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxBatchSize = NotANumber, maxBatchStatements = null")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for batch.maxBatchSize: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_max_batch_statements_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxBatchStatements = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for batch.maxBatchStatements: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_max_size_in_bytes_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxSizeInBytes = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for batch.maxSizeInBytes: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_buffer_size_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("bufferSize = NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for batch.bufferSize: Expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_batch_mode_invalid() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("mode = NotAMode")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value at 'mode': Expecting one of DISABLED, PARTITION_KEY, REPLICA_SET, got 'NotAMode'");
  }

  @Test
  void should_throw_exception_when_max_batch_statements_and_max_size_in_bytes_are_non_positive() {
    LoaderConfig config =
        new DefaultLoaderConfig(
                ConfigFactory.parseString(
                    "mode: PARTITION_KEY, maxBatchStatements = -1, maxSizeInBytes = 0"))
            .withFallback(ConfigFactory.load().getConfig("dsbulk.batch"));
    BatchSettings settings = new BatchSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "At least one of batch.maxSizeInBytes or batch.maxBatchStatements must be positive. See settings.md for more information.");
  }

  @Test
  void should_load_config_when_only_max_size_in_bytes_specified() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "mode: PARTITION_KEY, maxSizeInBytes = 1, bufferSize = 1, maxBatchStatements = -1"));
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(ReflectionUtils.getInternalState(settings, "maxSizeInBytes")).isEqualTo(1L);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxSizeInBytes")).isEqualTo(1L);
  }

  @Test
  void should_load_config_when_max_size_in_bytes_and_max_batch_sizes_specified() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "mode: PARTITION_KEY, maxSizeInBytes = 1, bufferSize = 10, maxBatchSize = 10"));
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(ReflectionUtils.getInternalState(settings, "maxSizeInBytes")).isEqualTo(1L);
    assertThat(ReflectionUtils.getInternalState(settings, "maxBatchStatements")).isEqualTo(10);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxSizeInBytes")).isEqualTo(1L);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(10);
  }

  @Test
  void should_load_config_when_max_size_in_bytes_and_max_batch_statements_specified() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                "mode: PARTITION_KEY, maxSizeInBytes = 1, bufferSize = 10, maxBatchStatements = 10"));
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    assertThat(ReflectionUtils.getInternalState(settings, "maxSizeInBytes")).isEqualTo(1L);
    assertThat(ReflectionUtils.getInternalState(settings, "maxBatchStatements")).isEqualTo(10);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxSizeInBytes")).isEqualTo(1L);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchStatements")).isEqualTo(10);
  }
}
