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
import static org.junit.jupiter.api.Assertions.assertThrows;
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

/** */
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
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchSize")).isEqualTo(32);
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
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchSize")).isEqualTo(32);
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
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchSize")).isEqualTo(32);
  }

  @Test
  void should_create_batcher_when_max_batch_size_mode_provided() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxBatchSize = 10")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    settings.init();
    // buffer size should implicitly be updated when max batch size is changed and it isn't
    // specified.
    assertThat(settings.getBufferSize()).isEqualTo(10);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(ReflectionUtils.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(ReflectionUtils.getInternalState(batcher, "maxBatchSize")).isEqualTo(10);
  }

  @Test
  void should_throw_exception_when_buffer_size_less_than_max_batch_size() {
    assertThrows(
        BulkConfigurationException.class,
        () -> {
          LoaderConfig config =
              new DefaultLoaderConfig(
                  ConfigFactory.parseString("maxBatchSize = 10, " + "bufferSize = 5")
                      .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
          BatchSettings settings = new BatchSettings(config);
          settings.init();
        },
        "batch.bufferSize (5) must be greater than or equal to buffer.maxBatchSize (10). See settings.md for more information.");
  }
}
