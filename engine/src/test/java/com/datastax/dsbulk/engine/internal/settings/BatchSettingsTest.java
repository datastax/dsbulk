/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.PARTITION_KEY;
import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.REPLICA_SET;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.executor.api.batch.ReactorStatementBatcher;
import com.typesafe.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

/** */
public class BatchSettingsTest {

  private Cluster cluster;

  @SuppressWarnings("Duplicates")
  @Before
  public void setUp() throws Exception {
    cluster = mock(Cluster.class);
    Configuration configuration = mock(Configuration.class);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
    when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Test
  public void should_create_batcher_when_mode_is_default() throws Exception {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.batch"));
    BatchSettings settings = new BatchSettings(config);
    assertThat(settings.getBufferSize()).isEqualTo(32);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(Whitebox.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(Whitebox.getInternalState(batcher, "maxBatchSize")).isEqualTo(32);
  }

  @Test
  public void should_create_batcher_when_batch_mode_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("mode = REPLICA_SET")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    assertThat(settings.getBufferSize()).isEqualTo(32);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(Whitebox.getInternalState(batcher, "batchMode")).isEqualTo(REPLICA_SET);
    assertThat(Whitebox.getInternalState(batcher, "maxBatchSize")).isEqualTo(32);
  }

  @Test
  public void should_create_batcher_when_buffer_size_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("bufferSize = 5000")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    assertThat(settings.getBufferSize()).isEqualTo(5000);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(Whitebox.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(Whitebox.getInternalState(batcher, "maxBatchSize")).isEqualTo(32);
  }

  @Test
  public void should_create_batcher_when_max_batch_size_mode_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxBatchSize = 10")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.batch")));
    BatchSettings settings = new BatchSettings(config);
    // buffer size should implicitly be updated when max batch size is changed and it isn't specified.
    assertThat(settings.getBufferSize()).isEqualTo(10);
    ReactorStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(Whitebox.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(Whitebox.getInternalState(batcher, "maxBatchSize")).isEqualTo(10);
  }
}
