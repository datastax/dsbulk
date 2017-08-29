/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static com.datastax.loader.engine.internal.Assertions.assertThat;
import static com.datastax.loader.executor.api.batch.StatementBatcher.BatchMode.PARTITION_KEY;
import static com.datastax.loader.executor.api.batch.StatementBatcher.BatchMode.REPLICA_SET;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.loader.commons.config.DefaultLoaderConfig;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.executor.api.batch.ReactorUnsortedStatementBatcher;
import com.typesafe.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

/** */
public class BatchSettingsTest {

  private Cluster cluster;

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
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("datastax-loader.batch"));
    BatchSettings settings = new BatchSettings(config);
    ReactorUnsortedStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(Whitebox.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(Whitebox.getInternalState(batcher, "bufferSize")).isEqualTo(10000);
    assertThat(Whitebox.getInternalState(batcher, "maxBatchSize")).isEqualTo(100);
  }

  @Test
  public void should_create_batcher_when_batch_mode_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("mode = REPLICA_SET")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.batch")));
    BatchSettings settings = new BatchSettings(config);
    ReactorUnsortedStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(Whitebox.getInternalState(batcher, "batchMode")).isEqualTo(REPLICA_SET);
    assertThat(Whitebox.getInternalState(batcher, "bufferSize")).isEqualTo(10000);
    assertThat(Whitebox.getInternalState(batcher, "maxBatchSize")).isEqualTo(100);
  }

  @Test
  public void should_create_batcher_when_buffer_size_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("bufferSize = 5000")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.batch")));
    BatchSettings settings = new BatchSettings(config);
    ReactorUnsortedStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(Whitebox.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(Whitebox.getInternalState(batcher, "bufferSize")).isEqualTo(5000);
    assertThat(Whitebox.getInternalState(batcher, "maxBatchSize")).isEqualTo(100);
  }

  @Test
  public void should_create_batcher_when_max_batch_size_mode_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxBatchSize = 10")
                .withFallback(ConfigFactory.load().getConfig("datastax-loader.batch")));
    BatchSettings settings = new BatchSettings(config);
    ReactorUnsortedStatementBatcher batcher = settings.newStatementBatcher(cluster);
    assertThat(Whitebox.getInternalState(batcher, "batchMode")).isEqualTo(PARTITION_KEY);
    assertThat(Whitebox.getInternalState(batcher, "bufferSize")).isEqualTo(10000);
    assertThat(Whitebox.getInternalState(batcher, "maxBatchSize")).isEqualTo(10);
  }
}
