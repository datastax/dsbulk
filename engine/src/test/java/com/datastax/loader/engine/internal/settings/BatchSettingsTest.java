/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static com.datastax.loader.engine.internal.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.loader.commons.config.DefaultLoaderConfig;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.executor.api.batch.ReactorSortedStatementBatcher;
import com.datastax.loader.executor.api.batch.ReactorUnsortedStatementBatcher;
import com.typesafe.config.ConfigFactory;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;

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
  public void should_create_unsorted_batcher_when_mode_is_default() throws Exception {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.batch"));
    BatchSettings settings = new BatchSettings(config);
    Function<? super Flux<Statement>, ? extends Flux<Statement>> batcher =
        settings.newStatementBatcher(cluster);
    assertThat(batcher).isNotNull().isInstanceOf(ReactorUnsortedStatementBatcher.class);
  }

  @Test
  public void should_create_sorted_batcher_when_sorted_mode_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.parseString("sorted = true, bufferSize = 100"));
    BatchSettings settings = new BatchSettings(config);
    Function<? super Flux<Statement>, ? extends Flux<Statement>> batcher =
        settings.newStatementBatcher(cluster);
    assertThat(batcher).isNotNull().isInstanceOf(ReactorSortedStatementBatcher.class);
  }

  @Test
  public void should_create_unsorted_batcher_when_unsorted_mode_provided() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.parseString("sorted = false, bufferSize = 100"));
    BatchSettings settings = new BatchSettings(config);
    Function<? super Flux<Statement>, ? extends Flux<Statement>> batcher =
        settings.newStatementBatcher(cluster);
    assertThat(batcher).isNotNull().isInstanceOf(ReactorUnsortedStatementBatcher.class);
  }
}
