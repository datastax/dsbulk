/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.statement.RxJavaSortedStatementBatcher;
import com.datastax.loader.executor.api.statement.RxJavaUnsortedStatementBatcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.reactivex.FlowableTransformer;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    Config config = ConfigFactory.load().getConfig("datastax-loader.batch");
    BatchSettings settings = new BatchSettings(config);
    FlowableTransformer<Statement, Statement> batcher = settings.newStatementBatcher(cluster);
    assertThat(batcher).isNotNull().isInstanceOf(RxJavaUnsortedStatementBatcher.class);
  }

  @Test
  public void should_create_sorted_batcher_when_sorted_mode_provided() throws Exception {
    Config config = ConfigFactory.parseString("mode = SORTED, buffer-size = 100");
    BatchSettings settings = new BatchSettings(config);
    FlowableTransformer<Statement, Statement> batcher = settings.newStatementBatcher(cluster);
    assertThat(batcher).isNotNull().isInstanceOf(RxJavaSortedStatementBatcher.class);
  }

  @Test
  public void should_create_unsorted_batcher_when_unsorted_mode_provided() throws Exception {
    Config config = ConfigFactory.parseString("mode = UNSORTED, buffer-size = 100");
    BatchSettings settings = new BatchSettings(config);
    FlowableTransformer<Statement, Statement> batcher = settings.newStatementBatcher(cluster);
    assertThat(batcher).isNotNull().isInstanceOf(RxJavaUnsortedStatementBatcher.class);
  }
}
