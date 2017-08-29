/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.loader.commons.config.DefaultLoaderConfig;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.executor.api.ContinuousReactorBulkExecutor;
import com.datastax.loader.executor.api.DefaultReactorBulkExecutor;
import com.datastax.loader.executor.api.reader.ReactiveBulkReader;
import com.datastax.loader.executor.api.writer.ReactiveBulkWriter;
import com.typesafe.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

/** */
public class ExecutorSettingsTest {

  private Session session;

  private ContinuousPagingSession dseSession;

  @Before
  public void setUp() throws Exception {
    session = mock(Session.class);
    dseSession = mock(ContinuousPagingSession.class);
    Cluster cluster = mock(Cluster.class);
    when(dseSession.getCluster()).thenReturn(cluster);
    Configuration configuration = mock(Configuration.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.DSE_V1);
  }

  @Test
  public void should_create_non_continuous_executor_when_write_workflow() throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.executor"));
    ExecutorSettings settings = new ExecutorSettings(config);
    ReactiveBulkWriter executor = settings.newWriteExecutor(session, null);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
  }

  @Test
  public void should_create_non_continuous_executor_when_read_workflow_and_session_not_dse()
      throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.executor"));
    ExecutorSettings settings = new ExecutorSettings(config);
    ReactiveBulkReader executor = settings.newReadExecutor(session, null);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
  }

  @Test
  public void should_create_continuous_executor_when_read_workflow_and_session_dse()
      throws Exception {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.executor"));
    ExecutorSettings settings = new ExecutorSettings(config);
    ReactiveBulkReader executor = settings.newReadExecutor(dseSession, null);
    assertThat(executor).isNotNull().isInstanceOf(ContinuousReactorBulkExecutor.class);
  }
}
