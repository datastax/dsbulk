/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Session;
import com.datastax.loader.executor.api.ContinuousRxJavaBulkExecutor;
import com.datastax.loader.executor.api.DefaultRxJavaBulkExecutor;
import com.datastax.loader.executor.api.writer.ReactiveBulkWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** */
public class ExecutorSettingsTest {

  private Session session;

  private ContinuousPagingSession dseSession;

  @Before
  public void setUp() throws Exception {
    session = mock(Session.class);
    dseSession = mock(ContinuousPagingSession.class);
  }

  @Test
  public void should_create_non_continuous_executor_when_session_not_dse() throws Exception {
    Config config = ConfigFactory.load().getConfig("datastax-loader.executor");
    ExecutorSettings settings = new ExecutorSettings(config);
    ReactiveBulkWriter executor = settings.newWriteEngine(session);
    assertThat(executor).isNotNull().isInstanceOf(DefaultRxJavaBulkExecutor.class);
  }

  @Test
  public void should_create_continuous_executor_when_session_dse() throws Exception {
    Config config = ConfigFactory.load().getConfig("datastax-loader.executor");
    ExecutorSettings settings = new ExecutorSettings(config);
    ReactiveBulkWriter executor = settings.newWriteEngine(dseSession);
    assertThat(executor).isNotNull().isInstanceOf(ContinuousRxJavaBulkExecutor.class);
  }
}
