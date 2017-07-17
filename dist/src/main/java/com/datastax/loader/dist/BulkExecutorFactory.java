/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.dist;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.Session;
import com.datastax.loader.engine.api.AbstractBulkExecutorBuilder;
import com.datastax.loader.engine.api.ContinuousRxJavaBulkExecutor;
import com.datastax.loader.engine.api.ContinuousRxJavaBulkExecutorBuilder;
import com.datastax.loader.engine.api.DefaultRxJavaBulkExecutor;
import com.datastax.loader.engine.api.DefaultRxJavaBulkExecutorBuilder;
import com.datastax.loader.engine.api.RxJavaBulkExecutor;
import com.datastax.loader.engine.api.writer.RxJavaBulkWriter;
import com.typesafe.config.Config;

/** */
public class BulkExecutorFactory {

  private final Config config;

  public BulkExecutorFactory(Config config) {
    this.config = config;
  }

  public RxJavaBulkWriter newReactiveWriter(Session session) {
    if (session instanceof ContinuousPagingSession) {
      ContinuousRxJavaBulkExecutorBuilder builder =
          ContinuousRxJavaBulkExecutor.builder(((ContinuousPagingSession) session));
      applyConfig(builder);
      return builder.build();
    } else {
      DefaultRxJavaBulkExecutorBuilder builder = DefaultRxJavaBulkExecutor.builder(session);
      applyConfig(builder);
      return builder.build();
    }
  }

  private void applyConfig(AbstractBulkExecutorBuilder<? extends RxJavaBulkExecutor> builder) {}
}
