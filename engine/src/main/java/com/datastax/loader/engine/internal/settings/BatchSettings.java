/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.executor.api.batch.ReactorUnsortedStatementBatcher;
import com.datastax.loader.executor.api.batch.StatementBatcher;

/** */
public class BatchSettings {

  private final LoaderConfig config;

  BatchSettings(LoaderConfig config) {
    this.config = config;
  }

  public ReactorUnsortedStatementBatcher newStatementBatcher(Cluster cluster) {
    return new ReactorUnsortedStatementBatcher(
        cluster,
        config.getEnum(StatementBatcher.BatchMode.class, "mode"),
        config.getInt("maxBatchSize"),
        config.getInt("bufferSize"));
  }
}
