/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.statement.RxJavaSortedStatementBatcher;
import com.datastax.loader.executor.api.statement.RxJavaUnsortedStatementBatcher;
import com.typesafe.config.Config;
import io.reactivex.FlowableTransformer;

/** */
public class BatchSettings {

  /** */
  @SuppressWarnings("unused")
  public enum BatchMode {
    UNSORTED {
      @Override
      public FlowableTransformer<Statement, Statement> newStatementBatcher(
          Cluster cluster, int bufferSize) {
        return new RxJavaUnsortedStatementBatcher(cluster, bufferSize);
      }
    },

    SORTED {
      @Override
      public FlowableTransformer<Statement, Statement> newStatementBatcher(
          Cluster cluster, int bufferSize) {
        return new RxJavaSortedStatementBatcher(cluster, bufferSize);
      }
    };

    public abstract FlowableTransformer<Statement, Statement> newStatementBatcher(
        Cluster cluster, int bufferSize);
  }

  private final Config config;

  public BatchSettings(Config config) {
    this.config = config;
  }

  public FlowableTransformer<Statement, Statement> newStatementBatcher(Cluster cluster) {
    BatchMode batchMode = config.getEnum(BatchMode.class, "mode");
    return batchMode.newStatementBatcher(cluster, config.getInt("buffer-size"));
  }
}
