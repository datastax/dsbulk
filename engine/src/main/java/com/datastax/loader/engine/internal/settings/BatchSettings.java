/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import static com.datastax.loader.engine.internal.settings.BatchSettings.SortingMode.SORTED;
import static com.datastax.loader.engine.internal.settings.BatchSettings.SortingMode.UNSORTED;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Statement;
import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.executor.api.batch.ReactorSortedStatementBatcher;
import com.datastax.loader.executor.api.batch.ReactorUnsortedStatementBatcher;
import java.util.function.Function;
import reactor.core.publisher.Flux;

/** */
public class BatchSettings {

  /** */
  public enum SortingMode {
    UNSORTED {
      @Override
      public ReactorUnsortedStatementBatcher newStatementBatcher(Cluster cluster, int bufferSize) {
        return new ReactorUnsortedStatementBatcher(cluster, bufferSize);
      }
    },

    SORTED {
      @Override
      public ReactorSortedStatementBatcher newStatementBatcher(Cluster cluster, int bufferSize) {
        return new ReactorSortedStatementBatcher(cluster, bufferSize);
      }
    };

    public abstract Function<? super Flux<Statement>, ? extends Flux<Statement>>
        newStatementBatcher(Cluster cluster, int bufferSize);
  }

  private final LoaderConfig config;

  BatchSettings(LoaderConfig config) {
    this.config = config;
  }

  public Function<? super Flux<Statement>, ? extends Flux<Statement>> newStatementBatcher(
      Cluster cluster) {
    SortingMode sortingMode = config.getBoolean("sorted") ? SORTED : UNSORTED;
    return sortingMode.newStatementBatcher(cluster, config.getInt("bufferSize"));
  }
}
