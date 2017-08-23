/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.batch;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Statement;
import java.util.function.Function;
import reactor.core.publisher.Flux;

/**
 * An {@link ReactorStatementBatcher} that implements {@code Function<Flux<Statement>,
 * Flux<Statement>>} so that it can be used in a {@link Flux#compose(Function) compose} operation to
 * batch statements together.
 *
 * <p>This operator assumes that the upstream source delivers statements whose partition keys are
 * randomly distributed; when the internal buffer is full, batches are created with the accumulated
 * items and passed downstream.
 *
 * @see ReactorStatementBatcher
 * @see ReactorSortedStatementBatcher
 */
public class ReactorUnsortedStatementBatcher extends ReactorStatementBatcher
    implements Function<Flux<? extends Statement>, Flux<Statement>> {

  /** The default size for the internal buffer. */
  public static final int DEFAULT_BUFFER_SIZE = 1000;

  private final int bufferSize;

  public ReactorUnsortedStatementBatcher() {
    this(DEFAULT_BUFFER_SIZE);
  }

  public ReactorUnsortedStatementBatcher(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public ReactorUnsortedStatementBatcher(Cluster cluster) {
    this(cluster, DEFAULT_BUFFER_SIZE);
  }

  public ReactorUnsortedStatementBatcher(Cluster cluster, int bufferSize) {
    this(cluster, StatementBatcher.BatchMode.PARTITION_KEY, bufferSize);
  }

  public ReactorUnsortedStatementBatcher(
      Cluster cluster, StatementBatcher.BatchMode batchMode, int bufferSize) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, bufferSize);
  }

  public ReactorUnsortedStatementBatcher(
      Cluster cluster,
      StatementBatcher.BatchMode batchMode,
      BatchStatement.Type batchType,
      int bufferSize) {
    super(cluster, batchMode, batchType);
    this.bufferSize = bufferSize;
  }

  @Override
  public Flux<Statement> apply(Flux<? extends Statement> upstream) {
    return upstream.buffer(bufferSize).map(this::batchByGroupingKey).flatMap(Flux::fromIterable);
  }
}
