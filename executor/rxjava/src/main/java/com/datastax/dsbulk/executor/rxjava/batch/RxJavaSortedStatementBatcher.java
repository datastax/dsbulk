/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava.batch;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.Predicate;

/**
 * An {@link RxJavaStatementBatcher} that implements {@link FlowableTransformer} so that it can be
 * used in a {@link Flowable#compose(FlowableTransformer) compose} operation to batch statements
 * together.
 *
 * <p>This operator assumes that the upstream source delivers statements whose partition keys are
 * already grouped together; when a new partition key is detected, a batch is created with the
 * accumulated items and passed downstream.
 *
 * <p>Use this operator with caution; if the given statements do not have their {@link
 * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} already grouped together,
 * the resulting batch could lead to sub-optimal write performance.
 *
 * @see RxJavaStatementBatcher
 * @see RxJavaUnsortedStatementBatcher
 */
public class RxJavaSortedStatementBatcher extends RxJavaStatementBatcher
    implements FlowableTransformer<Statement, Statement> {

  public RxJavaSortedStatementBatcher() {
    this(StatementBatcher.DEFAULT_MAX_BATCH_SIZE);
  }

  public RxJavaSortedStatementBatcher(int maxBatchSize) {
    super(maxBatchSize);
  }

  public RxJavaSortedStatementBatcher(Cluster cluster) {
    this(cluster, StatementBatcher.DEFAULT_MAX_BATCH_SIZE);
  }

  public RxJavaSortedStatementBatcher(Cluster cluster, int maxBatchSize) {
    this(cluster, StatementBatcher.BatchMode.PARTITION_KEY, maxBatchSize);
  }

  public RxJavaSortedStatementBatcher(
      Cluster cluster, StatementBatcher.BatchMode batchMode, int maxBatchSize) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, maxBatchSize);
  }

  public RxJavaSortedStatementBatcher(
      Cluster cluster,
      StatementBatcher.BatchMode batchMode,
      BatchStatement.Type batchType,
      int maxBatchSize) {
    super(cluster, batchMode, batchType, maxBatchSize);
  }

  @Override
  public Flowable<Statement> apply(Flowable<Statement> upstream) {
    Flowable<Statement> connectableFlowable = upstream.publish().autoConnect(2);
    Flowable<Statement> boundarySelector =
        connectableFlowable.filter(new StatementBatcherPredicate());
    return connectableFlowable.buffer(boundarySelector).flatMapIterable(this::batchAll);
  }

  private class StatementBatcherPredicate implements Predicate<Statement> {

    private Object groupingKey;

    private int size = 0;

    @Override
    public boolean test(Statement statement) {
      boolean bufferFull = ++size > maxBatchSize;
      Object groupingKey = groupingKey(statement);
      boolean groupingKeyChanged =
          this.groupingKey != null && !this.groupingKey.equals(groupingKey);
      this.groupingKey = groupingKey;
      boolean shouldFlush = groupingKeyChanged || bufferFull;
      if (shouldFlush) {
        size = 0;
      }
      return shouldFlush;
    }
  }
}
