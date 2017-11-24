/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava.batch;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

/**
 * An {@link RxJavaStatementBatcher} that implements {@link FlowableTransformer} so that it can be
 * used in a {@link Flowable#compose(FlowableTransformer) compose} operation to batch statements
 * together.
 *
 * <p>This operator assumes that the upstream source delivers statements whose partition keys are
 * randomly distributed; when the internal buffer is full, batches are created with the accumulated
 * items and passed downstream.
 *
 * @see RxJavaStatementBatcher
 * @see RxJavaSortedStatementBatcher
 */
public class RxJavaUnsortedStatementBatcher extends RxJavaStatementBatcher
    implements FlowableTransformer<Statement, Statement> {

  /** The default size for the internal buffer. */
  public static final int DEFAULT_BUFFER_SIZE = 512;

  private final int bufferSize;

  public RxJavaUnsortedStatementBatcher() {
    this(DEFAULT_BUFFER_SIZE);
  }

  public RxJavaUnsortedStatementBatcher(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public RxJavaUnsortedStatementBatcher(Cluster cluster) {
    this(cluster, DEFAULT_BUFFER_SIZE);
  }

  public RxJavaUnsortedStatementBatcher(Cluster cluster, int bufferSize) {
    this(
        cluster,
        StatementBatcher.BatchMode.PARTITION_KEY,
        StatementBatcher.DEFAULT_MAX_BATCH_SIZE,
        bufferSize);
  }

  public RxJavaUnsortedStatementBatcher(
      Cluster cluster, StatementBatcher.BatchMode batchMode, int maxBatchSize, int bufferSize) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, maxBatchSize, bufferSize);
  }

  public RxJavaUnsortedStatementBatcher(
      Cluster cluster,
      StatementBatcher.BatchMode batchMode,
      BatchStatement.Type batchType,
      int maxBatchSize,
      int bufferSize) {
    super(cluster, batchMode, batchType, maxBatchSize);
    this.bufferSize = bufferSize;
  }

  @Override
  public Flowable<Statement> apply(Flowable<Statement> upstream) {
    return upstream.window(bufferSize).flatMap(this::batchByGroupingKey);
  }
}
