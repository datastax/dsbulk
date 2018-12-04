/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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

  /**
   * The default size for the internal buffer.
   */
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
        StatementBatcher.DEFAULT_MAX_BATCH_STATEMENTS,
        bufferSize,
        StatementBatcher.DEFAULT_MAX_SIZE_BYTES);
  }

  public RxJavaUnsortedStatementBatcher(
      Cluster cluster,
      StatementBatcher.BatchMode batchMode,
      int maxBatchStatements,
      int bufferSize,
      long maxSizeInBytes) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, maxBatchStatements, bufferSize, maxSizeInBytes);
  }

  public RxJavaUnsortedStatementBatcher(
      Cluster cluster,
      StatementBatcher.BatchMode batchMode,
      BatchStatement.Type batchType,
      int maxBatchStatements,
      int bufferSize,
      long maxSizeInBytes) {
    super(cluster, batchMode, batchType, maxBatchStatements, maxSizeInBytes);
    this.bufferSize = bufferSize;
  }

  @Override
  public Flowable<Statement> apply(Flowable<Statement> upstream) {
    return upstream.window(bufferSize).flatMap(this::batchByGroupingKey);
  }
}
