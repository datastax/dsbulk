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
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import hu.akarnokd.rxjava2.operators.FlowableTransformers;
import io.reactivex.Flowable;
import io.reactivex.functions.Predicate;
import org.reactivestreams.Publisher;

/** A subclass of {@link StatementBatcher} that adds reactive-style capabilities to it. */
public class RxJavaStatementBatcher extends StatementBatcher {

  /**
   * Creates a new {@link RxJavaStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@link ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and
   * the default {@link CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It also uses the
   * default maximum batch size (100).
   */
  public RxJavaStatementBatcher() {}

  /**
   * Creates a new {@link RxJavaStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@link ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and
   * the default {@link CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It uses the given
   * maximum batch size.
   *
   * @param maxBatchStatements The maximum number of elements in a batch
   */
  public RxJavaStatementBatcher(int maxBatchStatements) {
    super(maxBatchStatements);
  }

  /**
   * Creates a new {@link RxJavaStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@link ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and
   * the default {@link CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It uses the given
   * maximum size in bytes.
   *
   * @param maxSizeInBytes The maximum size in bytes of a batch.
   */
  public RxJavaStatementBatcher(long maxSizeInBytes) {
    super(maxSizeInBytes);
  }

  /**
   * Creates a new {@link RxJavaStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@link ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and
   * the default {@link CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It uses the given
   * max batch statements and maximum size in bytes
   *
   * @param maxSizeInBytes The maximum size in bytes of a batch.
   * @param maxBatchStatements - the maximum number of statements in one batch
   */
  public RxJavaStatementBatcher(int maxBatchStatements, long maxSizeInBytes) {
    super(maxBatchStatements, maxSizeInBytes);
  }

  /**
   * Creates a new {@link RxJavaStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the given {@link Cluster} as its source for the {@link ProtocolVersion protocol
   * version} and the {@link CodecRegistry} instance to use. It also uses the default maximum batch
   * size (100).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   */
  public RxJavaStatementBatcher(Cluster cluster) {
    super(cluster);
  }

  /**
   * Creates a new {@link RxJavaStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in the
   * specified {@link com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode batch mode}
   * and uses the given {@link Cluster} as its source for the {@link ProtocolVersion protocol
   * version} and the {@link CodecRegistry} instance to use. It also uses the default maximum batch
   * size (100).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   */
  public RxJavaStatementBatcher(Cluster cluster, BatchMode batchMode) {
    super(cluster, batchMode);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * operates in the specified {@code batchMode} and uses the given {@link Cluster} as its source
   * for the {@link ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use.
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of elements in a batch
   */
  public RxJavaStatementBatcher(
      Cluster cluster, BatchMode batchMode, BatchStatement.Type batchType, int maxBatchStatements) {
    super(cluster, batchMode, batchType, maxBatchStatements);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * operates in the specified {@code batchMode} and uses the given {@link Cluster} as its source
   * for the {@link ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use.
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxSizeInBytes The maximum size in bytes of a batch.
   * @param maxBatchStatements - the maximum number of statements in one batch
   */
  public RxJavaStatementBatcher(
      Cluster cluster,
      BatchMode batchMode,
      BatchStatement.Type batchType,
      int maxBatchStatements,
      int maxSizeInBytes) {
    super(cluster, batchMode, batchType, maxBatchStatements, maxSizeInBytes);
  }

  /**
   * Batches together the given statements into groups of statements having the same grouping key.
   *
   * <p>The grouping key to use is determined by the {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode batch mode} in use by this
   * statement batcher.
   *
   * <p>When the number of statements for the same grouping key is greater than the maximum batch
   * size, statements will be split in different batches.
   *
   * <p>When {@link com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY
   * PARTITION_KEY} is used, the grouping key is the statement's {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or {@link
   * Statement#getRoutingToken() routing token}, whichever is available.
   *
   * <p>When {@link com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#REPLICA_SET
   * REPLICA_SET} is used, the grouping key is the replica set owning the statement's {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or {@link
   * Statement#getRoutingToken() routing token}, whichever is available.
   *
   * @param statements the statements to batch together.
   * @return A publisher of batched statements.
   */
  public Flowable<Statement> batchByGroupingKey(Publisher<? extends Statement> statements) {
    return Flowable.fromPublisher(statements).groupBy(this::groupingKey).flatMap(this::batchAll);
  }

  /**
   * Batches together all the given statements into groups of statements.
   *
   * <p>Note that when a group contains one single statement, this method will not create a batch
   * statement containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of given statements is greater than the max batch statements OR max size in
   * bytes, this method will split them into different batches.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}, the resulting batch could
   * lead to write throughput degradation.
   *
   * @param statements the statements to batch together.
   * @return A {@link Flowable} of batched statements.
   */
  public Flowable<? extends Statement> batchAll(Publisher<? extends Statement> statements) {

    return Flowable.fromPublisher(statements)
        .cast(Statement.class)
        .compose(FlowableTransformers.bufferUntil(new AdaptiveSizingBatchPredicate()))
        .flatMapMaybe(
            stmts ->
                Flowable.fromIterable(stmts)
                    .reduce(
                        (s1, s2) -> {
                          if (s1 instanceof BatchStatement) {
                            ((BatchStatement) s1).add(s2);
                            return s1;
                          } else {
                            return new BatchStatement(batchType).add(s1).add(s2);
                          }
                        }));
  }

  private class AdaptiveSizingBatchPredicate implements Predicate<Statement> {

    private int statementsCounter = 0;
    private long bytesInCurrentBatch = 0;

    @Override
    public boolean test(Statement statement) {
      boolean statementsOverflowBuffer = ++statementsCounter >= getMaxBatchStatements();
      boolean bytesOverflowBuffer =
          (bytesInCurrentBatch += calculateSize(statement)) >= getMaxSizeInBytes();

      boolean shouldFlush = statementsOverflowBuffer || bytesOverflowBuffer;
      if (shouldFlush) {
        statementsCounter = 0;
        bytesInCurrentBatch = 0;
      }
      return shouldFlush;
    }
  }

  private int calculateSize(Statement statement) {
    return statement.requestSizeInBytes(protocolVersion, codecRegistry);
  }

  private int getMaxBatchStatements() {
    if (maxBatchStatements <= 0) {
      return Integer.MAX_VALUE;
    }
    return maxBatchStatements;
  }

  private long getMaxSizeInBytes() {
    if (maxSizeInBytes <= 0) {
      return Long.MAX_VALUE;
    }
    return maxSizeInBytes;
  }
}
