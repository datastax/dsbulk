/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor.batch;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/** A subclass of {@link StatementBatcher} that adds reactive-style capabilities to it. */
public class ReactorStatementBatcher extends StatementBatcher {

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@link ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and
   * the default {@link CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It also uses the
   * default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and the
   * default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   */
  public ReactorStatementBatcher() {}

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@link ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and
   * the default {@link CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance and the default
   * {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited). It uses the given
   * maximum number of statements.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public ReactorStatementBatcher(int maxBatchStatements) {
    super(maxBatchStatements);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@link ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and
   * the default {@link CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance and the default
   * {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100). It uses the
   * given maximum data size in bytes.
   *
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public ReactorStatementBatcher(long maxSizeInBytes) {
    super(maxSizeInBytes);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@link ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and
   * the default {@link CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It uses the given
   * maximum number of statements and the maximum data size in bytes.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public ReactorStatementBatcher(int maxBatchStatements, long maxSizeInBytes) {
    super(maxBatchStatements, maxSizeInBytes);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the given {@link Cluster} as its source for the {@link ProtocolVersion protocol
   * version} and the {@link CodecRegistry} instance to use. It also uses the default {@linkplain
   * #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and the default {@linkplain
   * #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   */
  public ReactorStatementBatcher(@NotNull Cluster cluster) {
    super(cluster);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in the
   * specified {@link com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode batch mode}
   * and uses the given {@link Cluster} as its source for the {@link ProtocolVersion protocol
   * version} and the {@link CodecRegistry} instance to use. It also uses the default {@linkplain
   * #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and the default {@linkplain
   * #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   */
  public ReactorStatementBatcher(@NotNull Cluster cluster, @NotNull BatchMode batchMode) {
    super(cluster, batchMode);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * operates in the specified {@code batchMode} and uses the given {@link Cluster} as its source
   * for the {@link ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use.
   * It uses the given maximum number of statements and the default {@linkplain
   * #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public ReactorStatementBatcher(
      @NotNull Cluster cluster,
      @NotNull BatchMode batchMode,
      @NotNull BatchStatement.Type batchType,
      int maxBatchStatements) {
    super(cluster, batchMode, batchType, maxBatchStatements);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * operates in the specified {@code batchMode} and uses the given {@link Cluster} as its source
   * for the {@link ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use.
   * It uses the given maximum number of statements and the maximum data size in bytes.
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public ReactorStatementBatcher(
      @NotNull Cluster cluster,
      @NotNull BatchMode batchMode,
      @NotNull BatchStatement.Type batchType,
      int maxBatchStatements,
      long maxSizeInBytes) {
    super(cluster, batchMode, batchType, maxBatchStatements, maxSizeInBytes);
  }

  /**
   * Batches together the given statements into groups of statements having the same grouping key.
   * Each group size is capped by the maximum number of statements and the maximum data size.
   *
   * <p>The grouping key to use is determined by the {@link
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode batch mode} in use by this
   * statement batcher.
   *
   * <p>Note that when a resulting group contains only one statement, this method will not create a
   * batch statement containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of statements for the same grouping key is greater than the maximum number
   * of statements, or when their total data size is greater than the maximum data size, statements
   * will be split into smaller batches.
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
   * @return A {@link Flux} of batched statements.
   */
  @NotNull
  public Flux<Statement> batchByGroupingKey(@NotNull Publisher<? extends Statement> statements) {
    return Flux.from(statements).groupBy(this::groupingKey).flatMap(this::batchAll);
  }

  /**
   * Batches together all the given statements into groups of statements. Each group size is capped
   * by the maximum number of statements and the maximum data size.
   *
   * <p>Note that when a resulting group contains only one statement, this method will not create a
   * batch statement containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of statements for the same grouping key is greater than the maximum number
   * of statements, or when their total data size is greater than the maximum data size, statements
   * will be split into smaller batches.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}, the resulting batch could
   * lead to write throughput degradation.
   *
   * @param statements the statements to batch together.
   * @return A {@link Flux} of batched statements.
   */
  @NotNull
  public Flux<? extends Statement> batchAll(@NotNull Publisher<? extends Statement> statements) {
    return Flux.from(statements)
        .cast(Statement.class)
        .windowUntil(new ReactorAdaptiveSizingBatchPredicate(), false)
        .flatMap(
            stmts ->
                Flux.from(stmts)
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

  private class ReactorAdaptiveSizingBatchPredicate extends AdaptiveSizingBatchPredicate {}
}
