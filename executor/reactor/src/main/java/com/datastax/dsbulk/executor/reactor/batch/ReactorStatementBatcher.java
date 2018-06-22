/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor.batch;

import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/** A subclass of {@link StatementBatcher} that adds reactive-style capabilities to it. */
public class ReactorStatementBatcher extends StatementBatcher {

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol version and the
   * default {@link CodecRegistry#DEFAULT CodecRegistry} instance. It also uses the default
   * {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and the default
   * {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   */
  public ReactorStatementBatcher() {}

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol version and the
   * default {@link CodecRegistry#DEFAULT CodecRegistry} instance and the default {@linkplain
   * #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited). It uses the given maximum
   * number of statements.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public ReactorStatementBatcher(int maxBatchStatements) {
    super(maxBatchStatements);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol version and the
   * default {@link CodecRegistry#DEFAULT CodecRegistry} instance and the default {@linkplain
   * #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100). It uses the given maximum
   * data size in bytes.
   *
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public ReactorStatementBatcher(long maxSizeInBytes) {
    super(maxSizeInBytes);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol version and the
   * default {@link CodecRegistry#DEFAULT CodecRegistry} instance. It uses the given maximum number
   * of statements and the maximum data size in bytes.
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
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#PARTITION_KEY partition key}
   * mode and uses the given {@linkplain CqlSession session} as its source for the {@linkplain
   * ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use. It also uses
   * the default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and
   * the default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param cluster The {@linkplain CqlSession session} to use; cannot be {@code null}.
   */
  public ReactorStatementBatcher(@NonNull CqlSession cluster) {
    super(cluster);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in the specified {@linkplain
   * com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode batch mode} and uses the
   * given {@linkplain CqlSession session} as its source for the {@linkplain ProtocolVersion
   * protocol version} and the {@link CodecRegistry} instance to use. It also uses the default
   * {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and the default
   * {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param cluster The {@linkplain CqlSession session} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   */
  public ReactorStatementBatcher(@NonNull CqlSession cluster, @NonNull BatchMode batchMode) {
    super(cluster, batchMode);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces batches of the given {@code
   * batchType}, operates in the specified {@code batchMode} and uses the given {@linkplain
   * CqlSession session} as its source for the {@linkplain ProtocolVersion protocol version} and the
   * {@link CodecRegistry} instance to use. It uses the given maximum number of statements and the
   * default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param cluster The {@linkplain CqlSession session} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public ReactorStatementBatcher(
      @NonNull CqlSession cluster,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements) {
    super(cluster, batchMode, batchType, maxBatchStatements);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces batches of the given {@code
   * batchType}, operates in the specified {@code batchMode} and uses the given {@linkplain
   * CqlSession session} as its source for the {@linkplain ProtocolVersion protocol version} and the
   * {@link CodecRegistry} instance to use. It uses the given maximum number of statements and the
   * maximum data size in bytes.
   *
   * @param cluster The {@linkplain CqlSession session} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public ReactorStatementBatcher(
      @NonNull CqlSession cluster,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements,
      long maxSizeInBytes) {
    super(cluster, batchMode, batchType, maxBatchStatements, maxSizeInBytes);
  }

  /**
   * Batches together the given statements into groups of statements having the same grouping key.
   * Each group size is capped by the maximum number of statements and the maximum data size.
   *
   * <p>The grouping key to use is determined by the {@linkplain
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
   * PARTITION_KEY} is used, the grouping key is the statement's {@linkplain
   * Statement#getRoutingKey() routing key} or {@linkplain Statement#getRoutingToken() routing
   * token}, whichever is available.
   *
   * <p>When {@link com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode#REPLICA_SET
   * REPLICA_SET} is used, the grouping key is the replica set owning the statement's {@linkplain
   * Statement#getRoutingKey() routing key} or {@linkplain Statement#getRoutingToken() routing
   * token}, whichever is available.
   *
   * @param statements the statements to batch together.
   * @return A {@link Flux} of batched statements.
   */
  @NonNull
  public Flux<Statement<?>> batchByGroupingKey(
      @NonNull Publisher<BatchableStatement<?>> statements) {
    return Flux.from(statements).groupBy(this::groupingKey).flatMap(this::batchAll);
  }

  /**
   * Batches together all the given statements into groups of statements, <em>regardless of their
   * grouping key</em>. Each group size is capped by the maximum number of statements and the
   * maximum data size.
   *
   * <p>Note that when a resulting group contains only one statement, this method will not create a
   * batch statement containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of statements for the same grouping key is greater than the maximum number
   * of statements, or when their total data size is greater than the maximum data size, statements
   * will be split into smaller batches.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@linkplain
   * Statement#getRoutingKey() routing key}, the resulting batch could lead to write throughput
   * degradation.
   *
   * @param statements the statements to batch together.
   * @return A {@link Flux} of batched statements.
   */
  @NonNull
  public Flux<Statement<?>> batchAll(@NonNull Publisher<BatchableStatement<?>> statements) {
    return Flux.<Statement<?>>from(statements)
        .windowUntil(new ReactorAdaptiveSizingBatchPredicate(), false)
        .flatMap(
            stmts ->
                stmts.reduce(
                    (s1, s2) -> {
                      if (s1 instanceof BatchStatement) {
                        return ((BatchStatement) s1).add((BatchableStatement<?>) s2);
                      } else {
                        return BatchStatement.newInstance(
                            batchType, (BatchableStatement<?>) s1, (BatchableStatement<?>) s2);
                      }
                    }));
  }

  private class ReactorAdaptiveSizingBatchPredicate extends AdaptiveSizingBatchPredicate {}
}
