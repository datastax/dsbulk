/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.api.batch;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.dsbulk.commons.utils.StatementUtils;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A helper class to facilitate grouping statements together in batches sharing the same {@linkplain
 * BatchMode#PARTITION_KEY partition key} or {@linkplain BatchMode#REPLICA_SET replica set}.
 *
 * <p>This component can also optionally cap the created batches by either a maximum number of child
 * statements, a maximum size in bytes of actual data to be inserted, or both. Note however that the
 * heuristic used to compute the inserted data size is not 100% accurate.
 *
 * <p>Important: for this utility to work properly, statements must have their {@linkplain
 * Statement#getRoutingKey() routing key} or their {@linkplain Statement#getRoutingToken() routing
 * token} set. Furthermore, when grouping by {@link BatchMode#REPLICA_SET replica set}, it is also
 * required that they have their {@link Statement#getKeyspace() routing keyspace} set.
 *
 * @see <a href="http://docs.datastax.com/en/cql/3.1/cql/cql_using/useBatch.html">Using and misusing
 *     batches</a>
 */
public class StatementBatcher {

  /** Available batch modes. */
  public enum BatchMode {

    /**
     * Groups together statements that share the same partition key. This is the default mode, and
     * the preferred one.
     *
     * <p>Under the hood, this mode uses either the statement's {@linkplain
     * Statement#getRoutingKey() routing key} or {@linkplain Statement#getRoutingToken() routing
     * token}, whichever is available, starting with the routing token.
     */
    PARTITION_KEY,

    /**
     * Groups together statements that share the same replica set. This mode might yield better
     * results for small clusters and lower replication factors, but tends to perform equally well
     * or even worse than {@link #PARTITION_KEY} for larger clusters or high replication factors
     * (i.e. RF &gt; 3).
     *
     * <p>Note that this mode can only work if the statements to batch have their {@linkplain
     * Statement#getKeyspace() routing keyspace} set. If this condition is not met, the batcher will
     * silently fall back to {@code PARTITION_KEY} mode.
     */
    REPLICA_SET
  }

  /** The default maximum number of statements that a batch can contain. */
  public static final int DEFAULT_MAX_BATCH_STATEMENTS = 100;

  /** The default maximum data size in bytes that a batch can contain (unlimited). */
  public static final long DEFAULT_MAX_SIZE_BYTES = -1;

  protected final CqlSession session;
  protected final BatchMode batchMode;
  protected final BatchType batchType;
  protected final ProtocolVersion protocolVersion;
  protected final CodecRegistry codecRegistry;
  protected final int maxBatchStatements;
  protected final long maxSizeInBytes;

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain DefaultBatchType#UNLOGGED
   * unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses
   * the default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and
   * the default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   */
  protected StatementBatcher() {
    this.session = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = DefaultBatchType.UNLOGGED;
    this.protocolVersion = ProtocolVersion.DEFAULT;
    this.codecRegistry = CodecRegistry.DEFAULT;
    this.maxBatchStatements = DEFAULT_MAX_BATCH_STATEMENTS;
    this.maxSizeInBytes = DEFAULT_MAX_SIZE_BYTES;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain DefaultBatchType#UNLOGGED
   * unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses
   * the default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited). It
   * uses the given maximum number of statements.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public StatementBatcher(int maxBatchStatements) {
    this.session = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = DefaultBatchType.UNLOGGED;
    this.protocolVersion = ProtocolVersion.DEFAULT;
    this.codecRegistry = CodecRegistry.DEFAULT;
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = DEFAULT_MAX_SIZE_BYTES;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain DefaultBatchType#UNLOGGED
   * unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses
   * the default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100). It
   * uses the given maximum data size in bytes.
   *
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public StatementBatcher(long maxSizeInBytes) {
    this.session = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = DefaultBatchType.UNLOGGED;
    this.protocolVersion = ProtocolVersion.DEFAULT;
    this.codecRegistry = CodecRegistry.DEFAULT;
    this.maxBatchStatements = DEFAULT_MAX_BATCH_STATEMENTS;
    this.maxSizeInBytes = maxSizeInBytes;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain DefaultBatchType#UNLOGGED
   * unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses
   * the given maximum number of statements and the maximum data size in bytes.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public StatementBatcher(int maxBatchStatements, long maxSizeInBytes) {
    this.session = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = DefaultBatchType.UNLOGGED;
    this.protocolVersion = ProtocolVersion.DEFAULT;
    this.codecRegistry = CodecRegistry.DEFAULT;
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = maxSizeInBytes;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain DefaultBatchType#UNLOGGED
   * unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses
   * the default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and
   * the default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null} if {@code
   *     batchMode} is {@link BatchMode#REPLICA_SET REPLICA_SET}.
   */
  public StatementBatcher(@Nullable CqlSession session) {
    this(session, BatchMode.PARTITION_KEY);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain DefaultBatchType#UNLOGGED
   * unlogged} batches, operates in the specified {@linkplain BatchMode batch mode} and uses the
   * given {@linkplain CqlSession session} as its source for the {@linkplain ProtocolVersion
   * protocol version} and the {@link CodecRegistry} instance to use. It also uses the default
   * {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and the default
   * {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null} if {@code
   *     batchMode} is {@link BatchMode#REPLICA_SET REPLICA_SET}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   */
  public StatementBatcher(@Nullable CqlSession session, @NonNull BatchMode batchMode) {
    this(session, batchMode, DefaultBatchType.UNLOGGED, DEFAULT_MAX_BATCH_STATEMENTS);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * operates in the specified {@code batchMode} and uses the given {@linkplain CqlSession session}
   * as its source for the {@linkplain ProtocolVersion protocol version} and the {@link
   * CodecRegistry} instance to use. It also uses uses the given maximum number of statements and
   * the default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null} if {@code
   *     batchMode} is {@link BatchMode#REPLICA_SET REPLICA_SET}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public StatementBatcher(
      @Nullable CqlSession session,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements) {
    this(session, batchMode, batchType, maxBatchStatements, DEFAULT_MAX_SIZE_BYTES);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * operates in the specified {@code batchMode} and uses the given {@linkplain CqlSession session}
   * as its source for the {@linkplain ProtocolVersion protocol version} and the {@link
   * CodecRegistry} instance to use. It uses the given maximum number of statements and the maximum
   * data size in bytes.
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null} if {@code
   *     batchMode} is {@link BatchMode#REPLICA_SET REPLICA_SET}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public StatementBatcher(
      @Nullable CqlSession session,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements,
      long maxSizeInBytes) {
    this.session = Objects.requireNonNull(session);
    this.batchMode = Objects.requireNonNull(batchMode);
    this.batchType = Objects.requireNonNull(batchType);
    this.protocolVersion = session.getContext().getProtocolVersion();
    this.codecRegistry = session.getContext().getCodecRegistry();
    if (maxBatchStatements < 0 && maxSizeInBytes < 0) {
      throw new IllegalArgumentException(
          "At least one of maxBatchStatements or maxSizeInBytes must be positive");
    }
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = maxSizeInBytes;
  }

  /**
   * Batches together the given statements into groups of statements having the same grouping key.
   * Each group size is capped by the maximum number of statements and the maximum data size.
   *
   * <p>The grouping key to use is determined by the {@linkplain BatchMode batch mode} in use by
   * this statement batcher.
   *
   * <p>Note that when a resulting group contains only one statement, this method will not create a
   * batch statement containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of statements for the same grouping key is greater than the maximum number
   * of statements, or when their total data size is greater than the maximum data size, statements
   * will be split into smaller batches.
   *
   * <p>When {@link BatchMode#PARTITION_KEY PARTITION_KEY} is used, the grouping key is the
   * statement's {@linkplain Statement#getRoutingKey() routing key} or {@linkplain
   * Statement#getRoutingToken() routing token}, whichever is available.
   *
   * <p>When {@link BatchMode#REPLICA_SET REPLICA_SET} is used, the grouping key is the replica set
   * owning the statement's {@linkplain Statement#getRoutingKey() routing key} or {@linkplain
   * Statement#getRoutingToken() routing token}, whichever is available.
   *
   * @param statements the statements to batch together.
   * @return A list of batched statements.
   */
  @NonNull
  public List<Statement<?>> batchByGroupingKey(@NonNull BatchableStatement<?>... statements) {
    return batchByGroupingKey(Arrays.asList(statements));
  }

  /**
   * Batches together the given statements into groups of statements having the same grouping key.
   * Each group size is capped by the maximum number of statements and the maximum data size.
   *
   * <p>The grouping key to use is determined by the {@linkplain BatchMode batch mode} in use by
   * this statement batcher.
   *
   * <p>Note that when a resulting group contains only one statement, this method will not create a
   * batch statement containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of statements for the same grouping key is greater than the maximum number
   * of statements, or when their total data size is greater than the maximum data size, statements
   * will be split into smaller batches.
   *
   * <p>When {@link BatchMode#PARTITION_KEY PARTITION_KEY} is used, the grouping key is the
   * statement's {@linkplain Statement#getRoutingKey() routing key} or {@linkplain
   * Statement#getRoutingToken() routing token}, whichever is available.
   *
   * <p>When {@link BatchMode#REPLICA_SET REPLICA_SET} is used, the grouping key is the replica set
   * owning the statement's {@linkplain Statement#getRoutingKey() routing key} or {@linkplain
   * Statement#getRoutingToken() routing token}, whichever is available.
   *
   * @param statements the statements to batch together.
   * @return A list of batched statements.
   */
  @NonNull
  public List<Statement<?>> batchByGroupingKey(
      @NonNull Iterable<BatchableStatement<?>> statements) {
    return StreamSupport.stream(statements.spliterator(), false)
        .collect(Collectors.groupingBy(this::groupingKey))
        .values()
        .stream()
        .flatMap(stmts -> maybeBatch(stmts).stream())
        .collect(Collectors.toList());
  }

  /**
   * Batches together all the given statements into one or more {@link BatchStatement}s,
   * <em>regardless of their grouping key</em>. Each group size is capped by the maximum number of
   * statements and the maximum data size.
   *
   * <p>Note that when a resulting group contains only one statement, this method will not create a
   * batch statement containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of statements is greater than the maximum number of statements, or when
   * their total data size is greater than the maximum data size, statements will be split into
   * smaller batches.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@linkplain
   * Statement#getRoutingKey() routing key}, the resulting batch could lead to write throughput
   * degradation.
   *
   * @param statements the statements to batch together.
   * @return A list of {@link BatchStatement}s containing all the given statements batched together,
   *     or the original statement, if only one was provided.
   */
  @NonNull
  public List<Statement<?>> batchAll(@NonNull BatchableStatement<?>... statements) {
    return batchAll(Arrays.asList(statements));
  }

  /**
   * Batches together all the given statements into one or more {@link BatchStatement}s,
   * <em>regardless of their grouping key</em>. Each group size is capped by the maximum number of
   * statements and the maximum data size.
   *
   * <p>Note that when a resulting group contains only one statement, this method will not create a
   * batch statement containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of statements is greater than the maximum number of statements, or when
   * their total data size is greater than the maximum data size, statements will be split into
   * smaller batches.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@linkplain
   * Statement#getRoutingKey() routing key}, the resulting batch could lead to write throughput
   * degradation.
   *
   * @param statements the statements to batch together.
   * @return A list of {@link BatchStatement}s containing all the given statements batched together,
   *     or the original statement, if only one was provided.
   */
  @NonNull
  public List<Statement<?>> batchAll(@NonNull Collection<BatchableStatement<?>> statements) {
    return maybeBatch(statements);
  }

  @NonNull
  private List<Statement<?>> maybeBatch(@NonNull Collection<BatchableStatement<?>> stmts) {
    Objects.requireNonNull(stmts);
    Preconditions.checkArgument(!stmts.isEmpty());
    // Don't wrap single statements in batch.
    if (stmts.size() == 1) {
      if (stmts instanceof List) {
        List<BatchableStatement<?>> list = (List<BatchableStatement<?>>) stmts;
        return Collections.singletonList(list.get(0));
      } else {
        return Collections.singletonList(stmts.iterator().next());
      }
    } else {
      ImmutableList.Builder<Statement<?>> batches = ImmutableList.builder();
      List<BatchableStatement<?>> current = new ArrayList<>();
      AdaptiveSizingBatchPredicate shouldFlush = new AdaptiveSizingBatchPredicate();
      for (Iterator<? extends BatchableStatement<?>> it = stmts.iterator(); it.hasNext(); ) {
        BatchableStatement<?> stmt = it.next();
        current.add(stmt);
        if (shouldFlush.test(stmt)) {
          flush(current, batches);
          if (it.hasNext()) {
            current.clear();
          }
        }
      }
      if (current.size() > 0) {
        flush(current, batches);
      }
      return batches.build();
    }
  }

  private void flush(
      List<BatchableStatement<?>> current, ImmutableList.Builder<Statement<?>> batches) {
    if (current.size() == 1) {
      batches.add(current.get(0));
    } else {
      batches.add(BatchStatement.newInstance(batchType, current));
    }
  }

  @NonNull
  protected Object groupingKey(@NonNull Statement<?> statement) {
    Token routingToken = statement.getRoutingToken();
    ByteBuffer routingKey = statement.getRoutingKey();
    switch (batchMode) {
      case REPLICA_SET:
        CqlIdentifier keyspace = getKeyspace(statement);
        if (keyspace != null) {
          TokenMap tokenMap = session.getMetadata().getTokenMap().orElse(null);
          if (tokenMap != null) {
            Set<Node> replicas = null;
            if (routingKey != null) {
              replicas = tokenMap.getReplicas(keyspace, routingKey);
            } else if (routingToken != null) {
              replicas = tokenMap.getReplicas(keyspace, routingToken);
            }
            if (replicas != null && !replicas.isEmpty()) {
              return replicas.hashCode();
            }
          }
        }
        // fall-through

      case PARTITION_KEY:
        if (routingToken != null) {
          return routingToken;
        } else if (routingKey != null && routingKey.hasRemaining()) {
          return routingKey;
        }
    }
    // If no grouping key can be discerned, return the statement itself so it
    // will stay unbatched.
    return statement;
  }

  @Nullable
  private CqlIdentifier getKeyspace(Statement<?> statement) {
    if (statement.getKeyspace() != null) {
      return statement.getKeyspace();
    }
    if (statement.getRoutingKeyspace() != null) {
      return statement.getRoutingKeyspace();
    }
    return session.getKeyspace().orElse(null);
  }

  protected class AdaptiveSizingBatchPredicate implements Predicate<BatchableStatement<?>> {

    private int statementsCounter = 0;
    private long bytesInCurrentBatch = 0;

    @Override
    public boolean test(@NonNull BatchableStatement<?> statement) {
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

    long calculateSize(@NonNull Statement<?> statement) {
      return StatementUtils.getDataSize(statement, protocolVersion, codecRegistry);
    }

    int getMaxBatchStatements() {
      if (maxBatchStatements <= 0) {
        return Integer.MAX_VALUE;
      }
      return maxBatchStatements;
    }

    long getMaxSizeInBytes() {
      if (maxSizeInBytes <= 0) {
        return Long.MAX_VALUE;
      }
      return maxSizeInBytes;
    }
  }
}
