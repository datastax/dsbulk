/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.batch;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import com.datastax.dsbulk.commons.internal.utils.StatementUtils;
import com.google.common.base.Preconditions;
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
import org.jetbrains.annotations.NotNull;

/**
 * A helper class to facilitate grouping statements together in batches sharing the same {@linkplain
 * BatchMode#PARTITION_KEY partition key} or {@linkplain BatchMode#REPLICA_SET replica set}.
 *
 * <p>This component can also optionally cap the created batches by either a maximum number of child
 * statements, a maximum size in bytes of actual data to be inserted, or both. Note however that the
 * heuristic used to compute the inserted data size is not 100% accurate.
 *
 * <p>Important: for this utility to work properly, statements must have their {@linkplain
 * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or their {@linkplain
 * Statement#getRoutingToken() routing token} set. Furthermore, when grouping by {@link
 * BatchMode#REPLICA_SET replica set}, it is also required that they have their {@link
 * Statement#getKeyspace() routing keyspace} set.
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
     * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or {@linkplain
     * Statement#getRoutingToken() routing token}, whichever is available, starting with the routing
     * token.
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

  protected final Cluster cluster;
  protected final BatchMode batchMode;
  protected final BatchStatement.Type batchType;
  protected final ProtocolVersion protocolVersion;
  protected final CodecRegistry codecRegistry;
  protected final int maxBatchStatements;
  protected final long maxSizeInBytes;

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in
   * {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses the {@linkplain
   * ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and the default {@link
   * CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It also uses the default {@linkplain
   * #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and the default {@linkplain
   * #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   */
  protected StatementBatcher() {
    this.cluster = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = BatchStatement.Type.UNLOGGED;
    this.protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    this.codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    this.maxBatchStatements = DEFAULT_MAX_BATCH_STATEMENTS;
    this.maxSizeInBytes = DEFAULT_MAX_SIZE_BYTES;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in
   * {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses the {@linkplain
   * ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version, the default {@link
   * CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance and the default {@linkplain
   * #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited). It uses the given maximum
   * number of statements.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public StatementBatcher(int maxBatchStatements) {
    this.cluster = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = BatchStatement.Type.UNLOGGED;
    this.protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    this.codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = DEFAULT_MAX_SIZE_BYTES;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in
   * {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses the {@linkplain
   * ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version, the default {@link
   * CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance and the default {@linkplain
   * #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100). It uses the given maximum
   * data size in bytes.
   *
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public StatementBatcher(long maxSizeInBytes) {
    this.cluster = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = BatchStatement.Type.UNLOGGED;
    this.protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    this.codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    this.maxBatchStatements = DEFAULT_MAX_BATCH_STATEMENTS;
    this.maxSizeInBytes = maxSizeInBytes;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in
   * {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses the {@linkplain
   * ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and the default {@link
   * CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It uses the given maximum number of
   * statements and the maximum data size in bytes.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public StatementBatcher(int maxBatchStatements, long maxSizeInBytes) {
    this.cluster = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = BatchStatement.Type.UNLOGGED;
    this.protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    this.codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = maxSizeInBytes;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in
   * {@linkplain BatchMode#PARTITION_KEY partition key} mode and uses the given {@link Cluster} as
   * its source for the {@linkplain ProtocolVersion protocol version} and the {@link CodecRegistry}
   * instance to use. It also uses the default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum
   * number of statements} (100) and the default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data
   * size in bytes} (unlimited).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   */
  public StatementBatcher(@NotNull Cluster cluster) {
    this(cluster, BatchMode.PARTITION_KEY);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@linkplain
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in the
   * specified {@linkplain BatchMode batch mode} and uses the given {@link Cluster} as its source
   * for the {@linkplain ProtocolVersion protocol version} and the {@link CodecRegistry} instance to
   * use. It also uses the default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of
   * statements} (100) and the default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in
   * bytes} (unlimited).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   */
  public StatementBatcher(@NotNull Cluster cluster, @NotNull BatchMode batchMode) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, DEFAULT_MAX_BATCH_STATEMENTS);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * operates in the specified {@code batchMode} and uses the given {@link Cluster} as its source
   * for the {@linkplain ProtocolVersion protocol version} and the {@link CodecRegistry} instance to
   * use. It uses the given maximum number of statements and the default {@linkplain
   * #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public StatementBatcher(
      @NotNull Cluster cluster,
      @NotNull BatchMode batchMode,
      @NotNull BatchStatement.Type batchType,
      int maxBatchStatements) {
    this.cluster = Objects.requireNonNull(cluster);
    this.batchMode = Objects.requireNonNull(batchMode);
    this.batchType = Objects.requireNonNull(batchType);
    this.protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    this.codecRegistry = cluster.getConfiguration().getCodecRegistry();
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = DEFAULT_MAX_SIZE_BYTES;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * operates in the specified {@code batchMode} and uses the given {@link Cluster} as its source
   * for the {@linkplain ProtocolVersion protocol version} and the {@link CodecRegistry} instance to
   * use. It uses the given maximum number of statements and the maximum data size in bytes.
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public StatementBatcher(
      @NotNull Cluster cluster,
      @NotNull BatchMode batchMode,
      @NotNull BatchStatement.Type batchType,
      int maxBatchStatements,
      long maxSizeInBytes) {
    this.cluster = Objects.requireNonNull(cluster);
    this.batchMode = Objects.requireNonNull(batchMode);
    this.batchType = Objects.requireNonNull(batchType);
    this.protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    this.codecRegistry = cluster.getConfiguration().getCodecRegistry();
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
   * statement's {@linkplain Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or
   * {@linkplain Statement#getRoutingToken() routing token}, whichever is available.
   *
   * <p>When {@link BatchMode#REPLICA_SET REPLICA_SET} is used, the grouping key is the replica set
   * owning the statement's {@linkplain Statement#getRoutingKey(ProtocolVersion, CodecRegistry)
   * routing key} or {@linkplain Statement#getRoutingToken() routing token}, whichever is available.
   *
   * @param statements the statements to batch together.
   * @return A list of batched statements.
   */
  @NotNull
  public List<Statement> batchByGroupingKey(@NotNull Statement... statements) {
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
   * statement's {@linkplain Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or
   * {@linkplain Statement#getRoutingToken() routing token}, whichever is available.
   *
   * <p>When {@link BatchMode#REPLICA_SET REPLICA_SET} is used, the grouping key is the replica set
   * owning the statement's {@linkplain Statement#getRoutingKey(ProtocolVersion, CodecRegistry)
   * routing key} or {@linkplain Statement#getRoutingToken() routing token}, whichever is available.
   *
   * @param statements the statements to batch together.
   * @return A list of batched statements.
   */
  @NotNull
  public List<Statement> batchByGroupingKey(@NotNull Iterable<? extends Statement> statements) {
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
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}, the resulting batch could
   * lead to write throughput degradation.
   *
   * @param statements the statements to batch together.
   * @return A list of {@link BatchStatement}s containing all the given statements batched together,
   *     or the original statement, if only one was provided.
   */
  @NotNull
  public List<Statement> batchAll(@NotNull Statement... statements) {
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
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}, the resulting batch could
   * lead to write throughput degradation.
   *
   * @param statements the statements to batch together.
   * @return A list of {@link BatchStatement}s containing all the given statements batched together,
   *     or the original statement, if only one was provided.
   */
  @NotNull
  public List<Statement> batchAll(@NotNull Collection<? extends Statement> statements) {
    return maybeBatch(statements);
  }

  @NotNull
  private List<Statement> maybeBatch(@NotNull Collection<? extends Statement> stmts) {
    Objects.requireNonNull(stmts);
    Preconditions.checkArgument(!stmts.isEmpty());
    // Don't wrap single statements in batch.
    if (stmts.size() == 1) {
      return Collections.singletonList(stmts.iterator().next());
    } else {
      List<Statement> batches = new ArrayList<>();
      BatchStatement current = new BatchStatement(batchType);
      AdaptiveSizingBatchPredicate shouldFlush = new AdaptiveSizingBatchPredicate();
      for (Iterator<? extends Statement> it = stmts.iterator(); it.hasNext(); ) {
        Statement stmt = it.next();
        current.add(stmt);
        if (shouldFlush.test(stmt)) {
          flush(current, batches);
          if (it.hasNext()) {
            current = new BatchStatement(batchType);
          }
        }
      }
      if (current.size() > 0) {
        flush(current, batches);
      }
      return batches;
    }
  }

  private void flush(BatchStatement current, List<Statement> batches) {
    if (current.size() == 1) {
      batches.add(current.getStatements().iterator().next());
    } else {
      batches.add(current);
    }
  }

  @NotNull
  protected Object groupingKey(@NotNull Statement statement) {
    Token routingToken = statement.getRoutingToken();
    ByteBuffer routingKey = statement.getRoutingKey(protocolVersion, codecRegistry);
    switch (batchMode) {
      case REPLICA_SET:
        String keyspace = statement.getKeyspace();
        if (keyspace != null) {
          Set<Host> replicas = null;
          if (routingKey != null) {
            replicas = cluster.getMetadata().getReplicas(keyspace, routingKey);
          } else if (routingToken != null) {
            replicas = cluster.getMetadata().getReplicas(keyspace, routingToken);
          }
          if (replicas != null && !replicas.isEmpty()) {
            return replicas.hashCode();
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

  protected class AdaptiveSizingBatchPredicate implements Predicate<Statement> {

    private int statementsCounter = 0;
    private long bytesInCurrentBatch = 0;

    @Override
    public boolean test(@NotNull Statement statement) {
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

    long calculateSize(@NotNull Statement statement) {
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
