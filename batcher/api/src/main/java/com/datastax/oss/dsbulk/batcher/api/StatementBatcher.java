/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.batcher.api;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A component that groups statements together in batches sharing the same {@linkplain
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
public interface StatementBatcher {

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
  default List<Statement<?>> batchByGroupingKey(@NonNull BatchableStatement<?>... statements) {
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
  List<Statement<?>> batchByGroupingKey(@NonNull Iterable<BatchableStatement<?>> statements);

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
  default List<Statement<?>> batchAll(@NonNull BatchableStatement<?>... statements) {
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
  List<Statement<?>> batchAll(@NonNull Collection<BatchableStatement<?>> statements);
}
