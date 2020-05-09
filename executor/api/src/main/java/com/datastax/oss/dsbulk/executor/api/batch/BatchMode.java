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
package com.datastax.oss.dsbulk.executor.api.batch;

import com.datastax.oss.driver.api.core.cql.Statement;

/** Available batch modes. */
public enum BatchMode {

  /**
   * Groups together statements that share the same partition key. This is the default mode, and the
   * preferred one.
   *
   * <p>Under the hood, this mode uses either the statement's {@linkplain Statement#getRoutingKey()
   * routing key} or {@linkplain Statement#getRoutingToken() routing token}, whichever is available,
   * starting with the routing token.
   */
  PARTITION_KEY,

  /**
   * Groups together statements that share the same replica set. This mode might yield better
   * results for small clusters and lower replication factors, but tends to perform equally well or
   * even worse than {@link #PARTITION_KEY} for larger clusters or high replication factors (i.e. RF
   * &gt; 3).
   *
   * <p>Note that this mode can only work if the statements to batch have their {@linkplain
   * Statement#getKeyspace() routing keyspace} set. If this condition is not met, the batcher will
   * silently fall back to {@code PARTITION_KEY} mode.
   */
  REPLICA_SET
}
