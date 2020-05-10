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
package com.datastax.oss.dsbulk.executor.reactor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.BulkExecutor;
import com.datastax.oss.dsbulk.executor.api.publisher.ContinuousReadResultPublisher;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import java.util.Objects;
import reactor.core.publisher.Flux;

/**
 * An implementation of {@link BulkExecutor} using <a href="https://projectreactor.io">Reactor</a>,
 * that executes all reads using continuous paging. This executor can achieve significant
 * performance improvements for reads, provided that the read statements to be executed can be
 * properly routed to a replica.
 */
public class ContinuousReactorBulkExecutor extends DefaultReactorBulkExecutor
    implements ReactorBulkExecutor {

  /**
   * Creates a new builder for {@link ContinuousReactorBulkExecutor} instances using the given
   * {@link CqlSession}.
   *
   * @param session the {@link CqlSession} to use.
   * @return a new builder.
   */
  public static ContinuousReactorBulkExecutorBuilder continuousPagingBuilder(CqlSession session) {
    return new ContinuousReactorBulkExecutorBuilder(session);
  }

  private final CqlSession cqlSession;

  /**
   * Creates a new instance using the given {@link CqlSession} and using defaults for all
   * parameters.
   *
   * <p>If you need to customize your executor, use the {@link #builder(CqlSession) builder} method
   * instead.
   *
   * @param cqlSession the {@link CqlSession} to use.
   */
  public ContinuousReactorBulkExecutor(CqlSession cqlSession) {
    super(cqlSession);
    this.cqlSession = cqlSession;
  }

  ContinuousReactorBulkExecutor(ContinuousReactorBulkExecutorBuilder builder) {
    super(builder);
    this.cqlSession = builder.cqlSession;
  }

  @Override
  public Flux<ReadResult> readReactive(Statement<?> statement) {
    Objects.requireNonNull(statement);
    return Flux.from(
        new ContinuousReadResultPublisher(
            statement, cqlSession, failFast, listener, maxConcurrentRequests, rateLimiter));
  }
}
