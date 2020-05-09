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
package com.datastax.oss.dsbulk.executor.api;

import com.datastax.oss.driver.api.core.CqlSession;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A factory for {@link BulkExecutorBuilder} instances. */
public interface BulkExecutorBuilderFactory {

  /**
   * Creates a new {@link BulkExecutorBuilder} for the given session.
   *
   * @param session The session to create an executor for.
   * @param useContinuousPagingForReads whether the builder should create executors using continuous
   *     paging for reads.
   * @return A newly-allocated bulk executor builder.
   */
  @NonNull
  BulkExecutorBuilder<? extends BulkExecutor> create(
      @NonNull CqlSession session, boolean useContinuousPagingForReads);
}
