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
import com.datastax.oss.dsbulk.executor.api.BulkExecutorBuilder;
import com.datastax.oss.dsbulk.executor.api.BulkExecutorBuilderFactory;
import edu.umd.cs.findbugs.annotations.NonNull;

public class ReactorBulkExecutorBuilderFactory implements BulkExecutorBuilderFactory {

  @NonNull
  public BulkExecutorBuilder<? extends ReactorBulkExecutor> create(
      @NonNull CqlSession session, boolean useContinuousPagingForReads) {
    return useContinuousPagingForReads
        ? ContinuousReactorBulkExecutor.continuousPagingBuilder(session)
        : DefaultReactorBulkExecutor.builder(session);
  }
}
