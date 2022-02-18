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
package com.datastax.oss.dsbulk.executor.reactor.simulacron;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.executor.api.simulacron.BulkExecutorSimulacronITBase;
import com.datastax.oss.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.datastax.oss.dsbulk.tests.driver.SerializedSession;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import com.datastax.oss.simulacron.server.BoundCluster;

class DefaultReactorBulkExecutorSimulacronIT extends BulkExecutorSimulacronITBase {

  DefaultReactorBulkExecutorSimulacronIT(
      BoundCluster simulacron,
      @SessionConfig(settings = "advanced.protocol.version=V4") CqlSession session) {
    super(
        simulacron,
        DefaultReactorBulkExecutor.builder(new SerializedSession(session)).build(),
        DefaultReactorBulkExecutor.builder(new SerializedSession(session)).failSafe().build());
  }
}
