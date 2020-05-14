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
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractBulkExecutorBuilder<T extends BulkExecutor>
    implements BulkExecutorBuilder<T> {

  protected final CqlSession session;

  protected boolean failFast = true;

  protected int maxInFlightRequests = AbstractBulkExecutor.DEFAULT_MAX_IN_FLIGHT_REQUESTS;

  protected int maxRequestsPerSecond = AbstractBulkExecutor.DEFAULT_MAX_REQUESTS_PER_SECOND;

  protected ExecutionListener listener;

  protected AbstractBulkExecutorBuilder(CqlSession session) {
    this.session = session;
  }

  @Override
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> failSafe() {
    this.failFast = false;
    return this;
  }

  @Override
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withMaxInFlightRequests(int maxInFlightRequests) {
    this.maxInFlightRequests = maxInFlightRequests;
    return this;
  }

  @Override
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withMaxRequestsPerSecond(int maxRequestsPerSecond) {
    this.maxRequestsPerSecond = maxRequestsPerSecond;
    return this;
  }

  @Override
  @SuppressWarnings("UnusedReturnValue")
  public AbstractBulkExecutorBuilder<T> withExecutionListener(ExecutionListener listener) {
    this.listener = listener;
    return this;
  }
}
