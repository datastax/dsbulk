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
package com.datastax.oss.dsbulk.executor.api.publisher;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.oss.dsbulk.executor.api.result.Result;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public abstract class ResultPublisherTestBase<T extends Result> extends PublisherVerification<T> {

  static final ExecutionListener FAILED_LISTENER =
      new ExecutionListener() {
        // we need something that fails right away, inside the subscribe() method,
        // and that does not leave us with many choices.
        @Override
        public void onExecutionStarted(Statement<?> statement, ExecutionContext context) {
          throw new IllegalArgumentException("irrelevant");
        }
      };

  ResultPublisherTestBase() {
    super(new TestEnvironment());
  }
}
