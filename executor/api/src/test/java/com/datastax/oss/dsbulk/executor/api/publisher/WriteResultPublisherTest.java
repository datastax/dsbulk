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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.dsbulk.tests.driver.MockAsyncResultSet;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;

public class WriteResultPublisherTest extends ResultPublisherTestBase<WriteResult> {

  @Override
  public long maxElementsFromPublisher() {
    return 1;
  }

  @Override
  public Publisher<WriteResult> createPublisher(long elements) {
    Statement<?> statement = SimpleStatement.newInstance("irrelevant");
    CqlSession session = setUpSession();
    return new WriteResultPublisher(statement, session, true);
  }

  @Override
  public Publisher<WriteResult> createFailedPublisher() {
    Statement<?> statement = SimpleStatement.newInstance("irrelevant");
    CqlSession session = mock(CqlSession.class);
    return new WriteResultPublisher(statement, session, true, FAILED_LISTENER, null, null);
  }

  private static CqlSession setUpSession() {
    CqlSession session = mock(CqlSession.class);
    CompletableFuture<AsyncResultSet> future = new CompletableFuture<>();
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    future.complete(new MockAsyncResultSet(0, executionInfo, null));
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(future);
    return session;
  }
}
