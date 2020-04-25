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
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.tests.driver.MockAsyncResultSet;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class ReadResultPublisherTest extends ResultPublisherTestBase<ReadResult> {

  private static final int PAGE_SIZE = 5;

  @Override
  public Publisher<ReadResult> createPublisher(long elements) {
    Statement<?> statement = SimpleStatement.newInstance("irrelevant");
    CqlSession session = setUpSession(elements);
    return new ReadResultPublisher(statement, session, true);
  }

  @Override
  public Publisher<ReadResult> createFailedPublisher() {
    Statement<?> statement = SimpleStatement.newInstance("irrelevant");
    CqlSession session = setUpSession(1);
    return new ReadResultPublisher(statement, session, true, FAILED_LISTENER, null, null, null);
  }

  private static CqlSession setUpSession(long elements) {
    CqlSession session = mock(CqlSession.class);
    CompletionStage<AsyncResultSet> previous = mockPages(elements);
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(previous);
    return session;
  }

  private static CompletionStage<AsyncResultSet> mockPages(long elements) {
    // The TCK usually requests between 0 and 20 items, or Long.MAX_VALUE.
    // Past 3 elements it never checks how many elements have been effectively produced,
    // so we can safely cap at, say, 20.
    int effective = (int) Math.min(elements, 20L);
    CompletionStage<AsyncResultSet> previous = null;
    if (effective > 0) {
      // create pages of 5 elements each to exercise pagination
      List<Integer> pages =
          Flux.range(0, effective).buffer(PAGE_SIZE).map(List::size).collectList().block();
      assert pages != null;
      Collections.reverse(pages);
      for (Integer size : pages) {
        previous = mockPage(previous, size);
      }
    } else {
      previous = mockPage(null, 0);
    }
    return previous;
  }

  private static CompletionStage<AsyncResultSet> mockPage(
      CompletionStage<AsyncResultSet> previous, int size) {
    CompletableFuture<AsyncResultSet> future = new CompletableFuture<>();
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(executionInfo.getPagingState())
        .thenReturn(previous == null ? null : ByteBuffer.wrap(new byte[] {1}));
    future.complete(new MockAsyncResultSet(size, executionInfo, previous));
    previous = future;
    return previous;
  }
}
