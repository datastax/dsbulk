/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor.tck;

import static org.mockito.Mockito.mock;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.tck.ReadResultPublisherTestBase;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import org.reactivestreams.Publisher;

public class ReadResultPublisherTest extends ReadResultPublisherTestBase {

  @Override
  public Publisher<ReadResult> createPublisher(long elements) {
    DefaultReactorBulkExecutor executor =
        new DefaultReactorBulkExecutor(setUpSuccessfulSession(elements));
    return executor.readReactive("irrelevant");
  }

  @Override
  public Publisher<ReadResult> createFailedPublisher() {
    DefaultReactorBulkExecutor executor =
        DefaultReactorBulkExecutor.builder(mock(Session.class))
            .withExecutionListener(FAILED_LISTENER)
            .build();
    return executor.readReactive("irrelevant");
  }
}
