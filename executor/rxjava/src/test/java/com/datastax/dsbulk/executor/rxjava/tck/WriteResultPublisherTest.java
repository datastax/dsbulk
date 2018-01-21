/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava.tck;

import static com.datastax.dsbulk.executor.api.tck.ReadResultPublisherTestBase.FAILED_LISTENER;
import static com.datastax.dsbulk.executor.api.tck.ReadResultPublisherTestBase.setUpSuccessfulSession;
import static org.mockito.Mockito.mock;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.dsbulk.executor.api.tck.WriteResultPublisherTestBase;
import com.datastax.dsbulk.executor.rxjava.DefaultRxJavaBulkExecutor;
import org.reactivestreams.Publisher;

public class WriteResultPublisherTest extends WriteResultPublisherTestBase {

  @Override
  public Publisher<WriteResult> createPublisher(long elements) {
    DefaultRxJavaBulkExecutor executor = new DefaultRxJavaBulkExecutor(setUpSuccessfulSession(0));
    return executor.writeReactive("irrelevant");
  }

  @Override
  public Publisher<WriteResult> createFailedPublisher() {
    DefaultRxJavaBulkExecutor executor =
        DefaultRxJavaBulkExecutor.builder(mock(Session.class))
            .withExecutionListener(FAILED_LISTENER)
            .build();
    return executor.writeReactive("irrelevant");
  }
}
