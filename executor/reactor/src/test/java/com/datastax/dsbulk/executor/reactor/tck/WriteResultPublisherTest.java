/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor.tck;

import static com.datastax.dsbulk.executor.api.tck.ReadResultPublisherTestBase.setUpFailedSession;

import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.dsbulk.executor.api.tck.WriteResultPublisherTestBase;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import org.reactivestreams.Publisher;

public class WriteResultPublisherTest extends WriteResultPublisherTestBase {

  @Override
  public Publisher<WriteResult> createPublisher(long elements) {
    DefaultReactorBulkExecutor executor = new DefaultReactorBulkExecutor(setUpSuccessfulSession());
    return executor.writeReactive("irrelevant");
  }

  @Override
  public Publisher<WriteResult> createFailedPublisher() {
    DefaultReactorBulkExecutor executor = new DefaultReactorBulkExecutor(setUpFailedSession());
    return executor.writeReactive("irrelevant");
  }
}
