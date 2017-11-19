/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.dsbulk.executor.api.ContinuousReadResultPublisherTestBase;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import org.reactivestreams.Publisher;

public class ContinuousReadResultPublisherTest extends ContinuousReadResultPublisherTestBase {

  @Override
  public Publisher<ReadResult> createPublisher(long elements) {
    ContinuousReactorBulkExecutor executor =
        new ContinuousReactorBulkExecutor(setUpSuccessfulSession(elements));
    return executor.readReactive("irrelevant");
  }

  @Override
  public Publisher<ReadResult> createFailedPublisher() {
    ContinuousReactorBulkExecutor executor =
        new ContinuousReactorBulkExecutor(setUpFailedSession());
    return executor.readReactive("irrelevant");
  }
}
