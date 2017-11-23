/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava;

import com.datastax.dsbulk.executor.api.AbstractContinuousReadResultPublisherTest;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import org.reactivestreams.Publisher;

public class ContinuousReadResultPublisherTest extends AbstractContinuousReadResultPublisherTest {

  @Override
  public Publisher<ReadResult> createPublisher(long elements) {
    ContinuousRxJavaBulkExecutor executor =
        new ContinuousRxJavaBulkExecutor(setUpSuccessfulSession(elements));
    return executor.readReactive("irrelevant");
  }

  @Override
  public Publisher<ReadResult> createFailedPublisher() {
    ContinuousRxJavaBulkExecutor executor = new ContinuousRxJavaBulkExecutor(setUpFailedSession());
    return executor.readReactive("irrelevant");
  }
}
