/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava;

import com.datastax.dsbulk.executor.api.ReadResultPublisherTestBase;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import org.reactivestreams.Publisher;

public class ReadResultPublisherTest extends ReadResultPublisherTestBase {

  @Override
  public Publisher<ReadResult> createPublisher(long elements) {
    DefaultRxJavaBulkExecutor executor =
        new DefaultRxJavaBulkExecutor(setUpSuccessfulSession(elements));
    return executor.readReactive("irrelevant");
  }

  @Override
  public Publisher<ReadResult> createFailedPublisher() {
    DefaultRxJavaBulkExecutor executor = new DefaultRxJavaBulkExecutor(setUpFailedSession());
    return executor.readReactive("irrelevant");
  }
}
