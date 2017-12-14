/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava.tck;

import static com.datastax.dsbulk.executor.api.tck.ReadResultPublisherTestBase.setUpFailedSession;

import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.dsbulk.executor.api.tck.WriteResultPublisherTestBase;
import com.datastax.dsbulk.executor.rxjava.DefaultRxJavaBulkExecutor;
import org.reactivestreams.Publisher;

public class WriteResultPublisherTest extends WriteResultPublisherTestBase {

  @Override
  public Publisher<WriteResult> createPublisher(long elements) {
    DefaultRxJavaBulkExecutor executor = new DefaultRxJavaBulkExecutor(setUpSuccessfulSession());
    return executor.writeReactive("irrelevant");
  }

  @Override
  public Publisher<WriteResult> createFailedPublisher() {
    DefaultRxJavaBulkExecutor executor = new DefaultRxJavaBulkExecutor(setUpFailedSession());
    return executor.writeReactive("irrelevant");
  }
}
