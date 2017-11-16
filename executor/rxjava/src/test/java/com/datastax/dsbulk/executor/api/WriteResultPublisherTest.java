/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import static com.datastax.dsbulk.executor.api.AbstractReadResultPublisherTest.setUpFailedSession;

import com.datastax.dsbulk.executor.api.result.WriteResult;
import org.reactivestreams.Publisher;

public class WriteResultPublisherTest extends AbstractWriteResultPublisherTest {

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
