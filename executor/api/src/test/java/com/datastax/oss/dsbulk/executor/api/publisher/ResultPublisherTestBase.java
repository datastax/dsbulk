/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.api.publisher;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.oss.dsbulk.executor.api.result.Result;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public abstract class ResultPublisherTestBase<T extends Result> extends PublisherVerification<T> {

  static final ExecutionListener FAILED_LISTENER =
      new ExecutionListener() {
        // we need something that fails right away, inside the subscribe() method,
        // and that does not leave us with many choices.
        @Override
        public void onExecutionStarted(Statement<?> statement, ExecutionContext context) {
          throw new IllegalArgumentException("irrelevant");
        }
      };

  ResultPublisherTestBase() {
    super(new TestEnvironment());
  }
}
