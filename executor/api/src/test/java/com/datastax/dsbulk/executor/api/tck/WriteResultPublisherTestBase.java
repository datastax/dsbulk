/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.tck;

import com.datastax.dsbulk.executor.api.result.WriteResult;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public abstract class WriteResultPublisherTestBase extends PublisherVerification<WriteResult> {

  public WriteResultPublisherTestBase() {
    super(new TestEnvironment());
  }

  @Override
  public long maxElementsFromPublisher() {
    return 1;
  }
}
