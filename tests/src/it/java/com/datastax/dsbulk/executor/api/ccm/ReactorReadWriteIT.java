/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.ccm;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.datastax.dsbulk.tests.categories.LongTests;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import org.junit.experimental.categories.Category;

@CCMTest
@Category(LongTests.class)
public class ReactorReadWriteIT extends AbstractReadWriteIT {

  @Override
  protected BulkExecutor getBulkExecutor(ExecutionListener listener, Session session) {
    return DefaultReactorBulkExecutor.builder(session).withExecutionListener(listener).build();
  }
}
