/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.rxjava.DefaultRxJavaBulkExecutor;
import org.junit.jupiter.api.Tag;

@Tag("ccm")
public class RxJavaCSVUnloadLoadCCMIT extends CSVUnloadLoadCCMITBase {

  RxJavaCSVUnloadLoadCCMIT(Session session) {
    super(session);
  }

  @Override
  protected BulkExecutor getBulkExecutor(ExecutionListener listener, Session session) {
    return DefaultRxJavaBulkExecutor.builder(session).withExecutionListener(listener).build();
  }
}
