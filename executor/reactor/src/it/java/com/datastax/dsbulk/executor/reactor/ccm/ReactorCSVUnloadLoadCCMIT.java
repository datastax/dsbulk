/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.reactor.ccm;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.ccm.CSVUnloadLoadCCMITBase;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import org.junit.jupiter.api.Tag;

@Tag("ccm")
public class ReactorCSVUnloadLoadCCMIT extends CSVUnloadLoadCCMITBase {

  ReactorCSVUnloadLoadCCMIT(Session session) {
    super(session);
  }

  @Override
  protected BulkExecutor getBulkExecutor(ExecutionListener listener, Session session) {
    return DefaultReactorBulkExecutor.builder(session).withExecutionListener(listener).build();
  }
}
