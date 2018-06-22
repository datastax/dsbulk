/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.oss.driver.api.core.CqlSession;

/** A builder for {@link DefaultReactorBulkExecutor} instances. */
public class DefaultReactorBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<DefaultReactorBulkExecutor> {

  DefaultReactorBulkExecutorBuilder(CqlSession session) {
    super(session);
  }

  @Override
  public DefaultReactorBulkExecutor build() {
    return new DefaultReactorBulkExecutor(this);
  }
}
