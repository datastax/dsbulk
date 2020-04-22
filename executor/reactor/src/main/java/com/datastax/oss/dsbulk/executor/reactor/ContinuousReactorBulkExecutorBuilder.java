/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.reactor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.executor.api.AbstractBulkExecutorBuilder;

/** A builder for {@link ContinuousReactorBulkExecutor} instances. */
public class ContinuousReactorBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<ContinuousReactorBulkExecutor> {

  final CqlSession cqlSession;

  ContinuousReactorBulkExecutorBuilder(CqlSession cqlSession) {
    super(cqlSession);
    this.cqlSession = cqlSession;
  }

  @Override
  public ContinuousReactorBulkExecutor build() {
    return new ContinuousReactorBulkExecutor(this);
  }
}
