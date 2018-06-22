/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava;

import com.datastax.dsbulk.executor.api.AbstractBulkExecutorBuilder;
import com.datastax.dse.driver.api.core.DseSession;

/** A builder for {@link ContinuousRxJavaBulkExecutor} instances. */
public class ContinuousRxJavaBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<ContinuousRxJavaBulkExecutor> {

  final DseSession dseSession;

  ContinuousRxJavaBulkExecutorBuilder(DseSession dseSession) {
    super(dseSession);
    this.dseSession = dseSession;
  }

  @Override
  public ContinuousRxJavaBulkExecutor build() {
    return new ContinuousRxJavaBulkExecutor(this);
  }
}
