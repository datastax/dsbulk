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
import com.datastax.oss.driver.api.core.CqlSession;

/** A builder for {@link DefaultRxJavaBulkExecutor} instances. */
public class DefaultRxJavaBulkExecutorBuilder
    extends AbstractBulkExecutorBuilder<DefaultRxJavaBulkExecutor> {

  DefaultRxJavaBulkExecutorBuilder(CqlSession session) {
    super(session);
  }

  @Override
  public DefaultRxJavaBulkExecutor build() {
    return new DefaultRxJavaBulkExecutor(this);
  }
}
