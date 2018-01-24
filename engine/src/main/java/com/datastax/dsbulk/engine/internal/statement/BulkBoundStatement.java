/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.statement;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

/** */
public class BulkBoundStatement<T> extends BoundStatement implements BulkStatement<T> {

  private final T source;

  public BulkBoundStatement(T source, PreparedStatement statement) {
    super(statement);
    this.source = source;
  }

  @Override
  public T getSource() {
    return source;
  }
}
