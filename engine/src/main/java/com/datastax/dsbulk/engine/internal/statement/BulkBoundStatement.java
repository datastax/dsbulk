/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
