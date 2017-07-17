/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api.statement;

import com.datastax.driver.core.SimpleStatement;
import java.util.Map;

/** */
public class BulkSimpleStatement<T> extends SimpleStatement implements BulkStatement<T> {

  private final T source;

  public BulkSimpleStatement(T source, String query) {
    super(query);
    this.source = source;
  }

  public BulkSimpleStatement(T source, String query, Object... values) {
    super(query, values);
    this.source = source;
  }

  public BulkSimpleStatement(T source, String query, Map<String, Object> values) {
    super(query, values);
    this.source = source;
  }

  @Override
  public T getSource() {
    return source;
  }
}
