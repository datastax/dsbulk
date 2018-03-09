/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.statement;

import com.datastax.driver.core.SimpleStatement;
import java.util.Map;

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
