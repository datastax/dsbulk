/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.statement;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import java.util.List;
import java.util.Map;

public class BulkSimpleStatement<T> extends WrappedStatement<SimpleStatement>
    implements SimpleStatement, BulkStatement<T> {

  private final T source;

  public BulkSimpleStatement(T source, String query) {
    super(new SimpleStatementBuilder(query).build());
    this.source = source;
  }

  public BulkSimpleStatement(T source, String query, Object... values) {
    super(new SimpleStatementBuilder(query).addPositionalValues(values).build());
    this.source = source;
  }

  @Override
  public T getSource() {
    return source;
  }

  @Override
  public String getQuery() {
    return wrapped.getQuery();
  }

  @Override
  public SimpleStatement setQuery(String newQuery) {
    return wrapped.setQuery(newQuery);
  }

  @Override
  public SimpleStatement setKeyspace(CqlIdentifier newKeyspace) {
    return wrapped.setKeyspace(newKeyspace);
  }

  @Override
  public List<Object> getPositionalValues() {
    return wrapped.getPositionalValues();
  }

  @Override
  public SimpleStatement setPositionalValues(List<Object> newPositionalValues) {
    return wrapped.setPositionalValues(newPositionalValues);
  }

  @Override
  public Map<CqlIdentifier, Object> getNamedValues() {
    return wrapped.getNamedValues();
  }

  @Override
  public SimpleStatement setNamedValuesWithIds(Map<CqlIdentifier, Object> newNamedValues) {
    return wrapped.setNamedValuesWithIds(newNamedValues);
  }
}
