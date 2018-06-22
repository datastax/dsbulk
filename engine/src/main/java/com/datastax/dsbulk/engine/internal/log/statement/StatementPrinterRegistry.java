/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.BulkSimpleStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.jctools.maps.NonBlockingHashMap;

/**
 * A registry for {@link StatementPrinter statement printers}.
 *
 * <p>This class is thread-safe.
 */
public final class StatementPrinterRegistry {

  private static final ImmutableMap<Class<?>, StatementPrinter<?>> BUILT_IN_PRINTERS =
      ImmutableMap.<Class<?>, StatementPrinter<?>>builder()
          .put(BulkSimpleStatement.class, new BulkSimpleStatementPrinter())
          .put(BulkBoundStatement.class, new BulkBoundStatementPrinter())
          .put(SimpleStatement.class, new SimpleStatementPrinter<>())
          .put(BoundStatement.class, new BoundStatementPrinter<>())
          .put(BatchStatement.class, new BatchStatementPrinter())
          .put(Statement.class, new DefaultStatementPrinter())
          .build();

  private final ConcurrentMap<Class<?>, StatementPrinter<?>> printers = new NonBlockingHashMap<>();

  StatementPrinterRegistry() {}

  public <S extends Statement<S>> StatementPrinter<S> findPrinter(Class<S> statementClass) {
    StatementPrinter<?> printer = lookupPrinter(statementClass, printers);
    if (printer == null) {
      printer = lookupPrinter(statementClass, BUILT_IN_PRINTERS);
    }
    assert printer != null;
    @SuppressWarnings("unchecked")
    StatementPrinter<S> sp = (StatementPrinter<S>) printer;
    return sp;
  }

  public <S extends Statement<S>> void register(StatementPrinter<S> printer) {
    printers.put(printer.getSupportedStatementClass(), printer);
  }

  private static StatementPrinter<?> lookupPrinter(
      Class<?> clazz, Map<Class<?>, StatementPrinter<?>> map) {
    StatementPrinter<?> printer = null;
    for (Class<?> key = clazz; printer == null && key != null; key = key.getSuperclass()) {
      printer = map.get(key);
    }
    return printer;
  }
}
