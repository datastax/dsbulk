/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log.statement;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.loader.engine.internal.statement.BulkBoundStatement;
import com.datastax.loader.engine.internal.statement.BulkSimpleStatement;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A registry for {@link StatementPrinter statement printers}.
 *
 * <p>This class is thread-safe.
 */
public final class StatementPrinterRegistry {

  private static final Map<Class<?>, StatementPrinter<?>> BUILT_IN_PRINTERS =
      ImmutableMap.<Class<?>, StatementPrinter<?>>builder()
          .put(BulkSimpleStatement.class, new BulkSimpleStatementPrinter())
          .put(BulkBoundStatement.class, new BulkBoundStatementPrinter())
          .put(SimpleStatement.class, new SimpleStatementPrinter<>())
          .put(BuiltStatement.class, new BuiltStatementPrinter())
          .put(BoundStatement.class, new BoundStatementPrinter<>())
          .put(BatchStatement.class, new BatchStatementPrinter())
          .put(StatementWrapper.class, new StatementWrapperPrinter())
          .put(Statement.class, new DefaultStatementPrinter())
          .build();

  private final ConcurrentMap<Class<?>, StatementPrinter<?>> printers = new ConcurrentHashMap<>();

  StatementPrinterRegistry() {}

  /**
   * Attempts to locate the best {@link StatementPrinter printer} for the given statement.
   *
   * <p>The registry first tries to locate a user-defined printer that is capable of printing the
   * given statement; if none is found, then built-in printers will be used.
   *
   * @param statementClass The statement class to find a printer for.
   * @return The best {@link StatementPrinter printer} for the given statement. Cannot be {@code
   *     null}.
   */
  public <S extends Statement> StatementPrinter<? super S> findPrinter(Class<?> statementClass) {
    StatementPrinter<?> printer = lookupPrinter(statementClass, printers);
    if (printer == null) printer = lookupPrinter(statementClass, BUILT_IN_PRINTERS);
    assert printer != null;
    @SuppressWarnings("unchecked")
    StatementPrinter<? super S> sp = (StatementPrinter<? super S>) printer;
    return sp;
  }

  public <S extends Statement> void register(StatementPrinter<S> printer) {
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
