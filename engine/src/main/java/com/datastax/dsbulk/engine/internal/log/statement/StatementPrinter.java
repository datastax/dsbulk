/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import com.datastax.driver.core.Statement;

/**
 * A statement printer is responsible for printing a specific type of {@link Statement statement},
 * with a given {@link StatementFormatVerbosity verbosity level}, and using a given {@link
 * StatementWriter statement writer}.
 *
 * @param <S> The type of statement that this printer handles
 */
public interface StatementPrinter<S extends Statement> {

  /**
   * The concrete {@link Statement} subclass that this printer handles.
   *
   * <p>In case of subtype polymorphism, if this printer handles more than one concrete subclass,
   * the most specific common ancestor should be returned here.
   *
   * @return The concrete {@link Statement} subclass that this printer handles.
   */
  Class<? extends Statement> getSupportedStatementClass();

  /**
   * Prints the given {@link Statement statement}, using the given {@link StatementWriter statement
   * writer} and the given {@link StatementFormatVerbosity verbosity level}.
   *
   * @param statement the statement to print
   * @param out the writer to use
   * @param verbosity the verbosity to use
   */
  void print(S statement, StatementWriter out, StatementFormatVerbosity verbosity);
}
