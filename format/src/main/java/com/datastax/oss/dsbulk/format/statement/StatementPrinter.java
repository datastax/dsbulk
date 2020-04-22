/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.format.statement;

import com.datastax.oss.driver.api.core.cql.Statement;

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
  Class<?> getSupportedStatementClass();

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
