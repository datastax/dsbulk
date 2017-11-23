/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import static com.datastax.driver.core.DriverCoreHooks.wrappedStatement;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;

/** */
public class StatementWrapperPrinter implements StatementPrinter<StatementWrapper> {

  @Override
  public Class<StatementWrapper> getSupportedStatementClass() {
    return StatementWrapper.class;
  }

  @Override
  public void print(
      StatementWrapper statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    Statement wrappedStatement = wrappedStatement(statement);
    StatementPrinter<? super Statement> printer =
        out.getPrinterRegistry().findPrinter(wrappedStatement.getClass());
    printer.print(wrappedStatement, out, verbosity);
  }
}
