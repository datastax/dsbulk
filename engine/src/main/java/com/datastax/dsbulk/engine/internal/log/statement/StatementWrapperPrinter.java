/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import static com.datastax.driver.core.DriverCoreHooks.wrappedStatement;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;

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
