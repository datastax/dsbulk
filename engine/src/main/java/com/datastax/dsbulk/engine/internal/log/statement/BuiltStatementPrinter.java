/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import static com.datastax.driver.core.DriverCoreHooks.valuesCount;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.boundValuesCount;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import java.util.List;

/** */
public class BuiltStatementPrinter extends RegularStatementPrinter<BuiltStatement> {

  @Override
  public Class<? extends Statement> getSupportedStatementClass() {
    return BuiltStatement.class;
  }

  @Override
  protected List<String> collectStatementProperties(
      BuiltStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    List<String> properties = super.collectStatementProperties(statement, out, verbosity);
    properties.add(
        0,
        String.format(
            boundValuesCount,
            valuesCount(statement, out.getProtocolVersion(), out.getCodecRegistry())));
    return properties;
  }

  @Override
  protected void printBoundValues(
      BuiltStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    if (valuesCount(statement, out.getProtocolVersion(), out.getCodecRegistry()) > 0) {
      // BuiltStatement does not use named values
      for (int i = 0;
          i < valuesCount(statement, out.getProtocolVersion(), out.getCodecRegistry());
          i++) {
        out.newLine();
        out.indent();
        out.appendBoundValue(i, statement.getObject(i), null);
        if (out.maxAppendedBoundValuesExceeded()) break;
      }
    }
  }
}
