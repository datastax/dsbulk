/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.boundValuesCount;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import java.util.List;

public class SimpleStatementPrinter<T extends SimpleStatement> extends RegularStatementPrinter<T> {

  @Override
  public Class<? extends Statement> getSupportedStatementClass() {
    return SimpleStatement.class;
  }

  @Override
  protected List<String> collectStatementProperties(
      T statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    List<String> properties = super.collectStatementProperties(statement, out, verbosity);
    properties.add(0, String.format(boundValuesCount, statement.valuesCount()));
    return properties;
  }

  @Override
  protected void printBoundValues(
      T statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    if (statement.valuesCount() > 0) {
      if (statement.usesNamedValues()) {
        for (String valueName : statement.getValueNames()) {
          out.newLine();
          out.indent();
          out.appendBoundValue(valueName, statement.getObject(valueName), null);
          if (out.maxAppendedBoundValuesExceeded()) {
            break;
          }
        }
      } else {
        for (int i = 0; i < statement.valuesCount(); i++) {
          out.newLine();
          out.indent();
          out.appendBoundValue(i, statement.getObject(i), null);
          if (out.maxAppendedBoundValuesExceeded()) {
            break;
          }
        }
      }
    }
  }
}
