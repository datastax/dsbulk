/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.format.statement;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.util.List;
import java.util.Map;

public class SimpleStatementPrinter extends StatementPrinterBase<SimpleStatement> {

  @Override
  public Class<? extends SimpleStatement> getSupportedStatementClass() {
    return SimpleStatement.class;
  }

  @Override
  protected List<String> collectStatementProperties(
      SimpleStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    List<String> properties = super.collectStatementProperties(statement, out, verbosity);
    properties.add(
        0,
        String.format(
            StatementFormatterSymbols.boundValuesCount,
            statement.getPositionalValues().size() + statement.getNamedValues().size()));
    return properties;
  }

  @Override
  protected void printQueryString(
      SimpleStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    out.newLine();
    out.indent();
    out.appendQueryStringFragment(statement.getQuery());
  }

  @Override
  protected void printBoundValues(
      SimpleStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    List<Object> positionalValues = statement.getPositionalValues();
    Map<CqlIdentifier, Object> namedValues = statement.getNamedValues();
    if (!positionalValues.isEmpty() || !namedValues.isEmpty()) {
      if (!namedValues.isEmpty()) {
        for (CqlIdentifier valueName : namedValues.keySet()) {
          out.newLine();
          out.indent();
          out.appendBoundValue(valueName.asInternal(), namedValues.get(valueName), null);
          if (out.maxAppendedBoundValuesExceeded()) {
            break;
          }
        }
      }
      if (!positionalValues.isEmpty()) {
        for (int i = 0; i < positionalValues.size(); i++) {
          out.newLine();
          out.indent();
          out.appendBoundValue(i, positionalValues.get(i), null);
          if (out.maxAppendedBoundValuesExceeded()) {
            break;
          }
        }
      }
    }
  }
}
