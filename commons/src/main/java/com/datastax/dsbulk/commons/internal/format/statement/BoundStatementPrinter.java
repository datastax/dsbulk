/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.format.statement;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import java.util.List;

public class BoundStatementPrinter extends StatementPrinterBase<BoundStatement> {

  @Override
  public Class<? extends BoundStatement> getSupportedStatementClass() {
    return BoundStatement.class;
  }

  @Override
  protected List<String> collectStatementProperties(
      BoundStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    List<String> properties = super.collectStatementProperties(statement, out, verbosity);
    ColumnDefinitions metadata = statement.getPreparedStatement().getVariableDefinitions();
    properties.add(0, String.format(StatementFormatterSymbols.boundValuesCount, metadata.size()));
    return properties;
  }

  @Override
  protected void printQueryString(
      BoundStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    out.newLine().indent().appendQueryStringFragment(statement.getPreparedStatement().getQuery());
  }

  @Override
  protected void printBoundValues(
      BoundStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    if (statement.getPreparedStatement().getVariableDefinitions().size() > 0) {
      ColumnDefinitions metadata = statement.getPreparedStatement().getVariableDefinitions();
      for (int i = 0; i < metadata.size(); i++) {
        out.newLine().indent();
        if (statement.isSet(i)) {
          out.appendBoundValue(
              metadata.get(i).getName().asInternal(),
              statement.getObject(i),
              metadata.get(i).getType());
        } else {
          out.appendUnsetBoundValue(metadata.get(i).getName().asInternal());
        }
        if (out.maxAppendedBoundValuesExceeded()) {
          break;
        }
      }
    }
  }
}
