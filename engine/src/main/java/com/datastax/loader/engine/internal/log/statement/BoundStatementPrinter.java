/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log.statement;

import static com.datastax.loader.engine.internal.log.statement.StatementFormatterSymbols.boundValuesCount;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Statement;
import java.util.List;

/** */
public class BoundStatementPrinter<T extends BoundStatement> extends StatementPrinterBase<T> {

  @Override
  public Class<? extends Statement> getSupportedStatementClass() {
    return BoundStatement.class;
  }

  @Override
  protected List<String> collectStatementProperties(
      T statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    List<String> properties = super.collectStatementProperties(statement, out, verbosity);
    ColumnDefinitions metadata = statement.preparedStatement().getVariables();
    properties.add(0, String.format(boundValuesCount, metadata.size()));
    return properties;
  }

  @Override
  protected void printQueryString(
      T statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    out.newLine();
    out.indent();
    out.appendQueryStringFragment(statement.preparedStatement().getQueryString());
  }

  @Override
  protected void printBoundValues(
      T statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    if (statement.preparedStatement().getVariables().size() > 0) {
      ColumnDefinitions metadata = statement.preparedStatement().getVariables();
      if (metadata.size() > 0) {
        for (int i = 0; i < metadata.size(); i++) {
          out.newLine();
          out.indent();
          if (statement.isSet(i))
            out.appendBoundValue(metadata.getName(i), statement.getObject(i), metadata.getType(i));
          else out.appendUnsetBoundValue(metadata.getName(i));
          if (out.maxAppendedBoundValuesExceeded()) break;
        }
      }
    }
  }
}
