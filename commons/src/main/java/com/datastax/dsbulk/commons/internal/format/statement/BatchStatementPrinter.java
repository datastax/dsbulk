/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.format.statement;

import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatVerbosity.NORMAL;
import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatterSymbols.boundValuesCount;
import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatterSymbols.listElementSeparator;
import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatterSymbols.nameValueSeparator;
import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatterSymbols.statementsCount;
import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatterSymbols.summaryEnd;
import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatterSymbols.summaryStart;
import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatterSymbols.truncatedOutput;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

public class BatchStatementPrinter implements StatementPrinter<BatchStatement> {

  @Override
  public Class<BatchStatement> getSupportedStatementClass() {
    return BatchStatement.class;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void print(
      BatchStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    out.appendClassNameAndHashCode(statement)
        .append(summaryStart)
        .append(statement.getBatchType())
        .append(listElementSeparator)
        .append(String.format(statementsCount, statement.size()));
    if (verbosity.compareTo(NORMAL) > 0) {
      out.append(listElementSeparator)
          .append(String.format(boundValuesCount, valuesCount(statement)));
    }
    out.append(summaryEnd);
    if (verbosity.compareTo(NORMAL) >= 0 && out.getLimits().maxInnerStatements > 0) {
      out.newLine();
      int i = 1;
      for (Statement<?> stmt : statement) {
        if (i > out.getLimits().maxInnerStatements) {
          out.append(truncatedOutput);
          break;
        }
        out.append(i++).append(nameValueSeparator);
        StatementPrinter printer = out.getPrinterRegistry().findPrinter(stmt.getClass());
        out.indent();
        printer.print(stmt, out.createChildWriter(), verbosity);
        out.newLine();
      }
    }
  }

  private static int valuesCount(BatchStatement batchStatement) {
    int count = 0;
    for (BatchableStatement statement : batchStatement) {
      if (statement instanceof BoundStatement) {
        count += ((BoundStatement) statement).size();
      } else if (statement instanceof SimpleStatement) {
        count +=
            ((SimpleStatement) statement).getNamedValues().size()
                + ((SimpleStatement) statement).getPositionalValues().size();
      }
    }
    return count;
  }
}
