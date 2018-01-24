/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import static com.datastax.driver.core.DriverCoreHooks.batchType;
import static com.datastax.driver.core.DriverCoreHooks.valuesCount;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity.NORMAL;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.boundValuesCount;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.listElementSeparator;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.nameValueSeparator;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.statementsCount;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.summaryEnd;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.summaryStart;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.truncatedOutput;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;

/** */
public class BatchStatementPrinter implements StatementPrinter<BatchStatement> {

  @Override
  public Class<BatchStatement> getSupportedStatementClass() {
    return BatchStatement.class;
  }

  @Override
  public void print(
      BatchStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    out.appendClassNameAndHashCode(statement)
        .append(summaryStart)
        .append(batchType(statement))
        .append(listElementSeparator)
        .append(String.format(statementsCount, statement.size()));
    if (verbosity.compareTo(NORMAL) > 0) {
      out.append(listElementSeparator)
          .append(
              String.format(
                  boundValuesCount,
                  valuesCount(statement, out.getProtocolVersion(), out.getCodecRegistry())));
    }
    out.append(summaryEnd);
    if (verbosity.compareTo(NORMAL) >= 0 && out.getLimits().maxInnerStatements > 0) {
      out.newLine();
      int i = 1;
      for (Statement stmt : statement.getStatements()) {
        if (i > out.getLimits().maxInnerStatements) {
          out.append(truncatedOutput);
          break;
        }
        out.append(i++).append(nameValueSeparator);
        StatementPrinter<? super Statement> printer =
            out.getPrinterRegistry().findPrinter(stmt.getClass());
        out.indent();
        printer.print(stmt, out.createChildWriter(), verbosity);
        out.newLine();
      }
    }
  }
}
