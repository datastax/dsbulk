/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.format.statement;

import static com.datastax.oss.dsbulk.format.statement.StatementFormatterSymbols.boundValuesCount;
import static com.datastax.oss.dsbulk.format.statement.StatementFormatterSymbols.listElementSeparator;
import static com.datastax.oss.dsbulk.format.statement.StatementFormatterSymbols.nameValueSeparator;
import static com.datastax.oss.dsbulk.format.statement.StatementFormatterSymbols.statementsCount;
import static com.datastax.oss.dsbulk.format.statement.StatementFormatterSymbols.summaryEnd;
import static com.datastax.oss.dsbulk.format.statement.StatementFormatterSymbols.summaryStart;
import static com.datastax.oss.dsbulk.format.statement.StatementFormatterSymbols.truncatedOutput;

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
    if (verbosity.compareTo(StatementFormatVerbosity.NORMAL) > 0) {
      out.append(listElementSeparator)
          .append(String.format(boundValuesCount, valuesCount(statement)));
    }
    out.append(summaryEnd);
    if (verbosity.compareTo(StatementFormatVerbosity.NORMAL) >= 0
        && out.getLimits().maxInnerStatements > 0) {
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
