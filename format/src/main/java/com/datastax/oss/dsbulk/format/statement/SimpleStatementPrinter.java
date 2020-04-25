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
