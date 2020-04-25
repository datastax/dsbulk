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
