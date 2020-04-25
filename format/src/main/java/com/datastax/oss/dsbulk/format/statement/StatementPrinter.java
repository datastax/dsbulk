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

import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * A statement printer is responsible for printing a specific type of {@link Statement statement},
 * with a given {@link StatementFormatVerbosity verbosity level}, and using a given {@link
 * StatementWriter statement writer}.
 *
 * @param <S> The type of statement that this printer handles
 */
public interface StatementPrinter<S extends Statement> {

  /**
   * The concrete {@link Statement} subclass that this printer handles.
   *
   * <p>In case of subtype polymorphism, if this printer handles more than one concrete subclass,
   * the most specific common ancestor should be returned here.
   *
   * @return The concrete {@link Statement} subclass that this printer handles.
   */
  Class<?> getSupportedStatementClass();

  /**
   * Prints the given {@link Statement statement}, using the given {@link StatementWriter statement
   * writer} and the given {@link StatementFormatVerbosity verbosity level}.
   *
   * @param statement the statement to print
   * @param out the writer to use
   * @param verbosity the verbosity to use
   */
  void print(S statement, StatementWriter out, StatementFormatVerbosity verbosity);
}
