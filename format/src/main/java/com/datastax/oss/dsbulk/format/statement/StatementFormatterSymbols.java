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

public class StatementFormatterSymbols {

  public static final String lineSeparator = System.lineSeparator();
  public static final String summaryStart = " [";
  public static final String summaryEnd = "]";
  public static final String boundValuesCount = "%s values";
  public static final String statementsCount = "%s stmts";
  public static final String truncatedOutput = "...";
  public static final String nullValue = "<NULL>";
  public static final String unsetValue = "<UNSET>";
  public static final String listElementSeparator = ", ";
  public static final String nameValueSeparator = ": ";
  public static final String idempotent = "idempotence: %s";
  public static final String consistencyLevel = "CL: %s";
  public static final String serialConsistencyLevel = "serial CL: %s";
  public static final String timestamp = "timestamp: %s";
  public static final String timeout = "timeout: %s";
}
