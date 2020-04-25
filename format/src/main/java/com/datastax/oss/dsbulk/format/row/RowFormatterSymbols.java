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
package com.datastax.oss.dsbulk.format.row;

class RowFormatterSymbols {

  static final String lineSeparator = System.lineSeparator();
  static final String summaryStart = " [";
  static final String summaryEnd = "]";
  static final String valuesCount = "%s values";
  static final String truncatedOutput = "...";
  static final String nullValue = "<NULL>";
  static final String nameValueSeparator = ": ";
}
