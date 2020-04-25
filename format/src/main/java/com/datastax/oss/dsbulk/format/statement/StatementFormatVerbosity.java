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

/**
 * The desired statement format verbosity.
 *
 * <p>This should be used as a guideline as to how much information about the statement should be
 * extracted and formatted.
 */
public enum StatementFormatVerbosity {

  // the enum order matters

  /** Formatters should only print a basic information in summarized form. */
  ABRIDGED,

  /**
   * Formatters should print basic information in summarized form, and the statement's query string,
   * if available.
   *
   * <p>For batch statements, this verbosity level should allow formatters to print information
   * about the batch's inner statements.
   */
  NORMAL,

  /**
   * Formatters should print full information, including the statement's query string, if available,
   * and the statement's bound values, if available.
   */
  EXTENDED
}
