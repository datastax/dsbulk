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

import com.datastax.oss.driver.api.core.cql.BatchStatement;

/**
 * A set of user-defined limitation rules that {@link StatementPrinter printers} should strive to
 * comply with when formatting statements.
 *
 * <p>Limits defined in this class should be considered on a per-statement basis; i.e. if the
 * maximum query string length is 100 and the statement to format is a {@link BatchStatement} with 5
 * inner statements, each inner statement should be allowed to print a maximum of 100 characters of
 * its query string.
 *
 * <p>This class is NOT thread-safe.
 */
public final class StatementFormatterLimits {

  /**
   * A special value that conveys the notion of "unlimited". All fields in this class accept this
   * value.
   */
  public static final int UNLIMITED = -1;

  public final int maxQueryStringLength;
  public final int maxBoundValueLength;
  public final int maxBoundValues;
  public final int maxInnerStatements;
  public final int maxOutgoingPayloadEntries;
  public final int maxOutgoingPayloadValueLength;

  StatementFormatterLimits(
      int maxQueryStringLength,
      int maxBoundValueLength,
      int maxBoundValues,
      int maxInnerStatements,
      int maxOutgoingPayloadEntries,
      int maxOutgoingPayloadValueLength) {
    this.maxQueryStringLength = maxQueryStringLength;
    this.maxBoundValueLength = maxBoundValueLength;
    this.maxBoundValues = maxBoundValues;
    this.maxInnerStatements = maxInnerStatements;
    this.maxOutgoingPayloadEntries = maxOutgoingPayloadEntries;
    this.maxOutgoingPayloadValueLength = maxOutgoingPayloadValueLength;
  }
}
