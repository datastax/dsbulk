/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import com.datastax.driver.core.BatchStatement;

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
