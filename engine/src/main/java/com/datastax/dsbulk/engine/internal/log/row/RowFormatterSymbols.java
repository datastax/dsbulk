/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.row;

class RowFormatterSymbols {

  static final String lineSeparator = System.lineSeparator();
  static final String summaryStart = " [";
  static final String summaryEnd = "]";
  static final String valuesCount = "%s values";
  static final String truncatedOutput = "...";
  static final String nullValue = "<NULL>";
  static final String nameValueSeparator = ": ";
}
