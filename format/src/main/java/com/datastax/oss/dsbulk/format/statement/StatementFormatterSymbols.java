/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
