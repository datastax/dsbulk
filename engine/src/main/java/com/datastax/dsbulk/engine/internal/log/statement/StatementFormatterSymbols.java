/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

/** */
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
  public static final String nameValueSeparator = " : ";
  public static final String idempotent = "idempotence : %s";
  public static final String consistencyLevel = "CL : %s";
  public static final String serialConsistencyLevel = "serial CL : %s";
  public static final String defaultTimestamp = "default timestamp : %s";
  public static final String readTimeoutMillis = "read-timeout millis : %s";
}
