/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

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
