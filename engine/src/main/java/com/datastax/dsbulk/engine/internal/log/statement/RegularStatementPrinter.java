/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import com.datastax.driver.core.RegularStatement;

/**
 * Common parent class for {@link StatementPrinter} implementations dealing with subclasses of
 * {@link RegularStatement}.
 */
public abstract class RegularStatementPrinter<S extends RegularStatement>
    extends StatementPrinterBase<S> {

  @Override
  protected void printQueryString(
      S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    out.newLine();
    out.indent();
    out.appendQueryStringFragment(statement.getQueryString(out.getCodecRegistry()));
  }
}
