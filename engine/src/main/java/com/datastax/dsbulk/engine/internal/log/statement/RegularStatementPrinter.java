/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
