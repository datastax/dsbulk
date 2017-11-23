/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity.EXTENDED;

import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;

/** */
public class BulkBoundStatementPrinter extends BoundStatementPrinter<BulkBoundStatement<Record>> {

  @Override
  public Class<? extends Statement> getSupportedStatementClass() {
    return BulkBoundStatement.class;
  }

  @Override
  protected void printHeader(
      BulkBoundStatement<Record> statement,
      StatementWriter out,
      StatementFormatVerbosity verbosity) {
    super.printHeader(statement, out, verbosity);
    if (verbosity.compareTo(EXTENDED) >= 0) {
      Record record = statement.getSource();
      out.appendRecord(record);
    }
  }
}
