/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
