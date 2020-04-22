/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.format.statement;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.format.statement.BoundStatementPrinter;
import com.datastax.oss.dsbulk.format.statement.StatementFormatVerbosity;
import com.datastax.oss.dsbulk.format.statement.StatementWriter;
import com.datastax.oss.dsbulk.workflow.commons.statement.BulkBoundStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.BulkStatement;

public class BulkBoundStatementPrinter extends BoundStatementPrinter
    implements BulkStatementPrinter {

  @Override
  public Class<BulkBoundStatement> getSupportedStatementClass() {
    return BulkBoundStatement.class;
  }

  @Override
  protected void printHeader(
      BoundStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    super.printHeader(statement, out, verbosity);
    if (verbosity.compareTo(StatementFormatVerbosity.EXTENDED) >= 0) {
      @SuppressWarnings("unchecked")
      BulkStatement<Record> bulkStatement = (BulkStatement<Record>) statement;
      appendRecord(bulkStatement, out);
    }
  }
}
