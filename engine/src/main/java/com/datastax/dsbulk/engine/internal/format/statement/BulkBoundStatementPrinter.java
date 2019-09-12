/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.format.statement;

import static com.datastax.dsbulk.commons.internal.format.statement.StatementFormatVerbosity.EXTENDED;

import com.datastax.dsbulk.commons.internal.format.statement.BoundStatementPrinter;
import com.datastax.dsbulk.commons.internal.format.statement.StatementFormatVerbosity;
import com.datastax.dsbulk.commons.internal.format.statement.StatementWriter;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.internal.statement.BulkBoundStatement;
import com.datastax.dsbulk.engine.internal.statement.BulkStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

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
    if (verbosity.compareTo(EXTENDED) >= 0) {
      @SuppressWarnings("unchecked")
      BulkStatement<Record> bulkStatement = (BulkStatement<Record>) statement;
      appendRecord(bulkStatement, out);
    }
  }
}
