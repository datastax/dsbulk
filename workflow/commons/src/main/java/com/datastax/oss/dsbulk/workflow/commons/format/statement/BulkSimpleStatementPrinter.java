/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.format.statement;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.format.statement.SimpleStatementPrinter;
import com.datastax.oss.dsbulk.format.statement.StatementFormatVerbosity;
import com.datastax.oss.dsbulk.format.statement.StatementWriter;
import com.datastax.oss.dsbulk.workflow.commons.statement.BulkSimpleStatement;
import com.datastax.oss.dsbulk.workflow.commons.statement.BulkStatement;

public class BulkSimpleStatementPrinter extends SimpleStatementPrinter
    implements BulkStatementPrinter {

  @Override
  public Class<BulkSimpleStatement> getSupportedStatementClass() {
    return BulkSimpleStatement.class;
  }

  @Override
  protected void printHeader(
      SimpleStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
    super.printHeader(statement, out, verbosity);
    if (verbosity.compareTo(StatementFormatVerbosity.EXTENDED) >= 0) {
      @SuppressWarnings("unchecked")
      BulkStatement<Record> bulkStatement = (BulkStatement<Record>) statement;
      appendRecord(bulkStatement, out);
    }
  }
}
