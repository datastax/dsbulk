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

import com.datastax.dsbulk.commons.internal.format.statement.SimpleStatementPrinter;
import com.datastax.dsbulk.commons.internal.format.statement.StatementFormatVerbosity;
import com.datastax.dsbulk.commons.internal.format.statement.StatementWriter;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.internal.statement.BulkSimpleStatement;
import com.datastax.dsbulk.engine.internal.statement.BulkStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

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
    if (verbosity.compareTo(EXTENDED) >= 0) {
      @SuppressWarnings("unchecked")
      BulkStatement<Record> bulkStatement = (BulkStatement<Record>) statement;
      appendRecord(bulkStatement, out);
    }
  }
}
