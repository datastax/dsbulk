/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log.statement;

import com.datastax.driver.core.Statement;

public class DefaultStatementPrinter extends StatementPrinterBase<Statement> {

  @Override
  public Class<? extends Statement> getSupportedStatementClass() {
    return Statement.class;
  }
}
