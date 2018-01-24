/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import com.datastax.driver.core.Statement;

public class DefaultStatementPrinter extends StatementPrinterBase<Statement> {

  @Override
  public Class<? extends Statement> getSupportedStatementClass() {
    return Statement.class;
  }
}
