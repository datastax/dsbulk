/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.format.statement;

import com.datastax.oss.driver.api.core.cql.Statement;

public class DefaultStatementPrinter extends StatementPrinterBase {

  @Override
  public Class<? extends Statement> getSupportedStatementClass() {
    return Statement.class;
  }
}
