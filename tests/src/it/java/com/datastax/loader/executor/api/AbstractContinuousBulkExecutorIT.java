/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api;

import com.datastax.driver.dse.DseSession;
import com.datastax.loader.tests.utils.CsvUtils;
import javax.inject.Inject;
import org.junit.After;
import org.junit.BeforeClass;

public abstract class AbstractContinuousBulkExecutorIT extends AbstractBulkExecutorIT {

  @Inject static DseSession session;

  @BeforeClass
  public static void createTableAndPreparedStatement() {
    CsvUtils.createIpByCountryTable(session);
    insertStatement = CsvUtils.prepareInsertStatement(session);
  }

  @Override
  @After
  public void truncateTable() {
    CsvUtils.truncateIpByCountryTable(session);
  }

}
