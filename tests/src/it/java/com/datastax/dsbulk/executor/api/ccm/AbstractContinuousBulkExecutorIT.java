/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.ccm;

import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import javax.inject.Inject;
import org.junit.After;
import org.junit.BeforeClass;

public abstract class AbstractContinuousBulkExecutorIT extends AbstractBulkExecutorIT {

  @Inject static ContinuousPagingSession session;

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
