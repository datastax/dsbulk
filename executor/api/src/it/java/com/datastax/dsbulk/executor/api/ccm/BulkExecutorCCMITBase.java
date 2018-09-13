/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.ccm;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.BulkExecutorITBase;
import io.reactivex.Flowable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BulkExecutorCCMITBase extends BulkExecutorITBase {

  private final Session session;

  public BulkExecutorCCMITBase(
      Session session, BulkExecutor failFastExecutor, BulkExecutor failSafeExecutor) {
    super(failFastExecutor, failSafeExecutor);
    this.session = session;
  }

  @BeforeAll
  void createTables() {
    session.execute("CREATE TABLE IF NOT EXISTS test_write (pk int PRIMARY KEY, v int)");
    session.execute("CREATE TABLE IF NOT EXISTS test_read (pk int PRIMARY KEY, v int)");
    for (int i = 0; i < 100; i++) {
      session.execute(String.format("INSERT INTO test_read (pk, v) VALUES (%d, %d)", i, i));
    }
  }

  @AfterEach
  void truncateWriteTable() {
    session.execute("TRUNCATE test_write");
  }

  @Override
  protected void verifyWrites(int expected) {
    verifyReads(
        expected,
        0,
        Flowable.fromPublisher(failFastExecutor.readReactive("SELECT pk, v FROM test_write"))
            .blockingIterable());
  }
}
