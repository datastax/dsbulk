/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.ccm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.BulkExecutorITBase;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import io.reactivex.Flowable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BulkExecutorCCMITBase extends BulkExecutorITBase {

  private final CCMCluster ccm;
  private final Session session;

  public BulkExecutorCCMITBase(
      CCMCluster ccm,
      Session session,
      BulkExecutor failFastExecutor,
      BulkExecutor failSafeExecutor) {
    super(failFastExecutor, failSafeExecutor);
    this.ccm = ccm;
    this.session = session;
  }

  @BeforeAll
  void createTables() {
    session.execute("CREATE TABLE test_write (pk int PRIMARY KEY, v int)");
    session.execute("CREATE TABLE test_read (pk int PRIMARY KEY, v int)");
    for (int i = 0; i < 100; i++) {
      session.execute(String.format("INSERT INTO test_read (pk, v) VALUES (%d, %d)", i, i));
    }
  }

  @AfterEach
  void truncateWriteTable() {
    session.execute("TRUNCATE test_write");
  }

  @Test
  void should_insert_CAS() {

    assumeTrue(
        ccm.getCassandraVersion().compareTo(Version.parse("2.0.0")) > 0, "LWT requires C* 2.0+");

    // regular insert
    WriteResult result = failSafeExecutor.writeSync("INSERT INTO test_write (pk, v) VALUES (0, 0)");
    assertThat(result.wasApplied()).isTrue();
    assertThat(result.getFailedWrites()).isEmpty();
    assertThat(result.getError()).isEmpty();
    // failed insert
    result = failSafeExecutor.writeSync("not a valid query");
    assertThat(result.wasApplied()).isFalse();
    assertThat(result.getFailedWrites()).isEmpty();
    assertThat(result.getError())
        .isNotEmpty()
        .hasValueSatisfying(error -> assertThat(error).hasRootCauseInstanceOf(SyntaxError.class));
    // CAS insert (successful)
    result =
        failSafeExecutor.writeSync("INSERT INTO test_write (pk, v) VALUES (1, 1) IF NOT EXISTS");
    assertThat(result.wasApplied()).isTrue();
    assertThat(result.getFailedWrites()).isEmpty();
    assertThat(result.getError()).isEmpty();
    // CAS insert (unsuccessful)
    result =
        failSafeExecutor.writeSync("INSERT INTO test_write (pk, v) VALUES (1, 1) IF NOT EXISTS");
    assertThat(result.wasApplied()).isFalse();
    assertThat(result.getFailedWrites().map(row -> tuple(row.getInt("pk"), row.getInt("v"))))
        .containsExactly(tuple(1, 1));
    assertThat(result.getError()).isEmpty();
    // batch CAS insert (successful)
    result =
        failSafeExecutor.writeSync(
            "BEGIN UNLOGGED BATCH "
                + "INSERT INTO test_write (pk, v) VALUES (2, 2) IF NOT EXISTS; "
                + "INSERT INTO test_write (pk, v) VALUES (2, 3) IF NOT EXISTS; "
                + "APPLY BATCH");
    assertThat(result.wasApplied()).isTrue();
    assertThat(result.getFailedWrites()).isEmpty();
    assertThat(result.getError()).isEmpty();
    // batch CAS insert (unsuccessful)
    result =
        failSafeExecutor.writeSync(
            "BEGIN UNLOGGED BATCH "
                + "INSERT INTO test_write (pk, v) VALUES (2, 3) IF NOT EXISTS; "
                + "INSERT INTO test_write (pk, v) VALUES (2, 4) IF NOT EXISTS; "
                + "APPLY BATCH");
    assertThat(result.wasApplied()).isFalse();
    assertThat(result.getFailedWrites().map(row -> tuple(row.getInt("pk"), row.getInt("v"))))
        .containsExactly(tuple(2, 3));
    assertThat(result.getError()).isEmpty();
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
