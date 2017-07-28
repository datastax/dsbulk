/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.statement;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import io.reactivex.Flowable;
import org.junit.Test;

/** */
public class RxJavaStatementBatcherTest extends StatementBatcherTest {

  @Test
  public void should_batch_by_routing_key_publisher() throws Exception {
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher();
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchByRoutingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .usingFieldByFieldElementComparator()
        .contains(batch126, batch34, stmt5);
  }

  @Test
  public void should_batch_single_statement_publisher() throws Exception {
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher();
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchSingle(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(((BatchStatement) statements.singleOrError().blockingGet()).getStatements())
        .containsExactly(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
  }
}
