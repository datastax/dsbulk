/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava.batch;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Statement;
import io.reactivex.Flowable;
import java.util.List;
import org.junit.jupiter.api.Test;

class RxJavaUnsortedStatementBatcherTest extends RxJavaStatementBatcherTest {

  @Test
  void should_batch_by_routing_key_as_operator() throws Exception {
    assignRoutingKeys();
    RxJavaUnsortedStatementBatcher batcher = new RxJavaUnsortedStatementBatcher();
    List<Statement> statements =
        Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)
            .compose(batcher)
            .toList()
            .blockingGet();
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch126, batch34, stmt5);
  }

  @Test
  void should_batch_by_routing_token_as_operator() throws Exception {
    assignRoutingTokens();
    RxJavaUnsortedStatementBatcher batcher = new RxJavaUnsortedStatementBatcher();
    List<Statement> statements =
        Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)
            .compose(batcher)
            .toList()
            .blockingGet();
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }
}
