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

class RxJavaSortedStatementBatcherTest extends RxJavaStatementBatcherTest {

  @Test
  void should_batch_by_routing_key_as_operator() throws Exception {
    assignRoutingKeys();
    RxJavaSortedStatementBatcher batcher = new RxJavaSortedStatementBatcher();
    List<Statement> statements =
        Flowable.just(stmt1, stmt2, stmt6, stmt3, stmt4, stmt5)
            .compose(batcher)
            .toList()
            .blockingGet();
    assertThat(statements)
        .usingFieldByFieldElementComparator()
        .containsExactly(batch126, batch34, stmt5);
  }

  @Test
  void should_batch_by_routing_token_as_operator() throws Exception {
    assignRoutingTokens();
    RxJavaSortedStatementBatcher batcher = new RxJavaSortedStatementBatcher();
    List<Statement> statements =
        Flowable.just(stmt1, stmt2, stmt5, stmt6, stmt3, stmt4)
            .compose(batcher)
            .toList()
            .blockingGet();
    assertThat(statements).usingFieldByFieldElementComparator().containsExactly(batch1256, batch34);
  }
}
