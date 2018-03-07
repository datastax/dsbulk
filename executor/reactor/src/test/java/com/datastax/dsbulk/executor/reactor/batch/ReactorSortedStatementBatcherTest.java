/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor.batch;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class ReactorSortedStatementBatcherTest extends ReactorStatementBatcherTest {

  @Test
  void should_batch_by_routing_key_as_operator() throws Exception {
    assignRoutingKeys();
    ReactorSortedStatementBatcher batcher = new ReactorSortedStatementBatcher();
    List<Statement> statements =
        Flux.just(stmt1, stmt2, stmt6, stmt3, stmt4, stmt5).compose(batcher).collectList().block();
    assertThat(statements)
        .usingFieldByFieldElementComparator()
        .containsExactly(batch126, batch34, stmt5);
  }

  @Test
  void should_batch_by_routing_token_as_operator() throws Exception {
    assignRoutingTokens();
    ReactorSortedStatementBatcher batcher = new ReactorSortedStatementBatcher();
    List<Statement> statements =
        Flux.just(stmt1, stmt2, stmt5, stmt6, stmt3, stmt4).compose(batcher).collectList().block();
    assertThat(statements).usingFieldByFieldElementComparator().containsExactly(batch1256, batch34);
  }
}
