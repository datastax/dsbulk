/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.reactor.batch;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/** */
class ReactorUnsortedStatementBatcherTest extends ReactorStatementBatcherTest {

  @Test
  void should_batch_by_routing_key_as_operator() throws Exception {
    assignRoutingKeys();
    ReactorUnsortedStatementBatcher batcher = new ReactorUnsortedStatementBatcher();
    List<Statement> statements =
        Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)
            .transform(batcher)
            .collectList()
            .block();
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch126, batch34, stmt5);
  }

  @Test
  void should_batch_by_routing_token_as_operator() throws Exception {
    assignRoutingTokens();
    ReactorUnsortedStatementBatcher batcher = new ReactorUnsortedStatementBatcher();
    List<Statement> statements =
        Flux.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)
            .transform(batcher)
            .collectList()
            .block();
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }
}
