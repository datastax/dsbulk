/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava.batch;

import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.REPLICA_SET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dsbulk.executor.api.batch.StatementBatcherTest;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import io.reactivex.Flowable;
import java.util.HashSet;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RxJavaStatementBatcherTest extends StatementBatcherTest {

  @Test
  void should_batch_by_routing_key_reactive() {
    assignRoutingKeys();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher();
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt6), tuple(stmt3, stmt4), tuple(stmt5));
  }

  @Test
  void should_batch_by_routing_token_reactive() {
    assignRoutingTokens();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher();
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @SuppressWarnings("unchecked")
  @Test
  void should_batch_by_replica_set_and_routing_key_reactive() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn((Optional) Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas(ks, key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas(ks, key3)).thenReturn(replicaSet1);
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(session, REPLICA_SET);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @SuppressWarnings("unchecked")
  @Test
  void should_batch_by_replica_set_and_routing_token_reactive() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn((Optional) Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas(ks, key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas(ks, key3)).thenReturn(replicaSet1);
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(session, REPLICA_SET);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @SuppressWarnings("unchecked")
  @Test
  void should_batch_by_routing_key_when_replica_set_info_not_available_reactive() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn((Optional) Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key3)).thenReturn(new HashSet<>());
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(session, REPLICA_SET);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt6), tuple(stmt3, stmt4), tuple(stmt5));
  }

  @SuppressWarnings("unchecked")
  @Test
  void should_batch_by_routing_token_when_replica_set_info_not_available_reactive() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn((Optional) Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key3)).thenReturn(new HashSet<>());
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(session, REPLICA_SET);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_all_reactive() {
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher();
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchAll(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(((BatchStatement) statements.blockingFirst()))
        .containsExactly(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
  }

  @Test
  void should_honor_max_batch_statements_reactive() {
    assignRoutingTokens();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(2);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
    statements =
        Flowable.fromPublisher(
            batcher.batchAll(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_honor_max_size_in_bytes_reactive() {
    assignRoutingTokensWitSize();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(8L);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize),
            tuple(stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        Flowable.fromPublisher(
            batcher.batchAll(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize),
            tuple(stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
  }

  @Test
  void should_buffer_until_last_element_if_max_size_in_bytes_high_reactive() {
    assignRoutingTokensWitSize();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(1000);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize, stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        Flowable.fromPublisher(
            batcher.batchAll(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(
                stmt1WithSize,
                stmt2WithSize,
                stmt3WithSize,
                stmt4WithSize,
                stmt5WithSize,
                stmt6WithSize));
  }

  @Test
  void should_buffer_by_max_size_in_bytes_if_satisfied_before_max_batch_statements_reactive() {
    assignRoutingTokensWitSize();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(10, 8L);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize),
            tuple(stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        Flowable.fromPublisher(
            batcher.batchAll(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize),
            tuple(stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
  }

  @Test
  void should_buffer_by_max_batch_statements_if_satisfied_before_max_size_in_bytes_reactive() {
    assignRoutingTokensWitSize();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(1, 8L);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize),
            tuple(stmt2WithSize),
            tuple(stmt3WithSize),
            tuple(stmt4WithSize),
            tuple(stmt5WithSize),
            tuple(stmt6WithSize));
    statements =
        Flowable.fromPublisher(
            batcher.batchAll(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize),
            tuple(stmt2WithSize),
            tuple(stmt3WithSize),
            tuple(stmt4WithSize),
            tuple(stmt5WithSize),
            tuple(stmt6WithSize));
  }

  @Test
  void
      should_buffer_until_last_element_if_max_size_in_bytes_and_max_batch_statements_high_reactive() {
    assignRoutingTokensWitSize();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(100, 1000);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize, stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        Flowable.fromPublisher(
            batcher.batchAll(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(
                stmt1WithSize,
                stmt2WithSize,
                stmt3WithSize,
                stmt4WithSize,
                stmt5WithSize,
                stmt6WithSize));
  }

  @Test
  void
      should_buffer_until_last_element_if_max_size_in_bytes_and_max_batch_statements_negative_reactive() {
    assignRoutingTokensWitSize();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(-1, -1);
    Flowable<Statement<?>> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize, stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        Flowable.fromPublisher(
            batcher.batchAll(
                Flowable.just(
                    stmt1WithSize,
                    stmt2WithSize,
                    stmt3WithSize,
                    stmt4WithSize,
                    stmt5WithSize,
                    stmt6WithSize)));
    assertThat(statements.toList().blockingGet())
        .extracting(EXTRACTOR)
        .contains(
            tuple(
                stmt1WithSize,
                stmt2WithSize,
                stmt3WithSize,
                stmt4WithSize,
                stmt5WithSize,
                stmt6WithSize));
  }
}
