/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.batch;

import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.REPLICA_SET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Statement;
import io.reactivex.Flowable;
import java.util.HashSet;
import org.junit.Test;

/** */
public class RxJavaStatementBatcherTest extends StatementBatcherTest {

  @Test
  public void should_batch_by_routing_key_reactive() throws Exception {
    assignRoutingKeys();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher();
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .usingFieldByFieldElementComparator()
        .contains(batch126, batch34, stmt5);
  }

  @Test
  public void should_batch_by_routing_token_reactive() throws Exception {
    assignRoutingTokens();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher();
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .usingFieldByFieldElementComparator()
        .contains(batch1256, batch34);
  }

  @Test
  public void should_batch_by_replica_set_and_routing_key_reactive() throws Exception {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getReplicas("ks", key1)).thenReturn(replicaSet1);
    when(metadata.getReplicas("ks", key2)).thenReturn(replicaSet2);
    when(metadata.getReplicas("ks", key3)).thenReturn(replicaSet1);
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(cluster, REPLICA_SET);
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .usingFieldByFieldElementComparator()
        .contains(batch1256, batch34);
  }

  @Test
  public void should_batch_by_replica_set_and_routing_token_reactive() throws Exception {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getReplicas("ks", key1)).thenReturn(replicaSet1);
    when(metadata.getReplicas("ks", key2)).thenReturn(replicaSet2);
    when(metadata.getReplicas("ks", key3)).thenReturn(replicaSet1);
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(cluster, REPLICA_SET);
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .usingFieldByFieldElementComparator()
        .contains(batch1256, batch34);
  }

  @Test
  public void should_batch_by_routing_key_when_replica_set_info_not_available_reactive()
      throws Exception {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getReplicas("ks", key1)).thenReturn(new HashSet<>());
    when(metadata.getReplicas("ks", key2)).thenReturn(new HashSet<>());
    when(metadata.getReplicas("ks", key3)).thenReturn(new HashSet<>());
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(cluster, REPLICA_SET);
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .usingFieldByFieldElementComparator()
        .contains(batch126, batch34, stmt5);
  }

  @Test
  public void should_batch_by_routing_token_when_replica_set_info_not_available_reactive()
      throws Exception {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getReplicas("ks", key1)).thenReturn(new HashSet<>());
    when(metadata.getReplicas("ks", key2)).thenReturn(new HashSet<>());
    when(metadata.getReplicas("ks", key3)).thenReturn(new HashSet<>());
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(cluster, REPLICA_SET);
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .usingFieldByFieldElementComparator()
        .contains(batch1256, batch34);
  }

  @Test
  public void should_batch_all_reactive() throws Exception {
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher();
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchAll(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(((BatchStatement) statements.singleOrError().blockingGet()).getStatements())
        .containsExactly(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
  }

  @Test
  public void should_honor_max_batch_size_reactive() throws Exception {
    assignRoutingTokens();
    RxJavaStatementBatcher batcher = new RxJavaStatementBatcher(2);
    Flowable<Statement> statements =
        Flowable.fromPublisher(
            batcher.batchByGroupingKey(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .usingFieldByFieldElementComparator()
        .contains(batch12, batch56, batch34);
    statements =
        Flowable.fromPublisher(
            batcher.batchAll(Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)));
    assertThat(statements.toList().blockingGet())
        .usingFieldByFieldElementComparator()
        .contains(batch12, batch56, batch34);
  }
}
